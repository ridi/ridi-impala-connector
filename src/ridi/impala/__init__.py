from impala.dbapi import connect as impala_connect
from impala.error import Error as impala_Error
from .config import IMPALA_CONNECT_ARGS
import logging

logger = logging.getLogger(__name__)


class ImpalaError(Exception):
    def __init__(self, cause):
        self.cause = cause

    def __str__(self):
        return str(self.cause)


class impala_thrift_connection(object):
    def __init__(self, connection):
        self._conn = connection

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc_value, _traceback):
        self._conn.__exit__(_exc_type, _exc_value, _traceback)

    def execute_command(self, query):
        with self._conn.cursor() as cur:
            try:
                if not isinstance(query, list):
                    query = [query]
                for q in query:
                    logger.debug(q)
                    cur.execute(q)
            except impala_Error as e:
                raise ImpalaError(e)

    def execute_query(self, query, check_error=True):
        with self._conn.cursor() as cur:
            try:
                logger.debug(query)
                cur.execute(query)
                schema = cur.description
                rows = cur.fetchall()
                column_names = [col[0] for col in schema]
                named_rows = [{column_names[idx]: value for idx, value in enumerate(row)} for row in rows]
                return named_rows
            except impala_Error as e:
                if check_error:
                    raise ImpalaError(e)
                else:
                    return []

    def execute_query_buffered(self, query, arraysize=None, check_error=True):
        with self._conn.cursor() as cur:
            try:
                if arraysize:
                    cur.arraysize = arraysize
                cur.execute(query)
                schema = cur.description
                column_names = [col[0] for col in schema]
                for row in cur:
                    named_row = {column_names[idx]: value for idx, value in enumerate(row)}
                    yield named_row
            except impala_Error as e:
                if check_error:
                    raise ImpalaError(e)
                else:
                    return


class ImpalaThriftClient(object):
    def __init__(self, connect_args):
        self.connect_args = connect_args

    def get_connection(self, **extra_args):
        connect_args = self.connect_args
        connect_args.update(extra_args)
        impala_conn = impala_connect(**connect_args)
        return impala_thrift_connection(impala_conn)


def date_to_dict(the_date):
    return {'year': the_date.year, 'month': the_date.month, 'day': the_date.day}


def date_where_clause(the_date):
    return "year=%(year)d AND month=%(month)d AND day=%(day)d" % date_to_dict(the_date)


def date_to_partition_spec(the_date):
    return "(year=%(year)d, month=%(month)d, day=%(day)d)" % date_to_dict(the_date)


def date_gte_condition(the_date):
    return "(year > %(year)d OR (year=%(year)d AND month > %(month)d) OR (year=%(year)d AND month=%(month)d AND day >= %(day)d))" % date_to_dict(the_date)


def date_lte_condition(the_date):
    return "(year < %(year)d OR (year=%(year)d AND month < %(month)d) OR (year=%(year)d AND month=%(month)d AND day <= %(day)d))" % date_to_dict(the_date)


def date_eq_condition(the_date):
    return "(year = %(year)d AND month=%(month)d AND day=%(day)d)" % date_to_dict(the_date)

DATE_AS_STRING = "CONCAT(LPAD(CAST(year AS STRING),4,'0'),'-',LPAD(CAST(month AS STRING),2,'0'),'-',LPAD(CAST(day AS STRING),2,'0'))"

IMPALA_CLIENT = ImpalaThriftClient(IMPALA_CONNECT_ARGS.as_dict())
