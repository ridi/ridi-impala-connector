import logging

from impala.error import Error as impala_Error

from ridi.impala.error import ImpalaError

logger = logging.getLogger(__name__)


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


__all__ = ["impala_thrift_connection"]
