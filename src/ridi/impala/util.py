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

__all__ = [
    "date_to_dict", "date_where_clause", "date_to_partition_spec",
    "date_gte_condition", "date_lte_condition", "date_eq_condition",
    "DATE_AS_STRING",
]
