from impala.dbapi import connect as impala_connect

from ridi.impala.conn import impala_thrift_connection


class ImpalaThriftClient(object):
    def __init__(self, connect_args):
        self.connect_args = connect_args

    def get_connection(self, **extra_args):
        connect_args = self.connect_args
        connect_args.update(extra_args)
        impala_conn = impala_connect(**connect_args)
        return impala_thrift_connection(impala_conn)


__all__ = [
    "ImpalaThriftClient"
]
