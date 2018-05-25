from collections import namedtuple

ConnectArgsType = namedtuple('ConnectArgsType', [
    'host', 'port', 'user', 'password', 'database'
])

IMPALA_CONNECT_ARGS = None


class ImpalaConfig(object):
    @staticmethod
    def set(host=None, port=None, user=None, password=None, database=None):
        global IMPALA_CONNECT_ARGS
        IMPALA_CONNECT_ARGS = ConnectArgsType(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
        )._asdict()
