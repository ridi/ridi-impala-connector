from collections import namedtuple

ConnectArgsType = namedtuple('ConnectArgsType', [
    'host', 'port', 'user', 'password', 'database'
])

IMPALA_CONNECT_ARGS = ConnectArgsType(
    host="localhost",
    port=21050,
    user="user",
    password="password",
    database="default",
)


class ImpalaConfig(object):
    @staticmethod
    def set(host=None, port=None, auth=None, database=None):
        if host:
            IMPALA_CONNECT_ARGS.host = host
        if port:
            IMPALA_CONNECT_ARGS.port = port
        if auth:
            user, password = auth
            IMPALA_CONNECT_ARGS.user = user
            IMPALA_CONNECT_ARGS.password = password
        if database:
            IMPALA_CONNECT_ARGS.database = database
