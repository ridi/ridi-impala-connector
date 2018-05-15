from collections import namedtuple

ConnectArgsType = namedtuple('ConnectArgsTuple', [
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
    def set_host(host):
        IMPALA_CONNECT_ARGS.host = host

    @staticmethod
    def set_port(port):
        IMPALA_CONNECT_ARGS.port = port

    @staticmethod
    def set_auth(user, password):
        IMPALA_CONNECT_ARGS.user = user
        IMPALA_CONNECT_ARGS.password = password

    @staticmethod
    def set_database(database):
        IMPALA_CONNECT_ARGS.database = database
