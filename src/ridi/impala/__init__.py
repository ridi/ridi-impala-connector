from ridi.impala.client import *
from ridi.impala.config import *
from ridi.impala.conn import *
from ridi.impala.error import *
from ridi.impala.util import *


def connect(**extra_args):
    from ridi.impala.config import IMPALA_CONNECT_ARGS
    _client = ImpalaThriftClient(IMPALA_CONNECT_ARGS)

    return _client.get_connection(**extra_args)

