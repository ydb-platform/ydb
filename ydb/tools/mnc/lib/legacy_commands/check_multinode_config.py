import logging

from ydb.tools.mnc.lib import common, config
from ydb.tools.mnc.scheme import multinode


logger = logging.getLogger(__name__)

expected_config = multinode.scheme


def add_arguments(parser):
    common.add_common_options(parser)


async def do(args):
    print('original', config.get_config(multinode, args))
    print('recognized', args.config)
