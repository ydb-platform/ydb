from annet.annlib.rulebook import common
from annet.annlib.types import Op


def syslog_level(rule, key, diff, **_):
    # syslog-level can be overwritten only
    if diff[Op.ADDED]:
        yield from common.default(rule, key, diff)
