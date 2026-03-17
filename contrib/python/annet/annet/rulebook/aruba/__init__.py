from annet.annlib.rulebook import common
from annet.annlib.types import Op


def default_diff(old, new, diff_pre, _pops=(Op.AFFECTED,)):
    diff = common.base_diff(old, new, diff_pre, _pops, moved_to_affected=True)
    diff[:] = _skip_non_ap_env_affected(diff)
    return diff


def _skip_non_ap_env_affected(diff):
    for x in diff:
        if x.op == Op.AFFECTED and not x.children:
            if x.diff_pre["attrs"]["context"].get("block") != "ap-env":
                continue
        yield x
