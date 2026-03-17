from annet.annlib.types import Op
from annet.rulebook import common


# [NOCDEV-2180] After vrf was change we put ip config one more time
def binding_change(old, new, diff_pre, _pops=(Op.AFFECTED,)):
    ret = common.default_diff(old, new, diff_pre, _pops)
    vpn_changed = False
    for op, cmd, _, _ in ret:
        if op in {Op.ADDED, Op.REMOVED}:
            vpn_changed |= _is_vpn_cmd(cmd)
    if vpn_changed:
        for cmd in list(old.keys()):
            if not _is_vpn_cmd(cmd):
                del old[cmd]
        ret = common.default_diff(old, new, diff_pre, _pops)
    return ret


def _is_vpn_cmd(cmd):
    return cmd.startswith("ip binding vpn-instance")
