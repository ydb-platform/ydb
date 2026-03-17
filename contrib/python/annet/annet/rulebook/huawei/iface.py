import re

from annet.annlib.types import Op
from annet.rulebook import common


def permanent(rule, key, diff, **kwargs):  # pylint: disable=redefined-outer-name
    ifname = key[0]
    if re.match(r"(Eth-Trunk|Vlanif|Vbdif|Loop[Bb]ack|Tunnel|.*\.\d+)", ifname):
        # эти интерфейсы можно удалять
        yield from common.default(rule, key, diff, **kwargs)
    else:
        yield from common.permanent(rule, key, diff, **kwargs)


# [NOCDEV-2180] Хуавей просит переввести ip конфигурацию после изменения vrf
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
