from annet.annlib.lib import uniq
from annet.annlib.rulebook import common
from annet.annlib.types import Op
from annet.rulebook.cisco.iface import is_in_channel, is_ip_cmd, is_vpn_cmd


def diff(old, new, diff_pre, _pops=(Op.AFFECTED,)):
    for iface_row in old:
        _filter_channel_members(old[iface_row])
    for iface_row in new:
        _filter_channel_members(new[iface_row])

    for iface_row in uniq(old, new):
        iface_old = old.get(iface_row, {})
        iface_new = new.get(iface_row, {})
        iface_pre = diff_pre[iface_row]["subtree"]
        vpn_changed = False
        for op, cmd, _, _ in common.default_diff(iface_old, iface_new, iface_pre, _pops):
            if op in {Op.ADDED, Op.REMOVED}:
                vpn_changed |= is_vpn_cmd(cmd)
                break

        if vpn_changed:
            for cmd in list(iface_old.keys()):
                if is_ip_cmd(cmd) and not is_vpn_cmd(cmd):
                    del iface_old[cmd]

        if _is_diff_removed_lag_from_lag_member(iface_old, iface_new):
            for cmd in list(iface_old.keys()):
                if _is_allowed_on_old_lag_memeber(cmd):
                    del iface_old[cmd]

    ret = common.default_diff(old, new, diff_pre, _pops)

    return ret


# ===

# Вырезает все команды не разрешенные
# на членах агрегата. В running-config
# листинге они наследуются от самого port-channel


def _filter_channel_members(tree):
    if any(is_in_channel(x) for x in tree):
        for cmd in list(tree.keys()):
            if not _is_allowed_on_channel(cmd):
                del tree[cmd]


def _is_allowed_on_channel(cmd_line):
    return cmd_line.startswith(
        (
            "channel-group",
            "cdp",
            "description",
            "inherit",
            "ip port",
            "ipv6 port",
            "mac port",
            "lacp",
            "switchport host",
            "switchport",
            "shutdown",
            "rate-limit cpu",
            "snmp trap link-status",
            "mtu",
            "macsec",  # NOCDEV-9008
        )
    )


# ===

# При удалении lag, часть команд удаляется сначала с lag, тем самым удаляя с lag member,
# но рассчитанный diff этот нюанс не учитывает


def _is_diff_removed_lag_from_lag_member(old, new):
    """
    Проверяем, что в old есть признак lag member, а в new его нет. Следовательно порт выводится из lag.
    """
    if any(is_in_channel(x) for x in old) and not any(is_in_channel(x) for x in new):
        return True
    return False


def _is_allowed_on_old_lag_memeber(cmd_line):
    """
    Эти команды принудительно добавим на интерфейс после удаления его из lag
    """
    return cmd_line.startswith(("mtu",))
