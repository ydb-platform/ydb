import copy
import re

from annet.annlib.types import Op
from annet.rulebook import common


def rp_node(rule, key, diff, **_):
    # route-policy NAME ACTION node NUM
    (rp_name, node_id) = key
    if diff[Op.REMOVED]:
        if diff[Op.ADDED]:
            sub_diff = {Op.AFFECTED: [], Op.ADDED: [], Op.REMOVED: [], Op.MOVED: [], Op.UNCHANGED: []}
            sub_diff[Op.AFFECTED] = diff[Op.REMOVED]
            yield from common.default(rule, key, sub_diff)
        else:
            yield (False, "undo route-policy %s node %s" % (rp_name, node_id), None)

    if diff[Op.AFFECTED] or diff[Op.ADDED]:
        yield from common.default(rule, key, diff)


def undo_redo(rule, key, diff, **_):
    yield from common.undo_redo(rule, key, diff, **_)


def prefix_list(rule, key, diff, **kwargs):
    # To determine whether the prefix list is being fully modified,
    # the key (family, name) is defined in the h3c.rul rulebook.
    # However, from the command’s point of view, each index represents a separate command.
    # Therefore, we group them by index here and pass them to common.
    diff_by_index = {}
    for op, rows in diff.items():
        for row in rows:
            # prefix list format:
            # ip ip-prefix PRFX_CT_LU_ALLOWED_ROUTES index 15 ..
            # ip ipv6-prefix PFXS_SPECIALv6 index 20 ..
            _ip, _family, _name, _index, index, *_ = row["row"].split()
            if index not in diff_by_index:
                sub_diff = {op: [] for op in diff.keys()}
                diff_by_index[index] = sub_diff
            diff_by_index[index][op].append(row)

    family, name = key
    if family not in {"ip", "ipv6"}:
        raise NotImplementedError("Unknown family '%s'" % family)
    if diff[Op.ADDED] or diff[Op.REMOVED] or diff[Op.MOVED]:
        # Since the rule key originally doesn’t include the index,
        # we need to add it; otherwise, the undo rule will be missing it.
        indexed_rule = copy.deepcopy(rule)
        indexed_rule["reverse"] = "undo ip {}-prefix {} index {}"

        # The stub_index is referenced in the h3c.order rulebook
        # to ensure that the stub is added or removed first or last in order.

        stub, stub_index = "", 99999999

        # If we’re only adding new commands (for example, creating entries) in the prefix list,
        # or deleting/moving them while keeping some parts unchanged,
        # h3c will not treat the list as being removed, and the stub rule is not needed.

        if (diff[Op.REMOVED] or diff[Op.MOVED]) and not diff[Op.UNCHANGED]:
            stub = "deny 0.0.0.0 32" if family == "ip" else "deny :: 128"
        if stub:
            yield (True, f"ip {family}-prefix {name} index {stub_index} {stub}", None)
        for index, sub_diff in diff_by_index.items():
            yield from common.undo_redo(indexed_rule, (family, name, index), sub_diff, **kwargs)
        if stub:
            yield (False, f"undo ip {family}-prefix {name} index {stub_index}", None)


def static(rule, key, diff, **_):
    """
    To roll back a static route, we actually need to pass almost all arguments
    except for the various "track" options.
    At the same time, the number of arguments may vary — optional VRF, optional interface.
    Therefore, we don’t parse the command itself; we just remove the unnecessary arguments.
    """
    if diff[Op.REMOVED]:
        param = key[0]
        idx = param.find(" track")
        if idx > 0:
            key = (param[0:idx],)
        idx = param.find(" description")
        if idx > 0:
            key = (param[0:idx],)
        idx = param.find(" preference")
        if idx > 0:
            key = (param[0:idx],)
    yield from common.default(rule, key, diff)


def port_queue(rule, key, diff, **_):
    """
    To roll back the port-queue configuration on an interface, only a partial parameter specification is required.
    Example of disabling/enabling:
    interface 100GE0/1/33
        undo port-queue af3 wfq outbound
        port-queue af3 wfq weight 30 port-wred WRED outbound

    Essentially, we need to remove all parameters between 'wfq' and 'outbound'.
    NOC-19414
    """
    if diff[Op.REMOVED]:
        param = key[0]
        idx = param.find("weight")
        if idx > 0:
            key = (param[0:idx] + "outbound",)
    yield from common.default(rule, key, diff)


def netstream_undo(rule, key, diff, **_):
    if diff[Op.REMOVED]:
        # The only part we need is the last keyword: inbound or outbound
        # Unfortunately, key is a tuple so we cast it to a list and back
        key = list(key)
        key[1] = key[1].split(" ")[-1]
        key = tuple(key)
    yield from common.default(rule, key, diff)


def snmpagent_sysinfo_version(rule, key, diff, **_):
    """
    Special handling for SNMP sys-info version:
    If device has 'snmp-agent sys-info version v3' that must be removed,
    generate "undo snmp-agent sys-info version v3".
    Falls back to common.default for other cases.
    """
    if diff[Op.REMOVED]:
        for rem in diff[Op.REMOVED]:
            row = rem.get("row", "").strip()
            # If removed row contains a v3 variant, produce undo for v3
            if re.search(r"\bv3\b", row, flags=re.IGNORECASE):
                yield False, "undo snmp-agent sys-info version v3", None
                return
    yield from common.default(rule, key, diff)


def vty_acl_undo(rule, key, diff, **_):
    if diff[Op.REMOVED]:
        chunks = key[0].split()
        result_chunks = ["undo acl"]
        if len(chunks) == 3 and chunks[0] == "ipv6":
            result_chunks.append("ipv6")
        result_chunks.append(chunks[-1])
        yield False, " ".join(result_chunks), None
    else:
        yield from common.default(rule, key, diff)


def port_split(rule, key, diff, **_):
    # pylint: disable=unused-argument
    def _port_split(old, new, old_row, new_row):
        removed = set(old).difference(new)
        added = set(new).difference(old)
        if old and new:
            for ifname in removed:
                yield (False, "undo port split dimension interface " + ifname, None)
            for ifname in added:
                yield (True, "port split dimension interface " + ifname, None)
        elif old and not new:
            yield (False, "undo " + old_row, None)
        elif new and not old:
            yield (True, new_row, None)

    def _row_slot(row):
        res = ""
        for ch in row:
            if ch == "/":
                break
            res = res + ch if ch.isnumeric() else ""
        return int(res) if res else 0

    old_by_slot = {_row_slot(x["row"]): x["row"] for x in diff[Op.REMOVED]}
    new_by_slot = {_row_slot(x["row"]): x["row"] for x in diff[Op.ADDED]}
    for slot in set(old_by_slot.keys()).union(new_by_slot.keys()):
        old_row = old_by_slot[slot] if slot in old_by_slot else ""
        new_row = new_by_slot[slot] if slot in new_by_slot else ""
        old = _expand_portsplit(old_row)
        new = _expand_portsplit(new_row)
        yield from _port_split(old, new, old_row, new_row)
    if old_by_slot or new_by_slot:
        yield (True, "port split refresh", None)


def _expand_portsplit(row):
    expanded = []
    row_parts = row.split()
    for index, part in enumerate(row_parts):
        if part == "to":
            iface_base = "/".join(row_parts[index - 1].split("/")[:-1])
            left = int(row_parts[index - 1].split("/")[-1])
            right = int(row_parts[index + 1].split("/")[-1])
            for i in range(left + 1, right):
                expanded.append(iface_base + "/" + str(i))
        else:
            expanded.append(part)
    return expanded


def classifier(rule, key, diff, **_):
    # if type changes firstly remove all if-match
    # and then recreate classifier
    if diff[Op.ADDED] and diff[Op.REMOVED]:
        yield (True, diff[Op.REMOVED][0]["row"], diff[Op.REMOVED][0]["children"])
    yield from common.default(rule, key, diff)


def undo_children(rule, key, diff, **_):
    def removed_count(subdiff):
        ret = 0
        for child in subdiff["children"].values():
            for child_diff in child["items"].values():
                ret += len(child_diff[Op.REMOVED])
        return ret

    def common_default(op, subdiff):
        newdiff = {Op.ADDED: [], Op.REMOVED: [], Op.MOVED: [], Op.AFFECTED: [], Op.UNCHANGED: []}
        newdiff[op] = [subdiff]
        yield from common.default(rule, key, newdiff)

    # we should say undo because we pretend as single block
    for subdiff in diff[Op.REMOVED]:
        # firstly remove all group-members
        if diff[Op.REMOVED][0]["children"]:
            yield (True, diff[Op.REMOVED][0]["row"], diff[Op.REMOVED][0]["children"])
        yield False, "undo " + subdiff["row"], None
    # firstly destroy affected because inside we can have an undo
    for subdiff in sorted(diff[Op.AFFECTED], key=removed_count, reverse=True):
        yield from common_default(Op.AFFECTED, subdiff)
    for subdiff in diff[Op.ADDED]:
        yield from common_default(Op.ADDED, subdiff)


def clear_instead_undo(rule, key, diff, **_):
    # For some configuration lines, a persistent diff occurs because the line in the config is either explicitly enabled
    # or explicitly disabled. If it is not described in the generator (i.e., we rely on the default),
    # then by using "clear" instead of "undo" we return the configuration to its default state.
    # NOC-20102 @gslv 11-02-2022
    if diff[Op.REMOVED]:
        if diff[Op.REMOVED][0]["row"].endswith(" disable"):
            cmd = diff[Op.REMOVED][0]["row"].replace(" disable", "")
            yield (True, "clear " + cmd, False)
    else:
        yield from common.default(rule, key, diff)


def undo_port_link_mode(rule, key, diff, **_):
    """
    Сhanging port mode from bridge to route and back
    """

    if diff[Op.REMOVED]:
        if key[0] == "route":
            cmd = f"{diff[Op.REMOVED][0]['row']}".replace(key[0], "bridge")
            yield (True, cmd, False)

        if key[0] == "bridge":
            cmd = f"{diff[Op.REMOVED][0]['row']}".replace(key[0], "route")
            yield (True, cmd, False)


def hardware_resource_bfd(rule, key, diff, **_):
    """
    Changing hardware-resource firmware-mode from software
    to hardware and back without undo
    """

    if diff[Op.REMOVED]:
        if key[0] == "INT-PTP":
            cmd = f"{diff[Op.REMOVED][0]['row']}".replace(key[0], "INT-BFD")
            yield (True, cmd, False)

        if key[0] == "INT-BFD":
            cmd = f"{diff[Op.REMOVED][0]['row']}".replace(key[0], "INT-PTP")
            yield (True, cmd, False)


def change_user_password(rule, key, diff, **_):
    """
    If we have to change password hash
    then we just push new hash to configuration
    without undo
    """

    if diff[Op.REMOVED]:
        if key[0] == "route":
            cmd = f"{diff[Op.REMOVED][0]['row']}".replace(key[0], "bridge")
            yield (True, cmd, False)

        if key[0] == "bridge":
            cmd = f"{diff[Op.REMOVED][0]['row']}".replace(key[0], "route")
            yield (True, cmd, False)


def remove_trunk_pvid(rule, key, diff, **_):
    """
    Special-case undo for H3C: "port trunk pvid vlan <num>" -> "undo port trunk pvid"
    Falls back to common.default for other cases.
    """
    if diff[Op.REMOVED]:
        for rem in diff[Op.REMOVED]:
            row = rem.get("row", "").strip()
            if re.match(r"^port\s+trunk\s+pvid\s+vlan\s+\d+$", row, flags=re.IGNORECASE):
                yield False, "undo port trunk pvid", None
                return
    yield from common.default(rule, key, diff)
