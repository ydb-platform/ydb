from annet.annlib.lib import huawei_collapse_vlandb as collapse_vlandb
from annet.annlib.lib import huawei_expand_vlandb as expand_vlandb
from annet.annlib.types import Op
from annet.rulebook import common
from annet.rulebook.common import DiffItem


# =====
def single(rule, key, diff, **_):
    yield from _process_vlandb(rule, key, diff, False, False, None)


def multi(rule, key, diff, **_):
    yield from _process_vlandb(rule, key, diff, True, False, 10)


def multi_all(rule, key, diff, **_):
    yield from _process_vlandb(rule, key, diff, True, True, 10)


def vlan_diff(old, new, diff_pre, _pops):
    batch_new = set()  # vlan batch ... vlan ids
    for row in new:
        prefix, vlans = _parse_vlancfg(row)
        if prefix == "vlan batch":
            batch_new.update(vlans)
    ret = []
    for item in common.default_diff(old, new, diff_pre, _pops):
        prefix, vlan_ids = _parse_vlancfg(item.row)
        # If a VLAN was declared globally and still remains in the batch,
        # the command "undo vlan ..." will attempt to completely remove it from the device
        # as well as from the batch. However, using "undo vlan ... ; vlan batch ..." is not a solution,
        # since to delete it, the CLI requires removing all VLAN interfaces and related elements first.

        if prefix == "vlan" and item.op == Op.REMOVED and batch_new.intersection(vlan_ids):
            result_item = DiffItem(Op.AFFECTED, item.row, item.children, item.diff_pre)
        # If a VLAN is declared both globally and in the batch,
        # and the global declaration block has no additional options,
        # we don’t include it — it would just hang there unnecessarily.
        # This way, we preserve symmetry with the previous logic,
        # and both invariants will produce an empty patch.

        elif prefix == "vlan" and batch_new.intersection(vlan_ids) and not item.children:
            result_item = None
        # We don’t touch "vlan batch" or anything else.
        else:
            result_item = item
        if result_item:
            ret.append(result_item)
    return ret


# =====
def _process_vlandb(rule, key, diff, multi, multi_all, multi_chunk):
    assert len(diff[Op.AFFECTED]) == 0, "WTF? Affected signle: %r" % (diff[Op.AFFECTED])
    if not multi:
        for op in (Op.ADDED, Op.REMOVED):
            assert 0 <= len(diff[op]) <= 1, "Too many actions: %r" % (diff)

    if diff[Op.REMOVED] and not diff[Op.ADDED]:  # Removed
        if multi and multi_all:
            yield (False, rule["reverse"].format(*key) + " all", None)
            return
        elif not multi and not multi_all:
            yield (False, rule["reverse"].format(*key), None)
            return

    (prefix_add, new) = _parse_vlancfg_actions(diff[Op.ADDED])
    (prefix_del, old) = _parse_vlancfg_actions(diff[Op.REMOVED])
    removed = old.difference(new)
    added = new.difference(old)

    if removed:
        collapsed = collapse_vlandb(removed)
        for chunk in _chunked(collapsed, multi_chunk) if multi else [collapsed]:
            yield (False, "undo %s %s" % (prefix_del, " ".join(chunk)), None)

    if added:
        collapsed = collapse_vlandb(added)
        for chunk in _chunked(collapsed, multi_chunk) if multi else [collapsed]:
            yield (True, "%s %s" % (prefix_add, " ".join(chunk)), None)


def _chunked(items, size):
    for offset in range(0, len(items), size):
        yield items[offset : offset + size]


def _parse_vlancfg_actions(actions):
    prefix = None
    vlandb = set()
    for action in actions:
        (prefix, part) = _parse_vlancfg(action["row"])
        vlandb.update(part)
    return (prefix, vlandb)


def _parse_vlancfg(row):
    parts = row.split()
    assert len(parts) > 0, row
    index = None
    for index, item in reversed(list(enumerate(parts))):
        if not (item.isdigit() or item == "to"):
            break
    prefix = " ".join(parts[: index + 1])
    vlandb = expand_vlandb(" ".join(parts[index + 1 :]))
    return (prefix, vlandb)


def _find_new_vlans(root_pre):
    ret = set()
    for rule, pre in root_pre.items():
        if not rule.startswith("vlan batch"):
            continue
        new = _parse_vlancfg_actions(pre["items"][tuple()][Op.ADDED])[1]
        ret.update(new)
    return ret
