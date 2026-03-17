import functools
import re
from collections import OrderedDict as odict

from annet.annlib.lib import jun_activate, jun_is_inactive, merge_dicts
from annet.annlib.types import Op
from annet.rulebook import common
from annet.vendors.tabparser import JuniperFormatter


def comment_processor(item: common.DiffItem):
    if item.op in (Op.REMOVED, Op.ADDED) and item.row.startswith(JuniperFormatter.Comment.begin):
        comment = JuniperFormatter.Comment.loads(item.row)

        item.diff_pre["attrs"]["context"]["comment"] = True
        item.diff_pre["attrs"]["context"]["row"] = comment.row
        item.diff_pre["key"] = item.diff_pre["raw_rule"] = (
            f"{JuniperFormatter.Comment.begin} {comment.row} {JuniperFormatter.Comment.end}"
        )

        return common.DiffItem(
            item.op,
            f"{JuniperFormatter.Comment.begin} {comment.comment} {JuniperFormatter.Comment.end}",
            item.children,
            item.diff_pre,
        )
    else:
        return item


def inactive_blocks(diff_foo):
    @functools.wraps(diff_foo)
    def wrapper(old: odict, new: odict, diff_pre, *args, **kwargs):
        old_inactives = list(map(jun_activate, filter(jun_is_inactive, old)))
        new_inactives = list(map(jun_activate, filter(jun_is_inactive, new)))

        diff: list[common.DiffItem]

        if len(old_inactives) == 0 and len(new_inactives) == 0:
            diff = diff_foo(old, new, diff_pre, *args, **kwargs)
        else:
            inactive_pre = odict([(jun_activate(k), v) for k, v in diff_pre.items() if jun_is_inactive(k)])
            merged_pre = merge_dicts(diff_pre, inactive_pre)

            diff = diff_foo(strip_toplevel_inactives(old), strip_toplevel_inactives(new), merged_pre, *args, **kwargs)

            for activated in [k for k in old_inactives if k in new]:
                diff.append(
                    common.DiffItem(Op.ADDED, activate_cmd(activated, merged_pre), [], diff_pre[activated]["match"])
                )

            for deactivated in [k for k in new_inactives if k not in old_inactives]:
                # если деактивуруемого блока не существует - ставим один deactivate, глубже не идем
                if deactivated not in diff_pre:
                    diff = [
                        common.DiffItem(
                            Op.ADDED, deactivate_cmd(deactivated, merged_pre), [], inactive_pre[deactivated]["match"]
                        )
                    ]
                else:
                    diff.append(
                        common.DiffItem(
                            Op.ADDED, deactivate_cmd(deactivated, merged_pre), [], diff_pre[deactivated]["match"]
                        )
                    )

        return list(map(comment_processor, diff))

    return wrapper


@inactive_blocks
def default_diff(old, new, diff_pre, _pops=(Op.AFFECTED,)):
    diff = common.default_diff(old, new, diff_pre, _pops)
    diff = ignore_quotes(diff)
    diff = strip_inactive_removed(diff)
    return diff


@inactive_blocks
def ordered_diff(old, new, diff_pre, _pops=(Op.AFFECTED,)):
    diff = common.ordered_diff(old, new, diff_pre, _pops)
    diff = ignore_quotes(diff)
    diff = strip_inactive_removed(diff)
    return diff


# =====
def strip_toplevel_inactives(tree):
    for inactive in filter(jun_is_inactive, tree):
        assert jun_activate(inactive) not in tree
    return odict([(k, v) if not jun_is_inactive(k) else (jun_activate(k), v) for k, v in tree.items()])


def activate_cmd(active_key, diff_pre):
    return cmd(active_key, diff_pre, "activate")


def deactivate_cmd(active_key, diff_pre):
    return cmd(active_key, diff_pre, "deactivate")


def cmd(active_key, diff_pre, cmd):
    assert not jun_is_inactive(active_key)
    if not diff_pre[active_key]["subtree"]:
        # Если конанда не имеет подблоков И имеет агрументы то надо их отбросить
        return " ".join([cmd, active_key.split()[0]])
    return " ".join([cmd, active_key])


def ignore_quotes(diff):
    """
    Фильтрует из diff строки которые различаются
    только наличием/отсутсвием кавычек
    i.e.
    description "loopbacks";
    description loopbacks;
    эквивалентны
    """
    equivs = {}
    for elem in diff:
        key = strip_quotes(elem[1])
        if key not in equivs:
            equivs[key] = 0
        equivs[key] += 1
    filtered_diff = [elem for elem in diff if equivs[strip_quotes(elem[1])] == 1]
    return filtered_diff


def strip_quotes(key):
    return re.sub(r"\"(?P<quoted_text>[^\"]+)\"$", r"\g<quoted_text>", key)


def strip_inactive_removed(diff):
    for elem in diff:
        if elem[0] == Op.REMOVED and elem[3]["key"]:
            key = elem[3]["key"][0]
            if jun_is_inactive(key):
                elem[3]["key"] = tuple([jun_activate(key)])
    return diff
