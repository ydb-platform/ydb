import collections
import re
import typing

from annet.vendors import tabparser

from . import patching
from .diff import diff_ops, ops_sign
from .rbparser import acl


UnifiedInputConfig = str  # Конфиг классических сетевых устройств
FileInputConfig = typing.Dict[str, typing.Any]  # Конфиг вайтбоксов и серверов
InputConfig = typing.Union[UnifiedInputConfig, FileInputConfig]

Acl = typing.Dict[str, typing.Any]

UnifiedConfigTree = typing.OrderedDict[str, typing.Any]
FileConfigTree = typing.Dict[str, typing.Any]
ConfigTree = typing.Union[UnifiedConfigTree, FileConfigTree]

DiffTree = typing.OrderedDict[str, typing.Any]
UnifiedDiff = typing.List[typing.Tuple[str, str, typing.List, typing.Optional[int]]]
FileConfigDiff = typing.Dict[str, typing.Any]
Diff = typing.Union[UnifiedDiff, FileConfigDiff]


def make_acl(text: str, vendor: str) -> Acl:
    return acl.compile_acl_text(text, vendor)


def filter_config(acl: Acl, fmtr: tabparser.CommonFormatter, input_config: InputConfig) -> InputConfig:
    if isinstance(input_config, str):
        config: ConfigTree = tabparser.parse_to_tree(input_config, fmtr.split)
        config = patching.apply_acl(config, acl, fatal_acl=False)
        config = fmtr.join(config)
    else:
        config = apply_acl_fileconfig(input_config, acl)
    return config


def filter_diff(acl: Acl, fmtr: tabparser.CommonFormatter, input_config: InputConfig) -> InputConfig:
    if isinstance(input_config, str):
        input_config = shift_op(input_config)
        diff_tee: DiffTree = tabparser.parse_to_tree(input_config, fmtr.split)
        diff: Diff = tree_to_diff(diff_tee)
        diff = patching.apply_acl_diff(diff, acl)
        config = fmtr.join(diff_to_tree(diff))
        config = unshift_op(config)
        config = config.rstrip()
    else:
        config = apply_acl_fileconfig(input_config, acl)
    return config


def filter_patch(acl: Acl, fmtr: tabparser.CommonFormatter, text: str) -> str:
    return filter_config(acl, fmtr, text)


# NOCDEV-6378 на патч для Juniper/Nokia нельзя просто так наложить filter_acl
def filter_patch_jun_nokia(diff_filtered: InputConfig, fmtr: tabparser.CommonFormatter, text: str) -> str:
    """
    Накладываем ACL на патчи для Juniper/Nokia

    Поскольку в патче уже потерена иерархия команд - они развернуты в строки типа
    Нужна дополнительная информация о изначальном конфиге, которую можно подсмотреть в дифе
        set interface et-0/0/0 unit ....
        delete interface et-0/0/0 unit ....
        /configure port 1/1/c17/1 ...
        /configure delete port 1/1/c18/1 ...
    """
    diff_tree_stripped: DiffTree = tabparser.parse_to_tree(strip_op(diff_filtered), fmtr.split)
    _tree_expand_lists_nokia_jun(diff_tree_stripped)
    patch_lines_passed = []
    for patch_line in text.split("\n"):
        patch_parts = [x for x in patch_line.split(" ") if x]
        diff_current = diff_tree_stripped
        # strip set|delete|/configure
        while patch_parts and patch_parts[0] in {"/configure", "set", "delete"}:
            patch_parts = patch_parts[1:]
        while patch_parts and diff_current:
            for i in range(len(patch_parts), -1, -1):
                key = " ".join(patch_parts[:i])
                # consume parts and go down in diff hierarchy
                if key in diff_current:
                    patch_parts = patch_parts[i:]
                    diff_current = diff_current[key]
                    break
            # no progress has been made
            else:
                break
        if not patch_parts:
            patch_lines_passed.append(patch_line)
    return "\n".join(patch_lines_passed)


def apply_acl_fileconfig(config, rules):
    passed = {}
    for filename, filecontent in config.items():
        (match, _) = patching.match_row_to_acl(filename, rules)
        if match:
            if not (match["is_reverse"] and match["attrs"]["cant_delete"]):
                passed[filename] = filecontent
    return passed


def get_op(line: str) -> typing.Tuple[str, str, str]:
    op = " "
    indent = ""
    opidx = -1
    rowstart = 0

    for rowstart in range(len(line)):
        if line[rowstart] != " " and line[0:rowstart].strip():
            break
        if line[rowstart] not in diff_ops:
            break

    for opidx in range(rowstart):
        if line[opidx] != " ":
            break

    if opidx >= 0:
        op = line[opidx]
        indent = line[:opidx] + line[opidx + 1 : rowstart]
    if op != " ":
        indent = indent + " "

    return op, indent, line[rowstart:]


def shift_op(text: str) -> str:
    ret = ""
    for line in text.split("\n"):
        op, indent, line = get_op(line)
        ret += indent + op + line + "\n"
    return ret


def unshift_op(text: str) -> str:
    ret = ""
    for line in text.split("\n"):
        op, indent, line = get_op(line)
        ret += op + indent + line + "\n"
    return ret


def strip_op(text: str) -> str:
    ret: str = ""
    for line in text.split("\n"):
        op, indent, line = get_op(line)
        if op != " ":
            indent = indent[1:]
        ret += indent + line + "\n"
    return ret


def tree_to_diff(diff_tree: ConfigTree) -> Diff:
    ret = []
    for row, v in diff_tree.items():
        op, _, row = get_op(row)
        diff_op = diff_ops[op]
        children = []
        d_match = None
        if isinstance(v, dict):
            children = tree_to_diff(v)
        ret.append((diff_op, row, children, d_match))
    return ret


def diff_to_tree(diff: Diff) -> ConfigTree:
    ret = collections.OrderedDict()
    for diff_op, row, children, _ in diff:
        row = ops_sign[diff_op] + row
        ret[row] = diff_to_tree(children)
    return ret


def _tree_expand_lists_nokia_jun(diff_tree: DiffTree):
    """
    Раскрываем списки Nokia/Juniper в отдельные элементы
    {command: {"[a, b, c]": {}}}   ->   {command a: {}, command b: {}, command c: {}}

    В неупорядоченном множестве префиксов также стираем ';' на конце - их не бывает в патче
    {prefix-list: {"2a02::/64;": {}, "2a03::/64;": {}}}   ->   {prefix-list: {"2a02::/64": {}, "2a03::/64": {}}}
    """
    process: typing.List[DiffTree] = [diff_tree]
    list_regexp = re.compile(r"^(.*)\s+\[(.+)\]$")
    while process:
        tree, process = process[0], process[1:]
        for cmd in list(tree.keys()):
            children = tree[cmd]
            matches = list_regexp.search(cmd)
            normalized_cmd = cmd.rstrip(";")
            if matches:
                for c in matches.group(2).split(" "):
                    if c.strip():
                        tree[" ".join([matches.group(1), c])] = children
            if normalized_cmd != cmd:
                del tree[cmd]
                tree[normalized_cmd] = children
            if isinstance(children, dict):
                process.append(children)
