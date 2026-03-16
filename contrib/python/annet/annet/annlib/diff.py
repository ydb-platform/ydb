import functools
import ipaddress
from typing import Dict, Generator

import colorama

from .types import Diff, Op


# NOCDEV-1720


diff_ops = {
    "+": Op.ADDED,
    "-": Op.REMOVED,
    " ": Op.AFFECTED,
    ">": Op.MOVED,
}

ops_sign = {v: k for k, v in diff_ops.items()}

ops_order = {
    Op.AFFECTED: 0,
    Op.MOVED: 1,
    Op.REMOVED: 2,
    Op.ADDED: 3,
}

ops_color = {
    Op.REMOVED: colorama.Fore.RED,
    Op.ADDED: colorama.Fore.GREEN,
    Op.AFFECTED: colorama.Fore.CYAN,
    Op.MOVED: colorama.Fore.YELLOW,
}


def is_int(ts):
    try:
        int(ts)
        return True
    except ValueError:
        return False


def is_ip(ts):
    try:
        ipaddress.ip_interface(ts)
        return True
    except ValueError:
        return False


def diff_cmp(diff_l, diff_r):
    """
    Сборник костылей для сравнения двух строк диффа
    """
    (op_l, line_l, _, _) = diff_l
    (op_r, line_r, _, _) = diff_r

    cmp_op = ops_order[op_l] - ops_order[op_r]
    if line_l == line_r:
        # При равенстве строк порядок определяется операцией
        return cmp_op

    if cmp_op == 0:
        # Если для строк операции одинаковы, то считаем их равными
        # По идее TimSort стабилен и порядок просто не должен измениться
        return 0

    # Для частично совпадающих строк
    lws = line_l.split(" ")
    rws = line_r.split(" ")
    res = 0
    for i, lw in enumerate(lws):
        if len(rws) > i:
            rw = rws[i]
            if is_int(lw) and is_int(rw):
                # В одинаковом положении в строках есть инты, сортируем по ним
                res = int(lw) - int(rw)
                if res == 0:
                    # При равных интах - сортируем по операции
                    res = cmp_op
            elif is_ip(lw) and is_ip(rw):
                # Аналогично интам обрабатываем ip-адреса
                ip_l = ipaddress.ip_interface(lw)
                ip_r = ipaddress.ip_interface(rw)
                try:
                    res = (ip_l > ip_r) - (ip_l < ip_r)
                except TypeError:
                    res = 1
                    if ip_l.version == 4:
                        res = -1
                if res == 0:
                    res = cmp_op
            elif i > 0:
                if lw == rw:
                    res = cmp_op
            else:
                continue
            break
    return res


def resort_diff(diff: Diff) -> Diff:
    res = []
    df = sorted(diff, key=functools.cmp_to_key(diff_cmp))
    for line in df:
        ln = line
        if len(line[2]) > 0:
            ln = (line[0], line[1], resort_diff(line[2]), line[3])
        res.append(ln)
    return res


def colorize_line_with_color(line: str, color: int, no_color: bool):
    stripped = line.rstrip("\n")
    add_newlines = len(line) - len(stripped)
    line = stripped

    if not no_color:
        line = "%s%s%s%s" % (colorama.Style.BRIGHT, color, line, colorama.Style.RESET_ALL)

    line += "\n" * add_newlines
    return line


def colorize_line(line, no_color=False):
    op = diff_ops[line[0]]
    color = ops_color[op]
    return colorize_line_with_color(line, color, no_color)


def gen_pre_as_diff(
    pre: Dict, show_rules: bool, indent: str, no_color: bool, _level: int = 0
) -> Generator[str, None, None]:
    ops = [(order, op) for op, order in ops_order.items()]
    ops.sort()
    for raw_rule, content in pre.items():
        items = content["items"].items()
        for _, diff in items:  # pylint: disable=redefined-outer-name
            if show_rules and not raw_rule == "__MULTILINE_BODY__":
                line = "# %s%s\n" % (indent * _level, raw_rule)
                yield colorize_line_with_color(line, colorama.Fore.BLACK, no_color)
            for op, rows in [(op, diff[op]) for (_, op) in ops]:
                for item in rows:
                    line = "%s%s %s\n" % (ops_sign[op], indent * _level, item["row"])
                    yield colorize_line_with_color(line, ops_color[op], no_color)
                    if len(item["children"]) != 0:
                        yield from gen_pre_as_diff(item["children"], show_rules, indent, no_color, _level + 1)
