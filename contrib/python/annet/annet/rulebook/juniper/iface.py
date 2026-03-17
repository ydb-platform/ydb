import ipaddress

from annet.annlib.types import Op
from annet.rulebook import common
from annet.rulebook.common import DiffItem


def ipv6_addr(old, new, diff_pre, _pops):
    """
    Приводим все ipv6-адреса в объекты IPv6Interface и далее сравниваем
    """
    address_new_line = [a for a in map(_parse_ipv6, new) if a]
    address_old_line = [a for a in map(_parse_ipv6, old) if a]

    ret = []
    for item in common.default_diff(old, new, diff_pre, _pops):
        # Проверяем адрес помеченный под снос на наличии в новом списке
        if item.op == Op.REMOVED and _parse_ipv6(item.row) in address_new_line:
            result_item = DiffItem(Op.AFFECTED, item.row, item.children, item.diff_pre)
        # Проверяем адрес помеченный для добавления на наличии в старом списке
        elif item.op == Op.ADDED and _parse_ipv6(item.row) in address_old_line:
            result_item = None
        # Остальное без изменений
        else:
            result_item = item
        if result_item:
            ret.append(result_item)
    return ret


def _parse_ipv6(row):
    """
    Парсит IPv6-интерфейс из строки, предполагая, что адрес находится на втором месте.
    Возвращает объект IPv6Interface или None.
    """
    if row:
        parts = row.split()
        if len(parts) > 1:
            return ipaddress.IPv6Interface(parts[1])
    return None
