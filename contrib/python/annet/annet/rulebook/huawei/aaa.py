from collections import defaultdict

from annet.annlib.types import Op
from annet.rulebook.common import default, default_diff


def user(key, diff, **_):
    check_for_remove = True
    added = []
    for add in diff[Op.ADDED]:
        added.append((True, add["row"], None))
        if add["row"].startswith("local-user %s password" % key[0]):
            check_for_remove = False
    if check_for_remove:
        for rem in diff[Op.REMOVED]:
            # Обрабатывать удаление только пароля или привилегий,
            # если меняется что-то другое, можно просто накатить без удаления
            if rem["row"].startswith("local-user %s password" % key[0]):
                yield (False, "undo local-user %s" % key[0], None)
                return
            if rem["row"].startswith("local-user %s privilege" % key[0]) and not _added_contains(
                diff[Op.ADDED], "local-user %s privilege" % key[0]
            ):
                yield (False, "undo local-user %s" % key[0], None)
                return
    yield from added


def _added_contains(array: list[dict], lookup_string: str) -> bool:
    for item in array:
        if item["row"].startswith(lookup_string):
            return True
    return False


def domain(rule, key, diff, **_):
    """
    При удалении метода для accounting|authorization|authentication
    не нужно указывать сам метод, поэтому откидываем последний ключ.
    """
    if diff[Op.REMOVED]:
        yield (False, rule["reverse"].format(key[0], ""), None)
    else:
        yield from default(rule, key, diff)


def local_user_diff(old, new, diff_pre, **kwargs):
    diff = default_diff(old, new, diff_pre, **kwargs)
    filtered_diff = []
    # Группируем команды local-user по пользователю
    # и назначению (mode будет "password", "service-type", etc.)
    # {("username", "mode"): {op: diff_item}}}
    grouped = defaultdict(dict)
    for diff_item in diff:
        username, mode = _local_user_row_key(diff_item.row)
        if username and mode:
            grouped[(username, mode)][diff_item.op] = diff_item

    filtered_diff = []
    for diff_item in diff:
        username, mode = _local_user_row_key(diff_item.row)
        if username and mode:
            ops = set(grouped[(username, mode)])
            # NOCDEVDUTY-1786 делаем так чтобы в генераторе не требовалось точно попасть в порядок service-type
            # у хуавей порядок аргументов в данном месте меняется в зависимости от версии софта
            # при этом команда принимается в любом виде, меняется отображение в конфиге, вводить ее можно как угодно
            # если команды local-user * service-type ... совпадают с точностью до перестановки то ничего не правим
            if mode == "service-type" and ops == {Op.ADDED, Op.REMOVED}:
                added = set(grouped[(username, mode)][Op.ADDED].row.split())
                removed = set(grouped[(username, mode)][Op.REMOVED].row.split())
                if added == removed:
                    continue

        filtered_diff.append(diff_item)

    return filtered_diff


def _local_user_row_key(row):
    username, mode = None, None
    splitted_row = row.split()
    # Ожидаемый формат команды 'local-user <username> <mode> ...'
    if splitted_row and splitted_row[0] == "local-user":
        if len(splitted_row) >= 3:
            username = splitted_row[1]
            mode = splitted_row[2]
    return username, mode
