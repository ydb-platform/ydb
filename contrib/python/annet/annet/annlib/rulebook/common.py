import os
import typing
from collections import OrderedDict as odict

from annet.annlib.command import Command, CommandList
from annet.types import Op
from annet.vendors import registry_connector


# =====
def default(rule, key, diff, **_):
    r"""
    Функция default() обеспечивает базовую логику обработки всех правил. Ее можно заменить с помощью
    параметра %logic в текстовом рулбуке. Она вызывается для каждой команды с уникальным ключом и
    должна возвратить сгенерированный текст патча на основе предоставленного диффа, и, при необходимости,
    вызвать обработку дочерних правил/данных.

    Первым аргументом (rule) она принимает словарь с правилом:
        {
            # Однострочная команда, не блок, не имеет чилдов
            "logic": <function default at 0x7fe22ea83510>,  # Функция для обработки правила
            "provides": [],  # Макросы, реализуемые этим правилом
            "requires": [],  # Макросы, требуемые для правила

            # Регулярка для разбора строки
            "regexp": re.compile(r"^snmp-agent\s+sys-info\s+([^\s]+).*$"),

            # Шаблон для отмены команды (в качестве аргументов следует использовать ключ)
            "reverse": "undo snmp-agent sys-info {}",
        }

    Вторым аргументом (key) идет tuple, состоящий из ключа, распаршенного из строчки с помощью regexp:
        ("contact",)  # Пример для разбора строки "snmp-agent sys-info contact"

    В третий аргумент передается словарь с диффом:
        {
            # Команды/блоки, добавленные в новой конфигурации
            Op.ADDED: [{"children": None, "row": "undo snmp-agent sys-info version all"}],

            # Бывает только в блоках, содержит изменившихся чилдов внутри блоков
            Op.AFFECTED: [],

            # Удаленные команды/блоки
            Op.REMOVED: [{"children": None, "row": "undo snmp-agent sys-info version v3"}],

            # Команды которые никак не изменились (но иногда нужны для других команд)
            Op.UNCHANGED: [{"children": None, "row": "snmp all-interfaces"}]
        }
    """
    for op in [Op.ADDED, Op.REMOVED, Op.AFFECTED, Op.MOVED]:
        # Дефолтная функция генерации патчей считает, что не бывает команд с одинаковыми
        # ключами и разным значением. При этом unchanged мы так не проверяем, поскольку
        # такие случаи возможны, когда у нас подмешиваются implicit команды
        assert 0 <= len(diff[op]) <= 1, "Too many %s actions for rows %r" % (op, [x["row"] for x in diff[op]])
    if diff[Op.AFFECTED]:
        # При изменении блока нужно вызвать обработку чилдов
        yield (True, diff[Op.AFFECTED][0]["row"], diff[Op.AFFECTED][0]["children"])
    elif diff[Op.ADDED] or diff[Op.MOVED]:
        key = Op.ADDED if diff.get(Op.ADDED) else Op.MOVED
        # При модификации строки удаление нас не интересует, добавление проходит как affected
        yield (True, diff[key][0]["row"], diff[key][0]["children"])
    elif diff[Op.REMOVED]:
        # При удалении или перемещении блока просто снести строку
        yield (False, rule["reverse"].format(*key), None)


def ordered(rule, key, diff, **kwargs):
    if diff[Op.MOVED]:
        # Сносим top-level блок
        yield (False, rule["reverse"].format(*key), None)
    # Дальше Op.MOVED будут пересозданы заново в новом порядке
    # FIXME вообще-то следовало бы удалять REMOVED из чайлдов
    # поскольку блок уже очищен и пересоздается заново
    yield from default(rule, key, diff, **kwargs)


def rewrite(rule, key, diff, **kwargs):
    # Переписывает блок игнорируя предыдущее его состояние
    if not diff[Op.REMOVED]:
        yield from default(rule, key, diff, **kwargs)


def permanent(rule, key, diff, **kwargs):
    # Данный блок не подлежат удалению
    if diff[Op.REMOVED]:
        # Если он отдельный - просто игнорируем
        if not diff[Op.REMOVED][0]["children"]:
            return
        # Если у него есть потомки - сделаем их affected
        diff[Op.AFFECTED] += diff[Op.REMOVED]
        diff[Op.REMOVED] = []
    yield from default(rule, key, diff, **kwargs)


def ignore_changes(rule, key, diff, **kwargs):
    """
    logic-функция, которая удаляет или добавляет строки, но не меняет одну на другую.
    """
    if diff[Op.ADDED] and diff[Op.REMOVED]:
        pass
    else:
        yield from default(rule, key, diff, **kwargs)


def undo_redo(rule, key, diff, **_):
    """
    Если команда отменяется через undo key, но не может быть заменена через
    key value, а требует сначала undo key, а уж потом - key value,
    этот хелпер делает именно так: сначала undo key, потом - key value
    """
    if not (diff[Op.ADDED] and diff[Op.REMOVED] and not diff[Op.AFFECTED]):
        yield from default(rule, key, diff)
    else:
        for side in [Op.REMOVED, Op.ADDED]:
            new_diff = {op: [] for op in diff.keys()}
            new_diff[side] = diff[side]
            yield from default(rule, key, new_diff)


def default_instead_undo(rule, key, diff, **_):
    # Для ряда конфигурационных строк возникает вечный diff, поскольку в конфиге строка либо явно включена,
    # либо явно выключена. Если она не описана в генераторе, т.е. мы полагаемся на дефолт, то используя default
    # вместо "no ..." мы возвращаем конфиг в дефолтное состояние.
    # NOC-20503 @lesnix 11-08-2022
    if diff[Op.REMOVED]:
        rule["reverse"] = rule["reverse"].replace("no", "default")
    yield from default(rule, key, diff)


# =====
class DiffItem(typing.NamedTuple):
    op: str
    row: str
    children: typing.List["DiffItem"]
    diff_pre: typing.Dict[str, typing.Any]


Differ = typing.Callable[[odict, odict, odict, tuple[Op, ...]], list[DiffItem]]


def default_diff(old: odict, new: odict, diff_pre: odict, _pops: tuple[Op, ...] = (Op.AFFECTED,)) -> list[DiffItem]:
    diff = base_diff(old, new, diff_pre, _pops, moved_to_affected=True)
    return diff


def ordered_diff(old: odict, new: odict, diff_pre: odict, _pops: tuple[Op, ...] = (Op.AFFECTED,)) -> list[DiffItem]:
    diff = base_diff(old, new, diff_pre, _pops, moved_to_affected=False)
    return diff


def rewrite_diff(old: odict, new: odict, diff_pre: odict, _pops: tuple[Op, ...] = (Op.AFFECTED,)) -> list[DiffItem]:
    def iter_diff(diff: list[DiffItem]) -> typing.Iterable[tuple[int, list[DiffItem]]]:
        queue = [diff]
        while queue:
            items, queue = queue[0], queue[1:]
            for i, item in enumerate(items):
                yield i, items
                queue.append(item.children)

    # оставляем маркер чтобы увидеть что мы - старший rewrite
    rewrite_marker = "rewrite"
    rewrite_tail = (rewrite_marker, _pops[-1])
    _pops = _pops + rewrite_tail
    diff = base_diff(old, new, diff_pre, _pops, moved_to_affected=False)
    # если мы rewrite верхнего уровня, и в поддереве все Op.AFFECTED
    # то есть не было совершенно никаких изменений, удаляем его из дифа
    if rewrite_marker not in _pops[: -len(rewrite_tail)]:
        if all(its[i].op == Op.AFFECTED for i, its in iter_diff(diff)):
            diff.clear()
        else:
            for i, items in iter_diff(diff):
                if items[i].op == Op.AFFECTED:
                    items[i] = items[i]._replace(op=Op.MOVED)
    return diff


def multiline_diff(old: odict, new: odict, diff_pre: odict, _pops: tuple[Op, ...] = (Op.AFFECTED,)) -> list[DiffItem]:
    """
    Особая логика diff'a для хуавейных мультилайнов.
    Она трактует все дочерние элементы %multiline-команды как
    одну общую команду, покидывая внутрь тот Op который был
    определен на верхнем уровне
    """

    def process_multiline(op, tree):
        for row, children in tree.items():
            yield op, row, list(process_multiline(op, children)), None

    ret = []
    for item in default_diff(old, new, diff_pre, _pops):
        if old.get(item.row, {}) == new.get(item.row, {}):
            continue
        op, tree = Op.ADDED, new
        if item.op == Op.REMOVED:
            op, tree = Op.REMOVED, old
        children = list(process_multiline(op, tree[item.row]))
        ret.append(DiffItem(item.op, item.row, children, item.diff_pre))

    return ret


def base_diff(
    old: odict, new: odict, diff_pre: odict, pops: tuple[Op, ...], moved_to_affected: bool = False
) -> list[DiffItem]:
    diff_indexed: typing.List[typing.Tuple[int, DiffItem]] = []
    old = _ignore_case(diff_pre, old)
    new = _ignore_case(diff_pre, new)

    for index, row in enumerate(old):
        if row not in new:
            children = call_diff_logic(diff_pre[row]["subtree"], old[row], odict(), pops + (Op.REMOVED,))
            diff_indexed.append(
                (
                    index,
                    DiffItem(
                        op=Op.REMOVED,
                        row=row,
                        children=children,
                        diff_pre=diff_pre[row]["match"],
                    ),
                )
            )

    old_indexes = {row: index for (index, row) in enumerate(old)}
    block_in_disorder = False
    parent_op = pops[-1]
    for index, row in enumerate(new):
        if row not in old:
            block_in_disorder = True
            op = Op.ADDED
        elif block_in_disorder or index != old_indexes[row]:
            block_in_disorder = True
            op = Op.MOVED if not moved_to_affected else parent_op
        else:
            op = parent_op
        children = call_diff_logic(diff_pre[row]["subtree"], old.get(row, {}), new[row], pops + (op,))
        diff_indexed.append(
            (
                index,
                DiffItem(
                    op=op,
                    row=row,
                    children=children,
                    diff_pre=diff_pre[row]["match"],
                ),
            )
        )
    diff_indexed.sort()
    return [x[1] for x in diff_indexed]


def call_diff_logic(diff_pre: odict, old: odict, new: odict, pops: tuple[Op, ...] = (Op.AFFECTED,)):
    """
    Группируем команды в старом и новом конфиге согласно выставленным
    в рулбуке атрибутам %diff_logic и вызывает их поочереди согласно
    порядку команд в old и new, предпочитая old (т.е. сначала удаления)
    """
    diff_logics: odict = odict()
    for row in old:
        logic = diff_pre[row]["match"]["attrs"]["diff_logic"]
        if logic not in diff_logics:
            diff_logics[logic] = (odict(), odict())
        diff_logics[logic][0][row] = old[row]
    for row in new:
        logic = diff_pre[row]["match"]["attrs"]["diff_logic"]
        if logic not in diff_logics:
            diff_logics[logic] = (odict(), odict())
        diff_logics[logic][1][row] = new[row]
    ret = []
    for logic, (old, new) in diff_logics.items():
        ret.extend(logic(old=old, new=new, diff_pre=diff_pre, _pops=pops))
    return ret


def _ignore_case(diff_pre, cfg):
    has_ignore_case = False
    for row in diff_pre:
        if diff_pre[row]["match"]["attrs"]["ignore_case"]:
            has_ignore_case = True
    if not has_ignore_case:
        return cfg

    ret = cfg.__class__()
    for row in cfg:
        new_row = row
        if diff_pre[row]["match"]["attrs"]["ignore_case"]:
            new_row = row.lower()
        ret[new_row] = cfg[row]
        diff_pre[new_row] = diff_pre[row]
    return ret


# ====


class ApplyItem(typing.NamedTuple):
    before: CommandList
    after: CommandList


def apply(hw, do_commit, do_finalize, path):
    return registry_connector.get().match(hw).apply(hw, do_commit, do_finalize, path)
