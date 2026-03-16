import copy
import re
from collections import namedtuple

from contextlog import get_logger

from annet.annlib.types import Op
from annet.rulebook import common


class VRPVersion(namedtuple("VRPVersionBase", ["V", "R", "C", "SPC"])):
    ANY = object()
    ATTR_NAMES = ["V", "R", "C", "SPC"]

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return False

        for attr_name in self.ATTR_NAMES:
            self_attr = getattr(self, attr_name)
            if self_attr is self.ANY:
                continue
            other_attr = getattr(other, attr_name)
            if other_attr is self.ANY:
                continue

            if self_attr != other_attr:
                return False

        return True

    def __ne__(self, other):
        return not self == other


def parse_version(version: str) -> VRPVersion:
    # CP - Cold Patch
    # HP - Hot Patch
    if not version:
        # FIXME: возможно, если в RT нет данных, надо спрашивать у самого устройства?
        version = "VRP V200R002C50SPC800"
        get_logger().warning("SW version not set, falling back to %r", version)
    res = re.match(r"(?:VRP )?V(?P<v>\d+)R(?P<r>\d+)C(?P<c>\d+)(SPC(?P<spc>\d+))?(?P<opt>T)?", version)
    assert res is not None, f"can't parse version '{version}'"
    m = res.groupdict()  # pylint: disable=invalid-name
    return VRPVersion(int(m["v"]), int(m["r"]), int(m["c"] or 0), int(m["spc"] or 0))


# =====
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
    # для того чтобы опредилить полностью ли изменяется
    # префикс лист в рулбуке huawei.rul описан ключ (family, name)
    # однако с точки зрения команды каждый индекс - отдельная команда
    # поэтому мы группируем их по индексу тут и передаем в common
    diff_by_index = {}
    for op, rows in diff.items():
        for row in rows:
            # ожидаемый формат команды префикс-листа
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
        # поскольку исходно у нас в ключе правила нет индекса
        # нужно добавить его туда иначе undo-правило будет без оного
        indexed_rule = copy.deepcopy(rule)
        indexed_rule["reverse"] = "undo ip {}-prefix {} index {}"

        # stub_index референсится в рулбуке huawei.order чтобы обеспечить
        # добавление/удаление стаба в первую/последнюю очередь
        stub, stub_index = "", 99999999

        # если мы только добавляем новые команды (например создаем) в префик-лист
        # либо удаляем/двигаем но при этом у нас есть не изменяемые части
        # хуавей не будет считать лист удаляемым и стаб-правило не нужно
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
    Для отката статического маршрута фактически необходимо передавать почти все аргументы,
    кроме различных track ...
    При этом, аргументов может быть разное количество - опциональный VRF, опциональный интерфейс.
    Поэтому мы не парсим саму команду, а только удаляем ненужные аргументы.
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


def undo_trust(rule, key, diff, hw, **_):
    """на CE свитчах команда undo trust; на S undo trust *"""
    if diff[Op.REMOVED]:
        if hw.Quidway and not hw.S6700:
            yield False, "undo trust %s" % key, None
        else:
            yield False, "undo trust", None
    else:
        yield from common.default(rule, key, diff)


def port_queue(rule, key, diff, **_):
    """
    Для отката конфигурации port-queue на интерфейсе требуется только частичное указание параметров.
    Пример отключения/включения:
    interface 100GE0/1/33
        undo port-queue af3 wfq outbound
        port-queue af3 wfq weight 30 port-wred WRED outbound

    По сути на убрать все параметры между 'wfq' и 'outbound'
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


def old_snmp_iface_trap_undo(rule, key, diff, hw, **_):
    # хитрая логика для старый хуавеев
    # тут вместо полной команды с undo нужно сгенерить не полную строку
    if diff[Op.REMOVED]:
        if hw.Quidway:
            yield False, "undo mac-address trap notification", None
        else:
            yield False, "undo mac-address trap notification learn", None
    else:
        yield from common.default(rule, key, diff)


def stelnet(rule, key, diff, **_):
    # не заменяем строки stelnet ipv4 server enable и stelnet ipv6 server enable на stelnet server enable
    # чтобы не дергать SSH
    if diff[Op.REMOVED] and diff[Op.ADDED]:
        removed = {x["row"] for x in diff[Op.REMOVED]}
        added = {x["row"] for x in diff[Op.ADDED]}
        if removed == {"stelnet ipv4 server enable", "stelnet ipv6 server enable"} and added == {
            "stelnet server enable"
        }:
            return
    yield from common.default(rule, key, diff)


def snmpagent_sysinfo_version(rule, key, diff, hw, **_):
    if hw.Huawei.CE and (diff[Op.ADDED] or diff[Op.REMOVED]):
        assert len(diff[Op.AFFECTED]) == 0, "WTF? Affected not empty: %r" % (diff[Op.AFFECTED])
        versions = set(["v1", "v2c", "v3"])

        result = set()
        for op in [Op.REMOVED, Op.ADDED]:
            for action in diff[op]:
                args = action["row"].split()[3:]
                assert len(args) > 0, "Empty op %r: %r" % (op, action["row"])

                if args[-1] == "disable":
                    args = args[:-1]
                    disable = True
                else:
                    disable = False
                if "all" in args:
                    args = versions
                else:
                    assert len(set(args).difference(versions)) == 0, "Incorrect args: %r" % (args)

                if (op == Op.REMOVED and disable) or (op == Op.ADDED and not disable):
                    result.update(args)
                else:
                    result.difference_update(args)

        if result == versions:
            yield (True, "snmp-agent sys-info version all", None)
        else:
            yield (False, "snmp-agent sys-info version all disable", None)
            yield (True, "snmp-agent sys-info version %s" % (" ".join(result)), None)
    else:
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
    # если type меняется нужно сначала удалить все if-match
    # а после этого пересоздать classifier
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

    # Приходится самим говорить undo поскольку мы притворяемся одним блоком
    for subdiff in diff[Op.REMOVED]:
        # Сначала нужно удалить все group-member'ы
        if diff[Op.REMOVED][0]["children"]:
            yield (True, diff[Op.REMOVED][0]["row"], diff[Op.REMOVED][0]["children"])
        yield False, "undo " + subdiff["row"], None
    # Сначала разбираем affected, там внутри могут быть undo
    for subdiff in sorted(diff[Op.AFFECTED], key=removed_count, reverse=True):
        yield from common_default(Op.AFFECTED, subdiff)
    for subdiff in diff[Op.ADDED]:
        yield from common_default(Op.ADDED, subdiff)


def clear_instead_undo(rule, key, diff, **_):
    # Для ряда конфигурационных строк возникает вечный diff, поскольку в конфиге строка либо явно включена,
    # либо явно выключена. Если она не описана в генераторе, т.е. мы полагаемся на дефолт, то используя clear
    # вместо undo мы возвращаем конфиг в дефолтное состояние.
    # NOC-20102 @gslv 11-02-2022
    if diff[Op.REMOVED]:
        if diff[Op.REMOVED][0]["row"].endswith(" disable"):
            cmd = diff[Op.REMOVED][0]["row"].replace(" disable", "")
        yield (True, "clear " + cmd, False)
    else:
        yield from common.default(rule, key, diff)
