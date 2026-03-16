import abc
import bisect
import contextlib
import functools
import ipaddress
import itertools
import math
import os
import pathlib
import re
import socket
import sys
import textwrap
import typing
from collections import OrderedDict as odict
from collections.abc import Iterable
from functools import lru_cache
from typing import List, NamedTuple, Optional, Tuple, Union

import contextlog
import mako.template


_logger = contextlog.get_logger()


# =====
class HuaweiNumBlock(metaclass=abc.ABCMeta):
    def __init__(self, generator, block_name, start=5, step=5):
        self._gen = generator
        self._block_name = block_name
        self._rule_num = start
        self._rule_step = step
        self._block = None

    def _current(self):
        (current, self._rule_num) = (self._rule_num, self._rule_num + self._rule_step)
        return current

    def __enter__(self):
        self._block = self._gen.block(self._block_name)
        self._block.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, trace):
        self._block.__exit__(exc_type, exc_val, trace)
        self._block = None

    def __call__(self, rule_str, min_number=None):
        if min_number is not None:
            self._rule_num = max(self._rule_num, int(min_number))
        return self.format_rule_string(rule_str)

    @abc.abstractmethod
    def format_rule_string(self, rule_str):
        pass


def huawei_expand_vlandb(row):
    expanded = set()
    row_parts = row.split()
    for index, part in enumerate(row_parts):
        if part == "to":
            left = int(row_parts[index - 1])
            right = int(row_parts[index + 1])
            expanded = expanded.union(range(left + 1, right))
        else:
            expanded.add(int(part))
    return expanded


def cisco_expand_vlandb(row):
    expanded = set()
    for part in row.split(","):
        range_parts = [int(p.strip()) for p in part.strip().split("-")]
        assert len(range_parts) in (1, 2)
        if len(range_parts) > 1:
            left = range_parts[0]
            right = range_parts[1]
            expanded.update(range(left, right + 1))
        else:
            expanded.add(int(range_parts[0]))
    return expanded


def huawei_collapse_vlandb(vlans, chunk_len=0):
    return collapse_vlandb(vlans, " to ", chunk_len=chunk_len)


def cisco_collapse_vlandb(vlans, tiny_ranges=True):
    return collapse_vlandb(vlans, "-", tiny_ranges)


def juniper_fmt_prefix_lists_acl(prefix_list_names: typing.Iterable[str]) -> str:
    """Форматирует строку для acl из prefix-list-имён.

    Производит следующие операции:

    - вырезает дубликаты
    - сортирует
    - добавляет quoted-вариант имени (NOCDEV-5321)
    - соединяет всё в строку
    """
    sorted_unique_names = sorted(frozenset(prefix_list_names))
    quoted_and_unquoted_names = []
    for prefix_list_name in sorted_unique_names:
        quoted_and_unquoted_names.append(prefix_list_name)
        quoted_and_unquoted_names.append('"{}"'.format(prefix_list_name))
    joined_prefix_lists = "|".join(quoted_and_unquoted_names)
    return joined_prefix_lists


# tiny_ranges - если выставлен в False, то два рядом стоящих влана не конвертятся в
# рендж (поведение cisco catalyst)
def collapse_vlandb(vlans, range_sep, tiny_ranges=True, chunk_len=0):
    assert len(vlans) != 0
    vlans = sorted(set(vlans))
    res = []
    row = [vlans[0], vlans[0]]
    for vlan in vlans[1::]:
        if row[1] == (vlan - 1):
            row[1] = vlan
        elif not tiny_ranges and row[1] - row[0] == 1:
            res.append([row[0], row[0]])
            res.append([row[1], row[1]])
            row = [vlan, vlan]
        else:
            res.append(row)
            row = [vlan, vlan]
    res.append(row)
    res = list(map(lambda x: x[0] != x[1] and "%s%s%s" % (x[0], range_sep, x[1]) or str(x[0]), res))

    # Устройства бьют списки вланов на чанки при добавлении в конфиг
    if chunk_len:
        chunks = [res[i : i + chunk_len] for i in range(0, len(res), chunk_len)]
        return chunks

    return res


def huawei_iface_ranges(iface_names):
    ret = []
    grouped = odict()
    for iface in iface_names:
        match = re.match(r"(.*[:/])(\d+)$", iface)
        if not match:
            ret.append(iface)
        else:
            prefix, index = match.groups()
            grouped.setdefault(prefix, []).append((int(index), iface))

    for ifaces in grouped.values():
        start = 0
        while start < len(ifaces):
            end = start
            while end + 1 < len(ifaces) and ifaces[end + 1][0] == ifaces[end][0] + 1:
                end += 1
            if start == end:
                ret.append(ifaces[start][1])
            else:
                ret.append((ifaces[start][1], ifaces[end][1]))
            start = end + 1

    return ret


def juniper_port_split(iface_name):
    ret = dict(zip(["speed", "fpc", "pic", "port"], re.split(r"-+|/+|:", iface_name)))
    return ret


def make_ip4_mask(prefix_len, inverse=False):
    """
    Returns dotted-quad string representing IPv4 mask of given length

    E.g., for prefix_len=25 it returns "255.255.255.128"
    """
    if not isinstance(prefix_len, int) or prefix_len < 0 or prefix_len > 32:
        raise ValueError("invalid prefix_len %r" % prefix_len)
    bin_mask = (0xFFFFFFFF & (0xFFFFFFFF << (32 - prefix_len))).to_bytes(4, byteorder="big")
    if inverse:
        bin_mask = bytes(byte ^ 255 for byte in bin_mask)
    return socket.inet_ntop(socket.AF_INET, bin_mask)


def merge_dicts(*args: dict | odict) -> odict:
    if len(args) == 0:
        return odict()
    if len(args) == 1:
        return args[0]
    if all(map(lambda x: x[0] == x[1], zip(args[:-1], args[1:]))):
        return args[0]

    merged = odict()
    for dictionary in args:
        assert isinstance(dictionary, (dict, odict))
        for key, value in dictionary.items():
            if isinstance(value, (dict, odict)):
                merged[key] = merge_dicts(*[x[key] for x in args if key in x])
            elif key not in merged:
                merged[key] = value
            elif isinstance(merged[key], (list, tuple)):
                merged[key] = merged[key].__class__(itertools.chain(merged[key], value))
            else:
                merged[key] = value
    return merged


def meta_decor(*method_list):
    def wrapper(*args, **kwds):
        for method in method_list:
            res = method(*args, **kwds)
        return res

    return wrapper


def first(iterable, default=None):
    return next(iter(iterable), default)


def flatten(iterable):
    for x in iterable:
        if not isinstance(x, (str, bytes)) and isinstance(x, Iterable):
            yield from flatten(x)
        else:
            yield x


def uniq(*iterables):
    seen = set()
    for iterable in iterables:
        for item in iterable:
            if item not in seen:
                seen.add(item)
                yield item


# Хелпер чтобы при итерировании по вложенным структурам
# правильно подбирать значения и флаги (local_as, update_source, multipath, etc)
# которые должны наследоваться от верхних блоков к нижним
# XXX Покрыть тестами
class ContextOrderedDict:
    def __init__(self, iterable):
        self.dict = odict(iterable)

    @contextlib.contextmanager
    def update(self, peers_sub_dict):
        values_backup = self.dict.copy()
        for key in set(self.dict).intersection(peers_sub_dict.keys()):
            if not isinstance(peers_sub_dict[key], dict):
                self.dict[key] = peers_sub_dict[key]
        yield
        self.dict = values_backup
        del values_backup


# {{{ http://code.activestate.com/recipes/511478/ (r1)
def percentile(list_numbers, percent, key=lambda x: x):
    """
    Find the percentile of a list of values.

    @parameter N - is a list of values.
    @parameter percent - a float value from 0.0 to 1.0.
    @parameter key - optional key function to compute value from each element of N.

    @return - the percentile of the values
    """
    if not list_numbers:
        return None
    list_numbers = sorted(list_numbers, key=key)
    kkk = (len(list_numbers) - 1) * percent
    floo = math.floor(kkk)
    cei = math.ceil(kkk)
    if floo == cei:
        return key(list_numbers[int(kkk)])
    d0 = key(list_numbers[int(floo)]) * (cei - kkk)
    d1 = key(list_numbers[int(cei)]) * (kkk - floo)
    return d0 + d1


def mako_render(template, dedent=False, **kwargs):
    @lru_cache(None)
    def _compile_mako(template, dedent):
        if dedent:
            template = textwrap.dedent(template).strip()
        return mako.template.Template(template)

    ret = _compile_mako(template, dedent).render(**kwargs)
    if dedent:
        ret = ret.strip()
    return ret


# =====
def find_exc_in_stack(
    container_exc: Exception, target_exc_type: Union[type, Tuple[type, ...]]
) -> Optional[BaseException]:
    curr_err: Optional[BaseException] = container_exc
    while curr_err is not None:
        if isinstance(curr_err, target_exc_type):
            return curr_err
        curr_err = curr_err.__context__
    return None


def find_modules(base_dir):
    """
    Рекурсивно ищем в base_dir файлы python модулей
    """
    for entry in os.scandir(base_dir):
        if entry.name[:1] in ["_", "."]:
            continue

        if entry.is_dir():
            child_dir = os.path.join(base_dir, entry.name)
            yield from (".".join((entry.name, child_name)) for child_name in find_modules(child_dir))
        elif entry.is_file() and entry.name.endswith(".py"):
            yield entry.name[:-3]


def catch_ctrl_c(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except KeyboardInterrupt:
            _logger.error("Got Ctrl+C, exiting")
            sys.exit(127)

    return wrapper


class LMSegment(NamedTuple):
    """
    LMSegment хранит подсеть в виде целочисленных пар (int(network_addr), int(broadcast_addr)):
    0.0.0.0/0:  (0b0000000000000000000000000, 0b1111111111111111111111111)
    0.0.0.0/16: (0b0000000000000000000000000, 0b0000000000001111111111111)
    1.0.0.0/24: (0b1000000000000000000000000, 0b1000000000000000011111111)

    Для двух x=(A, B), y=(C, D) таких пар (A <= B, C <= D по определению):

    Отношение включения: x contains y <-> A <= C && D <= B:
    1.0.0.0/24 contains 1.0.0.1/32
    0.0.0.0/0 contains 0.0.0.0/16

    Отношение порядка: x < y <-> A < C || (A == C && B > D), или проще (A, -B) < (C, -D):
    1.0.0.0/24 < 1.0.0.1/32
    0.0.0.0/0  < 0.0.0.0/16
    """

    begin: int
    end: int

    def contains(self, other: "LMSegment") -> bool:
        return self.begin <= other.begin and other.end <= self.end

    @classmethod
    def from_net(cls, net: Union[ipaddress.IPv4Network, ipaddress.IPv6Network]):
        return cls(
            begin=int(net.network_address),
            end=int(net.network_address) | int(net.hostmask),
        )

    def _cmp_identity(self) -> Tuple[int, int]:
        return (self.begin, -self.end)

    def __eq__(self, other: "LMSegment"):
        return self._cmp_identity() == other._cmp_identity()

    def __lt__(self, other: "LMSegment"):
        return self._cmp_identity() < other._cmp_identity()

    def __le__(self, other: "LMSegment"):
        return self._cmp_identity() <= other._cmp_identity()

    def __ge__(self, other: "LMSegment"):
        return self._cmp_identity() >= other._cmp_identity()

    def __gt__(self, other: "LMSegment"):
        return self._cmp_identity() > other._cmp_identity()


class LMSegmentList:
    """Упорядоченный список подсетей"""

    def __init__(self):
        self.pfxs: List[LMSegment] = []

    def add(self, pref: LMSegment) -> None:
        """Добавляем новый префикс в упорядоченный список, если он не дублируется"""
        idx = bisect.bisect(self.pfxs, pref) - 1
        if 0 <= idx < len(self.pfxs):
            if pref == self.pfxs[idx]:
                return
        self.pfxs.insert(idx + 1, pref)

    def find(self, target: LMSegment) -> Optional[LMSegment]:
        """LPM поиск в добавленных"""
        upper_bound = bisect.bisect(self.pfxs, target)
        for i in reversed(range(0, upper_bound)):
            if self.pfxs[i].contains(target):
                return self.pfxs[i]
            if self.pfxs[i] > target:
                break
        return None


class LMSMatcher:
    """Обертка над парой LMSegmentList над парой LMSegmentList для v4/v6"""

    def __init__(self):
        self.v4 = LMSegmentList()
        self.v6 = LMSegmentList()

    def add(self, pref: str) -> None:
        net = ipaddress.ip_network(pref)
        self._af(net).add(LMSegment.from_net(net))

    def find(self, pref: str) -> Optional[str]:
        net = ipaddress.ip_network(pref)
        found = self._af(net).find(LMSegment.from_net(net))
        if found:
            net_addr = net.__class__(found.begin).network_address
            net_hostmask = (found.begin ^ found.end).bit_length()
            net_mask = net_addr.max_prefixlen - net_hostmask
            return str(net_addr) + "/" + str(net_mask)

    def _af(self, net):
        if net.version == 4:
            return self.v4
        elif net.version == 6:
            return self.v6
        else:
            raise ValueError(str(net))


def is_relative(path: pathlib.PurePath, *other: str) -> bool:
    """Проверяет является ли путь path относительным любого из other"""
    for checkpath in other:
        try:
            path.relative_to(checkpath)
            return True
        except ValueError:
            pass
    return False


_ANNOTATION_SEP = "\t# "


def add_annotation(row: str, annotation: str) -> str:
    return _ANNOTATION_SEP.join((row, annotation))


def strip_annotation(row: str) -> str:
    return row.rsplit(_ANNOTATION_SEP, 1)[0]


def jun_inactive_pfx() -> str:
    return "inactive: "


def jun_is_inactive(key) -> bool:
    return key.startswith(jun_inactive_pfx())


def jun_activate(key) -> str:
    if jun_is_inactive(key):
        key = key[len(jun_inactive_pfx()) :]
    return key


# TODO: Remove with python3.10
async def anext(async_iter):
    return await async_iter.__anext__()  # pylint: disable=unnecessary-dunder-call
