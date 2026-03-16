import sys
from collections import namedtuple
from typing import List, Any, Callable, Optional, Union

from fakeredis import _msgs as msgs
from fakeredis._command_args_parsing import extract_args
from fakeredis._commands import command, Key, Float, CommandItem
from fakeredis._helpers import SimpleError, Database
from fakeredis.model import ZSet
from fakeredis.geo import distance, geo_encode, geo_decode


UNIT_TO_M = {"km": 0.001, "mi": 0.000621371, "ft": 3.28084, "m": 1}


def translate_meters_to_unit(unit_arg: bytes) -> float:
    """number of meters in a unit.
    :param unit_arg: unit name (km, mi, ft, m)
    :returns: number of meters in unit
    """
    unit = UNIT_TO_M.get(unit_arg.decode().lower())
    if unit is None:
        raise SimpleError(msgs.GEO_UNSUPPORTED_UNIT)
    return unit


GeoResult = namedtuple("GeoResult", "name long lat hash distance")


def _parse_results(items: List[GeoResult], withcoord: bool, withdist: bool) -> List[Any]:
    """Parse list of GeoResults to redis response
    :param withcoord: include coordinates in response
    :param withdist: include distance in response
    :returns: Parsed list
    """
    res = list()
    for item in items:
        new_item = [
            item.name,
        ]
        if withdist:
            new_item.append(Float.encode(item.distance, False))
        if withcoord:
            new_item.append([Float.encode(item.long, False), Float.encode(item.lat, False)])
        if len(new_item) == 1:
            new_item = new_item[0]
        res.append(new_item)
    return res


def _find_near(
    zset: ZSet,
    lat: float,
    long: float,
    radius: float,
    conv: float,
    count: int,
    count_any: bool,
    desc: bool,
) -> List[GeoResult]:
    """Find items within area (lat,long)+radius
    :param zset: list of items to check
    :param lat: latitude
    :param long: longitude
    :param radius: radius in whatever units
    :param conv: conversion of radius to meters
    :param count: number of results to give
    :param count_any: should we return any results that match? (vs. sorted)
    :param desc: should results be sorted descending order?
    :returns: List of GeoResults
    """
    results = list()
    for name, _hash in zset.items():
        p_lat, p_long, _, _ = geo_decode(_hash)
        dist = distance((p_lat, p_long), (lat, long)) * conv
        if dist < radius:
            results.append(GeoResult(name, p_long, p_lat, _hash, dist))
            if count_any and len(results) >= count:
                break
    results = sorted(results, key=lambda x: x.distance, reverse=desc)
    if count:
        results = results[:count]
    return results


class GeoCommandsMixin:
    _encodefloat: Callable[[float, bool], bytes]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super(GeoCommandsMixin, self).__init__(*args, **kwargs)
        self._db: Database

    def _store_geo_results(self, item_name: bytes, geo_results: List[GeoResult], scoredist: bool) -> int:
        db_item = CommandItem(item_name, self._db, item=self._db.get(item_name), default=ZSet())
        db_item.value = ZSet()
        for item in geo_results:
            val = item.distance if scoredist else item.hash
            db_item.value.add(item.name, val)
        db_item.writeback()
        return len(geo_results)

    @command(name="GEOADD", fixed=(Key(ZSet),), repeat=(bytes,))
    def geoadd(self, key: CommandItem, *args: bytes) -> int:
        (xx, nx, ch), data = extract_args(
            args,
            ("nx", "xx", "ch"),
            error_on_unexpected=False,
            left_from_first_unexpected=True,
        )
        if xx and nx:
            raise SimpleError(msgs.NX_XX_GT_LT_ERROR_MSG)
        if len(data) == 0 or len(data) % 3 != 0:
            raise SimpleError(msgs.SYNTAX_ERROR_MSG)
        zset = key.value
        old_len, changed_items = len(zset), 0
        for i in range(0, len(data), 3):
            long, lat, name = (
                Float.decode(data[i + 0]),
                Float.decode(data[i + 1]),
                data[i + 2],
            )
            if (name in zset and not xx) or (name not in zset and not nx):
                if zset.add(name, geo_encode(lat, long, 10)):
                    changed_items += 1
        if changed_items:
            key.updated()
        if ch:
            return changed_items
        return len(zset) - old_len

    @command(name="GEOHASH", fixed=(Key(ZSet), bytes), repeat=(bytes,))
    def geohash(self, key: CommandItem, *members: bytes) -> List[bytes]:
        hashes = map(key.value.get, members)
        geohash_list = [((x + "0").encode() if x is not None else x) for x in hashes]
        return geohash_list

    @command(name="GEOPOS", fixed=(Key(ZSet), bytes), repeat=(bytes,))
    def geopos(self, key: CommandItem, *members: bytes) -> List[Optional[List[bytes]]]:
        gospositions = map(
            lambda x: geo_decode(x) if x is not None else x,
            map(key.value.get, members),
        )
        res = [
            (
                [
                    self._encodefloat(x[1], False),
                    self._encodefloat(x[0], False),
                ]
                if x is not None
                else None
            )
            for x in gospositions
        ]
        return res

    @command(name="GEODIST", fixed=(Key(ZSet), bytes, bytes), repeat=(bytes,))
    def geodist(self, key: CommandItem, m1: bytes, m2: bytes, *args: bytes) -> Optional[float]:
        geohashes = [key.value.get(m1), key.value.get(m2)]
        if any(elem is None for elem in geohashes):
            return None
        geo_locs = [geo_decode(x) for x in geohashes]
        res = distance((geo_locs[0][0], geo_locs[0][1]), (geo_locs[1][0], geo_locs[1][1]))
        unit = translate_meters_to_unit(args[0]) if len(args) == 1 else 1
        return res * unit

    def _search(
        self,
        key: CommandItem,
        long: float,
        lat: float,
        radius: float,
        conv: float,
        withcoord: bool,
        withdist: bool,
        _: bool,
        count: int,
        count_any: bool,
        desc: bool,
        store: Optional[bytes],
        storedist: Optional[bytes],
    ) -> Union[List[Any], int]:
        zset = key.value
        geo_results = _find_near(zset, lat, long, radius, conv, count, count_any, desc)

        if store:
            self._store_geo_results(store, geo_results, scoredist=False)
            return len(geo_results)
        if storedist:
            self._store_geo_results(storedist, geo_results, scoredist=True)
            return len(geo_results)
        ret = _parse_results(geo_results, withcoord, withdist)
        return ret

    @command(name="GEORADIUS_RO", fixed=(Key(ZSet), Float, Float, Float), repeat=(bytes,))
    def georadius_ro(self, key: CommandItem, long: float, lat: float, radius: float, *args: bytes) -> List[Any]:
        (withcoord, withdist, withhash, count, count_any, desc), left_args = extract_args(
            args,
            ("withcoord", "withdist", "withhash", "+count", "any", "desc"),
            error_on_unexpected=False,
            left_from_first_unexpected=False,
        )
        count = count or sys.maxsize
        conv: float = translate_meters_to_unit(args[0]) if len(args) >= 1 else 1.0
        res: List[Any] = self._search(  # type: ignore
            key, long, lat, radius, conv, withcoord, withdist, withhash, count, count_any, desc, None, None
        )
        return res

    @command(name="GEORADIUS", fixed=(Key(ZSet), Float, Float, Float), repeat=(bytes,))
    def georadius(self, key: CommandItem, long: float, lat: float, radius: float, *args: bytes) -> List[Any]:
        (withcoord, withdist, withhash, count, count_any, desc, store, storedist), left_args = extract_args(
            args,
            ("withcoord", "withdist", "withhash", "+count", "any", "desc", "*store", "*storedist"),
            error_on_unexpected=False,
            left_from_first_unexpected=False,
        )
        count = count or sys.maxsize
        conv = translate_meters_to_unit(args[0]) if len(args) >= 1 else 1
        res: List[Any] = self._search(  # type: ignore
            key,
            long,
            lat,
            radius,
            conv,
            withcoord,
            withdist,
            withhash,
            count,
            count_any,
            desc,
            store,
            storedist,
        )
        return res

    @command(name="GEORADIUSBYMEMBER", fixed=(Key(ZSet), bytes, Float), repeat=(bytes,))
    def georadiusbymember(self, key: CommandItem, member_name: bytes, radius: float, *args: bytes):
        member_score = key.value.get(member_name)
        lat, long, _, _ = geo_decode(member_score)
        return self.georadius(key, long, lat, radius, *args)

    @command(name="GEORADIUSBYMEMBER_RO", fixed=(Key(ZSet), bytes, Float), repeat=(bytes,))
    def georadiusbymember_ro(self, key: CommandItem, member_name: bytes, radius: float, *args: float) -> List[Any]:
        member_score = key.value.get(member_name)
        lat, long, _, _ = geo_decode(member_score)
        return self.georadius_ro(key, long, lat, radius, *args)

    @command(name="GEOSEARCH", fixed=(Key(ZSet),), repeat=(bytes,))
    def geosearch(self, key: CommandItem, *args: bytes) -> List[Any]:
        (frommember, (long, lat), radius), left_args = extract_args(
            args,
            ("*frommember", "..fromlonlat", ".byradius"),
            error_on_unexpected=False,
            left_from_first_unexpected=False,
        )
        if frommember is None and long is None:
            raise SimpleError(msgs.SYNTAX_ERROR_MSG)
        if frommember is not None and long is not None:
            raise SimpleError(msgs.SYNTAX_ERROR_MSG)
        if frommember:
            return self.georadiusbymember_ro(key, frommember, radius, *left_args)
        else:
            return self.georadius_ro(key, long, lat, radius, *left_args)  # type: ignore

    @command(
        name="GEOSEARCHSTORE",
        fixed=(
            bytes,
            Key(ZSet),
        ),
        repeat=(bytes,),
    )
    def geosearchstore(self, dst: bytes, src: CommandItem, *args: bytes) -> List[Any]:
        (frommember, (long, lat), radius, storedist), left_args = extract_args(
            args,
            ("*frommember", "..fromlonlat", ".byradius", "storedist"),
            error_on_unexpected=False,
            left_from_first_unexpected=False,
        )
        if frommember is None and long is None:
            raise SimpleError(msgs.SYNTAX_ERROR_MSG)
        if frommember is not None and long is not None:
            raise SimpleError(msgs.SYNTAX_ERROR_MSG)
        additional = [b"storedist", dst] if storedist else [b"store", dst]

        if frommember:
            return self.georadiusbymember(src, frommember, radius, *left_args, *additional)
        else:
            return self.georadius(src, long, lat, radius, *left_args, *additional)  # type: ignore
