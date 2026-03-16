from shapely.wkb import dumps, loads
from shapely.geometry import Point
import binascii


def bin2hex(value):
    return binascii.b2a_hex(value).upper().decode("utf-8")


def hex2bin(value):
    return binascii.a2b_hex(value)


def test_dumps_srid():
    p1 = Point(1.2, 3.4)
    result = dumps(p1)
    assert bin2hex(result) == "0101000000333333333333F33F3333333333330B40"
    result = dumps(p1, srid=4326)
    assert bin2hex(result) == "0101000020E6100000333333333333F33F3333333333330B40"


def test_dumps_endianness():
    p1 = Point(1.2, 3.4)
    result = dumps(p1)
    assert bin2hex(result) == "0101000000333333333333F33F3333333333330B40"
    result = dumps(p1, big_endian=False)
    assert bin2hex(result) == "0101000000333333333333F33F3333333333330B40"
    result = dumps(p1, big_endian=True)
    assert bin2hex(result) == "00000000013FF3333333333333400B333333333333"


def test_dumps_hex():
    p1 = Point(1.2, 3.4)
    result = dumps(p1, hex=True)
    assert result == "0101000000333333333333F33F3333333333330B40"
    

def test_loads_srid():
    # load a geometry which includes an srid
    geom = loads(hex2bin("0101000020E6100000333333333333F33F3333333333330B40"))
    assert isinstance(geom, Point)
    assert geom.coords[:] == [(1.2, 3.4)]
    # by default srid is not exported
    result = dumps(geom)
    assert bin2hex(result) == "0101000000333333333333F33F3333333333330B40"
    # include the srid in the output
    result = dumps(geom, include_srid=True)
    assert bin2hex(result) == "0101000020E6100000333333333333F33F3333333333330B40"
    # replace geometry srid with another
    result = dumps(geom, srid=27700)
    assert bin2hex(result) == "0101000020346C0000333333333333F33F3333333333330B40"
