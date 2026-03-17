"""Test of deprecations following RFC 1"""

import pytest

from fiona.errors import FionaDeprecationWarning
from fiona.model import (
    _Geometry,
    Feature,
    Geometry,
    Object,
    ObjectEncoder,
    Properties,
    decode_object,
)


def test_object_len():
    """object len is correct"""
    obj = Object(g=1)
    assert len(obj) == 1


def test_object_iter():
    """object iter is correct"""
    obj = Object(g=1)
    assert [obj[k] for k in obj] == [1]


def test_object_setitem_warning():
    """Warn about __setitem__"""
    obj = Object()
    with pytest.warns(FionaDeprecationWarning, match="immutable"):
        obj["g"] = 1
    assert "g" in obj
    assert obj["g"] == 1


def test_object_update_warning():
    """Warn about update"""
    obj = Object()
    with pytest.warns(FionaDeprecationWarning, match="immutable"):
        obj.update(g=1)
    assert "g" in obj
    assert obj["g"] == 1


def test_object_popitem_warning():
    """Warn about pop"""
    obj = Object(g=1)
    with pytest.warns(FionaDeprecationWarning, match="immutable"):
        assert obj.pop("g") == 1
    assert "g" not in obj


def test_object_delitem_warning():
    """Warn about __delitem__"""
    obj = Object(g=1)
    with pytest.warns(FionaDeprecationWarning, match="immutable"):
        del obj["g"]
    assert "g" not in obj


def test_object_setitem_delegated():
    """Delegation in __setitem__ works"""

    class ThingDelegate:
        def __init__(self, value):
            self.value = value

    class Thing(Object):
        _delegated_properties = ["value"]

        def __init__(self, value=None, **data):
            self._delegate = ThingDelegate(value)
            super().__init__(**data)

    thing = Thing()
    assert thing["value"] is None
    with pytest.warns(FionaDeprecationWarning, match="immutable"):
        thing["value"] = 1
    assert thing["value"] == 1


def test_object_delitem_delegated():
    """Delegation in __delitem__ works"""

    class ThingDelegate:
        def __init__(self, value):
            self.value = value

    class Thing(Object):
        _delegated_properties = ["value"]

        def __init__(self, value=None, **data):
            self._delegate = ThingDelegate(value)
            super().__init__(**data)

    thing = Thing(1)
    assert thing["value"] == 1
    with pytest.warns(FionaDeprecationWarning, match="immutable"):
        del thing["value"]
    assert thing["value"] is None


def test__geometry_ctor():
    """Construction of a _Geometry works"""
    geom = _Geometry(type="Point", coordinates=(0, 0))
    assert geom.type == "Point"
    assert geom.coordinates == (0, 0)


def test_geometry_type():
    """Geometry has a type"""
    geom = Geometry(type="Point")
    assert geom.type == "Point"


def test_geometry_coordinates():
    """Geometry has coordinates"""
    geom = Geometry(coordinates=[(0, 0), (1, 1)])
    assert geom.coordinates == [(0, 0), (1, 1)]


def test_geometry__props():
    """Geometry properties as a dict"""
    assert Geometry(coordinates=(0, 0), type="Point")._props() == {
        "coordinates": (0, 0),
        "type": "Point",
        "geometries": None,
    }


def test_geometry_gi():
    """Geometry __geo_interface__"""
    gi = Geometry(coordinates=(0, 0), type="Point", geometries=[]).__geo_interface__
    assert gi["type"] == "Point"
    assert gi["coordinates"] == (0, 0)


def test_feature_no_geometry():
    """Feature has no attribute"""
    feat = Feature()
    assert feat.geometry is None


def test_feature_geometry():
    """Feature has a geometry attribute"""
    geom = Geometry(type="Point")
    feat = Feature(geometry=geom)
    assert feat.geometry is geom


def test_feature_no_id():
    """Feature has no id"""
    feat = Feature()
    assert feat.id is None


def test_feature_id():
    """Feature has an id"""
    feat = Feature(id="123")
    assert feat.id == "123"


def test_feature_no_properties():
    """Feature has no properties"""
    feat = Feature()
    assert len(feat.properties) == 0


def test_feature_properties():
    """Feature has properties"""
    feat = Feature(properties=Properties(foo=1))
    assert len(feat.properties) == 1
    assert feat.properties["foo"] == 1


def test_feature_from_dict_kwargs():
    """Feature can be created from GeoJSON kwargs"""
    data = {
        "id": "foo",
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": (0, 0)},
        "properties": {"a": 0, "b": "bar"},
        "extras": {"this": 1},
    }
    feat = Feature.from_dict(**data)
    assert feat.id == "foo"
    assert feat.type == "Feature"
    assert feat.geometry.type == "Point"
    assert feat.geometry.coordinates == (0, 0)
    assert len(feat.properties) == 2
    assert feat.properties["a"] == 0
    assert feat.properties["b"] == "bar"
    assert feat["extras"]["this"] == 1


def test_feature_from_dict_obj():
    """Feature can be created from GeoJSON obj"""
    data = {
        "id": "foo",
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": (0, 0)},
        "properties": {"a": 0, "b": "bar"},
        "extras": {"this": 1},
    }
    feat = Feature.from_dict(data)
    assert feat.id == "foo"
    assert feat.type == "Feature"
    assert feat.geometry.type == "Point"
    assert feat.geometry.coordinates == (0, 0)
    assert len(feat.properties) == 2
    assert feat.properties["a"] == 0
    assert feat.properties["b"] == "bar"
    assert feat["extras"]["this"] == 1


def test_feature_from_dict_kwargs_2():
    """From GeoJSON kwargs using Geometry and Properties"""
    data = {
        "id": "foo",
        "type": "Feature",
        "geometry": Geometry(type="Point", coordinates=(0, 0)),
        "properties": Properties(a=0, b="bar"),
        "extras": {"this": 1},
    }
    feat = Feature.from_dict(**data)
    assert feat.id == "foo"
    assert feat.type == "Feature"
    assert feat.geometry.type == "Point"
    assert feat.geometry.coordinates == (0, 0)
    assert len(feat.properties) == 2
    assert feat.properties["a"] == 0
    assert feat.properties["b"] == "bar"
    assert feat["extras"]["this"] == 1


def test_geometry_encode():
    """Can encode a geometry"""
    assert ObjectEncoder().default(Geometry(type="Point", coordinates=(0, 0))) == {
        "type": "Point",
        "coordinates": (0, 0),
    }


def test_feature_encode():
    """Can encode a feature"""
    o_dict = ObjectEncoder().default(
        Feature(
            id="foo",
            geometry=Geometry(type="Point", coordinates=(0, 0)),
            properties=Properties(a=1, foo="bar", bytes=b"01234"),
        )
    )
    assert o_dict["id"] == "foo"
    assert o_dict["geometry"]["type"] == "Point"
    assert o_dict["geometry"]["coordinates"] == (0, 0)
    assert o_dict["properties"]["bytes"] == b'3031323334'


def test_decode_object_hook():
    """Can decode a feature"""
    data = {
        "id": "foo",
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": (0, 0)},
        "properties": {"a": 0, "b": "bar"},
        "extras": {"this": 1},
    }
    feat = decode_object(data)
    assert feat.id == "foo"
    assert feat.type == "Feature"
    assert feat.geometry.type == "Point"
    assert feat.geometry.coordinates == (0, 0)
    assert len(feat.properties) == 2
    assert feat.properties["a"] == 0
    assert feat.properties["b"] == "bar"
    assert feat["extras"]["this"] == 1


def test_decode_object_hook_geometry():
    """Can decode a geometry"""
    data = {"type": "Point", "coordinates": (0, 0)}
    geometry = decode_object(data)
    assert geometry.type == "Point"
    assert geometry.coordinates == (0, 0)


@pytest.mark.parametrize("o", [{}, {"a": 1}, {"type": "FeatureCollection"}])
def test_decode_object_hook_fallback(o):
    """Pass through an ordinary dict"""
    assert decode_object(o) == o


def test_properties():
    """Property factory works"""
    assert Properties.from_dict(a=1, foo="bar")["a"] == 1


def test_feature_gi():
    """Feature __geo_interface__."""
    gi = Feature(
        id="foo",
        geometry=Geometry(type="Point", coordinates=(0, 0)),
        properties=Properties(a=1, foo="bar"),
    )

    assert gi["id"] == "foo"
    assert gi["geometry"]["type"] == "Point"
    assert gi["geometry"]["coordinates"] == (0, 0)


def test_encode_bytes():
    """Bytes are encoded using base64."""
    assert ObjectEncoder().default(b"01234") == b'3031323334'


def test_null_property_encoding():
    """A null feature property is retained."""
    # Verifies fix for gh-1270.
    assert ObjectEncoder().default(Properties(a=1, b=None)) == {"a": 1, "b": None}


def test_null_geometry_encoding():
    """A null feature geometry is retained."""
    # Verifies fix for gh-1270.
    o_dict = ObjectEncoder().default(Feature())
    assert o_dict["geometry"] is None


def test_geometry_collection_encoding():
    """No coordinates in a GeometryCollection."""
    assert "coordinates" not in ObjectEncoder().default(
        Geometry(type="GeometryCollection", geometries=[])
    )


def test_feature_repr():
    feat = Feature(
        id="1",
        geometry=Geometry(type="LineString", coordinates=[(0, 0)] * 100),
        properties=Properties(a=1, foo="bar"),
    )
    assert repr(feat) == "fiona.Feature(geometry=fiona.Geometry(coordinates=[(0, 0), ...], type='LineString'), id='1', properties=fiona.Properties(a=1, foo='bar'))"


def test_issue1430():
    """__getitem__() returns property, not disconnected dict."""
    feat = Feature(properties=Properties())
    with pytest.warns(FionaDeprecationWarning, match="immutable"):
        feat["properties"]["foo"] = "bar"
    assert feat["properties"]["foo"] == "bar"
    assert feat.properties["foo"] == "bar"
