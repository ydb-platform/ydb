# -*- coding: utf-8 -*-

import collections
import json

from typing import Mapping
import six

from . import geobuf_pb2


class Encoder:
    geometry_types = {
        'Point': 0,
        'MultiPoint': 1,
        'LineString': 2,
        'MultiLineString': 3,
        'Polygon': 4,
        'MultiPolygon': 5,
        'GeometryCollection': 6,
    }

    def __init__(self):
        self.json: Mapping = dict()
        self.data: geobuf_pb2.Data = geobuf_pb2.Data()
        self.precision: int = 6
        self.dim: int = 2
        self.e: int = pow(10, self.precision)
        self.keys = collections.OrderedDict()

    def encode(self, data_json: Mapping, precision: int = 6, dim: int = 2):
        obj = self.json = data_json
        data = self.data = geobuf_pb2.Data()
        data.dimensions = dim
        data.precision = precision

        self.precision = precision
        self.dim = dim
        self.e = pow(10, precision)  # multiplier for converting coordinates into integers

        data_type = obj['type']

        if data_type == 'FeatureCollection':
            self.encode_feature_collection(data.feature_collection, obj)
        elif data_type == 'Feature':
            self.encode_feature(data.feature, obj)
        else:
            self.encode_geometry(data.geometry, obj)

        return data.SerializeToString()

    def encode_feature_collection(self, feature_collection, feature_collection_json):
        self.encode_custom_properties(feature_collection, feature_collection_json, ('type', 'features'))
        for feature_json in feature_collection_json.get('features'):
            self.encode_feature(feature_collection.features.add(), feature_json)

    def encode_feature(self, feature, feature_json):
        self.encode_id(feature, feature_json.get('id'))
        self.encode_properties(feature, feature_json.get('properties'))
        self.encode_custom_properties(feature, feature_json, ('type', 'id', 'properties', 'geometry'))
        self.encode_geometry(feature.geometry, feature_json.get('geometry'))

    def encode_geometry(self, geometry, geometry_json):

        gt = geometry_json['type']
        coords = geometry_json.get('coordinates')

        geometry.type = self.geometry_types[gt]

        self.encode_custom_properties(geometry, geometry_json,
                                      ('type', 'id', 'coordinates', 'arcs', 'geometries', 'properties'))

        if gt == 'GeometryCollection':
            for geom in geometry_json.get('geometries'):
                self.encode_geometry(geometry.geometries.add(), geom)
        elif gt == 'Point':
            self.add_point(geometry.coords, coords)
        elif gt == 'MultiPoint':
            self.add_line(geometry.coords, coords)
        elif gt == 'LineString':
            self.add_line(geometry.coords, coords)
        elif gt == 'MultiLineString':
            self.add_multi_line(geometry, coords)
        elif gt == 'Polygon':
            self.add_multi_line(geometry, coords, is_closed=True)
        elif gt == 'MultiPolygon':
            self.add_multi_polygon(geometry, coords)

    def encode_properties(self, obj, props_json):
        if props_json:
            for key, val in props_json.items():
                self.encode_property(key, val, obj.properties, obj.values)

    def encode_custom_properties(self, obj, obj_json, exclude):
        for key, val in obj_json.items():
            if key not in exclude:
                self.encode_property(key, val, obj.custom_properties, obj.values)

    def encode_property(self, key, val, properties, values):
        keys = self.keys

        if key not in keys:
            keys[key] = True
            self.data.keys.append(key)
            key_index = len(self.data.keys) - 1
        else:
            key_index = list(keys.keys()).index(key)

        value = values.add()

        if isinstance(val, dict) or isinstance(val, list):
            value.json_value = json.dumps(val, separators=(',', ':')).encode('utf-8')
        elif isinstance(val, six.text_type):
            value.string_value = val
        elif isinstance(val, float):
            if val.is_integer():
                self.encode_int(int(val), value)
            else:
                value.double_value = val
        elif isinstance(val, bool):
            value.bool_value = val
        elif isinstance(val, six.integer_types):
            self.encode_int(val, value)

        properties.append(key_index)
        properties.append(len(values) - 1)

    @staticmethod
    def encode_int(val, value):
        try:
            if val >= 0:
                value.pos_int_value = val
            else:
                value.neg_int_value = -val
        except ValueError:
            value.double_value = val

    @staticmethod
    def encode_id(obj, id):
        if id is not None:
            if isinstance(id, int):
                obj.int_id = id
            else:
                obj.id = str(id)

    def add_coord(self, coords, coord):
        coords.append(int(round(coord * self.e)))

    def add_point(self, coords, point):
        for x in point:
            self.add_coord(coords, x)

    def add_line(self, coords, points, is_closed=False):
        sum = [0] * self.dim
        r = range(0, len(points) - int(is_closed))
        for i in r:
            for j in range(0, self.dim):
                n = int(round(points[i][j] * self.e) - sum[j])
                coords.append(n)
                sum[j] += n

    def add_multi_line(self, geometry, lines, is_closed=False):
        if len(lines) != 1:
            for points in lines:
                geometry.lengths.append(len(points) - int(is_closed))

        for points in lines:
            self.add_line(geometry.coords, points, is_closed)

    def add_multi_polygon(self, geometry, polygons):
        if len(polygons) != 1 or len(polygons[0]) != 1:
            geometry.lengths.append(len(polygons))
            for rings in polygons:
                geometry.lengths.append(len(rings))
                for points in rings:
                    geometry.lengths.append(len(points) - 1)

        for rings in polygons:
            for points in rings:
                self.add_line(geometry.coords, points, is_closed=True)
