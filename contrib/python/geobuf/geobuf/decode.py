# -*- coding: utf-8 -*-

import collections
import json

from . import geobuf_pb2


class Decoder:
    geometry_types = ('Point', 'MultiPoint', 'LineString', 'MultiLineString',
                      'Polygon', 'MultiPolygon', 'GeometryCollection')

    def __init__(self):
        self.data: geobuf_pb2.Data = geobuf_pb2.Data()
        self.e: int = 1
        self.dim: int = 2

    def decode(self, data_str: bytes):

        data = self.data = geobuf_pb2.Data()
        data.ParseFromString(data_str)

        self.e = pow(10, data.precision)
        self.dim = data.dimensions or 2

        data_type = data.WhichOneof('data_type')

        if data_type == 'feature_collection':
            return self.decode_feature_collection(data.feature_collection)
        elif data_type == 'feature':
            return self.decode_feature(data.feature)
        elif data_type == 'geometry':
            return self.decode_geometry(data.geometry)

    def decode_feature_collection(self, feature_collection):
        obj = {'type': 'FeatureCollection', 'features': []}
        self.decode_properties(feature_collection.custom_properties, feature_collection.values, obj)
        for feature in feature_collection.features:
            obj['features'].append(self.decode_feature(feature))

        return obj

    def decode_feature(self, feature):
        obj = collections.OrderedDict()
        obj['type'] = 'Feature'

        self.decode_properties(feature.custom_properties, feature.values, obj)

        self.decode_id(feature, obj)
        obj['geometry'] = self.decode_geometry(feature.geometry)
        if len(feature.properties):
            obj['properties'] = self.decode_properties(feature.properties, feature.values)

        return obj

    def decode_properties(self, props, values, dest=None):
        if dest is None:
            dest = {}
        for i in range(0, len(props), 2):
            key = self.data.keys[props[i]]
            val = values[props[i + 1]]

            value_type = val.WhichOneof('value_type')
            if value_type == 'string_value':
                dest[key] = val.string_value
            elif value_type == 'double_value':
                dest[key] = val.double_value
            elif value_type == 'pos_int_value':
                dest[key] = val.pos_int_value
            elif value_type == 'neg_int_value':
                dest[key] = -val.neg_int_value
            elif value_type == 'bool_value':
                dest[key] = val.bool_value
            elif value_type == 'json_value':
                dest[key] = json.loads(val.json_value)
        return dest

    @staticmethod
    def decode_id(obj, obj_json):
        id_type = obj.WhichOneof('id_type')
        if id_type == 'id':
            obj_json['id'] = obj.id
        elif id_type == 'int_id':
            obj_json['id'] = obj.int_id

    def decode_geometry(self, geometry):
        obj = collections.OrderedDict()
        gt = obj['type'] = self.geometry_types[geometry.type]

        self.decode_properties(geometry.custom_properties, geometry.values, obj)

        if gt == 'GeometryCollection':
            obj['geometries'] = [self.decode_geometry(geom) for geom in geometry.geometries]
        elif gt == 'Point':
            obj['coordinates'] = self.decode_point(geometry.coords)
        elif gt == 'MultiPoint':
            obj['coordinates'] = self.decode_line(geometry.coords)
        elif gt == 'LineString':
            obj['coordinates'] = self.decode_line(geometry.coords)
        elif gt == 'MultiLineString':
            obj['coordinates'] = self.decode_multi_line(geometry)
        elif gt == 'Polygon':
            obj['coordinates'] = self.decode_multi_line(geometry, is_closed=True)
        elif gt == 'MultiPolygon':
            obj['coordinates'] = self.decode_multi_polygon(geometry)

        return obj

    def decode_coord(self, coord):
        return float(coord) / self.e

    def decode_point(self, coords):
        return [self.decode_coord(x) for x in coords]

    def decode_line(self, coords, is_closed=False):
        obj = []

        r = range(self.dim)
        r2 = range(0, len(coords), self.dim)
        p0 = [0] * self.dim
        for i in r2:
            p = [p0[j] + coords[i + j] for j in r]
            obj.append(self.decode_point(p))
            p0 = p

        if is_closed:
            p = [coords[j] for j in r]
            obj.append(self.decode_point(p))

        return obj

    def decode_multi_line(self, geometry, is_closed=False):
        if len(geometry.lengths) == 0:
            return [self.decode_line(geometry.coords, is_closed=is_closed)]

        obj = []
        i = 0

        for l in geometry.lengths:
            obj.append(self.decode_line(geometry.coords[i:i + l * self.dim], is_closed=is_closed))
            i += l * self.dim

        return obj

    def decode_multi_polygon(self, geometry):
        if len(geometry.lengths) == 0:
            return [[self.decode_line(geometry.coords, is_closed=True)]]

        obj = []
        i = 0
        j = 1
        num_polygons = geometry.lengths[0]

        for n in range(num_polygons):  # for every polygon
            num_rings = geometry.lengths[j]
            j += 1
            rings = []
            for l in geometry.lengths[j:j + num_rings]:
                rings.append(self.decode_line(geometry.coords[i:i + l * self.dim], is_closed=True))
                j += 1
                i += l * self.dim
            obj.append(rings)
        return obj
