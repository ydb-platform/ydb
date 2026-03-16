from __future__ import absolute_import
from django.apps import apps

MOMMY_GIS = apps.is_installed("django.contrib.gis")

default_gis_mapping = {}

__all__ = ['MOMMY_GIS', 'default_gis_mapping']

if MOMMY_GIS:
    from . import random_gen
    from django.contrib.gis.db.models import (
        GeometryField,
        PointField,
        LineStringField,
        PolygonField,
        MultiPointField,
        MultiLineStringField,
        MultiPolygonField,
        GeometryCollectionField,
    )

    default_gis_mapping[GeometryField] = random_gen.gen_geometry
    default_gis_mapping[PointField] = random_gen.gen_point
    default_gis_mapping[LineStringField] = random_gen.gen_line_string
    default_gis_mapping[PolygonField] = random_gen.gen_polygon
    default_gis_mapping[MultiPointField] = random_gen.gen_multi_point
    default_gis_mapping[MultiLineStringField] = random_gen.gen_multi_line_string
    default_gis_mapping[MultiPolygonField] = random_gen.gen_multi_polygon
    default_gis_mapping[GeometryCollectionField] = random_gen.gen_geometry_collection
