from rest_framework.utils.model_meta import get_field_info

from drf_spectacular.drainage import warn
from drf_spectacular.extensions import OpenApiSerializerExtension, OpenApiSerializerFieldExtension
from drf_spectacular.plumbing import (
    ResolvedComponent, build_array_type, build_object_type, follow_field_source, get_doc,
)


def build_point_schema():
    return {
        "type": "array",
        "items": {"type": "number", "format": "float"},
        "example": [12.9721, 77.5933],
        "minItems": 2,
        "maxItems": 3,
    }


def build_linestring_schema():
    return {
        "type": "array",
        "items": build_point_schema(),
        "example": [[22.4707, 70.0577], [12.9721, 77.5933]],
        "minItems": 2,
    }


def build_polygon_schema():
    return {
        "type": "array",
        "items": {**build_linestring_schema(), "minItems": 4},
        "example": [
            [
                [0.0, 0.0],
                [0.0, 50.0],
                [50.0, 50.0],
                [50.0, 0.0],
                [0.0, 0.0],
            ],
        ]
    }


def build_geo_container_schema(name, coords):
    return build_object_type(
        properties={
            "type": {"type": "string", "enum": [name]},
            "coordinates": coords,
        }
    )


def build_point_geo_schema():
    return build_geo_container_schema("Point", build_point_schema())


def build_linestring_geo_schema():
    return build_geo_container_schema("LineString", build_linestring_schema())


def build_polygon_geo_schema():
    return build_geo_container_schema("Polygon", build_polygon_schema())


def build_geometry_geo_schema():
    return {
        'oneOf': [
            build_point_geo_schema(),
            build_linestring_geo_schema(),
            build_polygon_geo_schema(),
        ]
    }


def build_bbox_schema():
    return {
        "type": "array",
        "items": {"type": "number"},
        "minItems": 4,
        "maxItems": 4,
        "example": [12.9721, 77.5933, 12.9721, 77.5933],
    }


def build_geo_schema(model_field):
    from django.contrib.gis.db import models

    if isinstance(model_field, models.PointField):
        return build_point_geo_schema()
    elif isinstance(model_field, models.LineStringField):
        return build_linestring_geo_schema()
    elif isinstance(model_field, models.PolygonField):
        return build_polygon_geo_schema()
    elif isinstance(model_field, models.MultiPointField):
        return build_geo_container_schema(
            "MultiPoint", build_array_type(build_point_schema())
        )
    elif isinstance(model_field, models.MultiLineStringField):
        return build_geo_container_schema(
            "MultiLineString", build_array_type(build_linestring_schema())
        )
    elif isinstance(model_field, models.MultiPolygonField):
        return build_geo_container_schema(
            "MultiPolygon", build_array_type(build_polygon_schema())
        )
    elif isinstance(model_field, models.GeometryCollectionField):
        return build_geo_container_schema(
            "GeometryCollection", build_array_type(build_geometry_geo_schema())
        )
    elif isinstance(model_field, models.GeometryField):
        return build_geometry_geo_schema()
    else:
        warn("Encountered unknown GIS geometry field")
        return {}


def map_geo_field(serializer, geo_field_name):
    from rest_framework_gis.fields import GeometrySerializerMethodField

    field = serializer.fields[geo_field_name]
    if isinstance(field, GeometrySerializerMethodField):
        warn("Geometry generation for GeometrySerializerMethodField is not supported.")
        return {}
    model_field = get_field_info(serializer.Meta.model).fields[geo_field_name]
    return build_geo_schema(model_field)


def _inject_enum_collision_fix(collection):
    from drf_spectacular.settings import spectacular_settings
    if not collection and 'GisFeatureEnum' not in spectacular_settings.ENUM_NAME_OVERRIDES:
        spectacular_settings.ENUM_NAME_OVERRIDES['GisFeatureEnum'] = ('Feature',)
    if collection and 'GisFeatureCollectionEnum' not in spectacular_settings.ENUM_NAME_OVERRIDES:
        spectacular_settings.ENUM_NAME_OVERRIDES['GisFeatureCollectionEnum'] = ('FeatureCollection',)


class GeoFeatureModelSerializerExtension(OpenApiSerializerExtension):
    target_class = 'rest_framework_gis.serializers.GeoFeatureModelSerializer'
    match_subclasses = True

    def map_serializer(self, auto_schema, direction):
        _inject_enum_collision_fix(collection=False)

        base_schema = auto_schema._map_serializer(self.target, direction, bypass_extensions=True)
        return self.map_geo_feature_model_serializer(self.target, base_schema)

    def map_geo_feature_model_serializer(self, serializer, base_schema):
        from rest_framework_gis.serializers import GeoFeatureModelSerializer

        geo_properties = {
            "type": {"type": "string", "enum": ["Feature"]}
        }
        if serializer.Meta.id_field:
            geo_properties["id"] = base_schema["properties"].pop(serializer.Meta.id_field)

        geo_properties["geometry"] = map_geo_field(serializer, serializer.Meta.geo_field)
        base_schema["properties"].pop(serializer.Meta.geo_field)

        if serializer.Meta.auto_bbox or serializer.Meta.bbox_geo_field:
            geo_properties["bbox"] = build_bbox_schema()
            base_schema["properties"].pop(serializer.Meta.bbox_geo_field, None)

        # only expose if description comes from the user
        description = base_schema.pop('description', None)
        if description == get_doc(GeoFeatureModelSerializer):
            description = None

        # ignore this aspect for now
        base_schema.pop('required', None)

        # nest remaining fields under property "properties"
        geo_properties["properties"] = base_schema

        return build_object_type(
            properties=geo_properties,
            description=description,
        )


class GeoFeatureModelListSerializerExtension(OpenApiSerializerExtension):
    target_class = 'rest_framework_gis.serializers.GeoFeatureModelListSerializer'

    def map_serializer(self, auto_schema, direction):
        _inject_enum_collision_fix(collection=True)

        # build/retrieve feature component generated by GeoFeatureModelSerializerExtension.
        # wrap the ref in the special list structure and build another component based on that.
        feature_component = auto_schema.resolve_serializer(self.target.child, direction)
        collection_schema = build_object_type(
            properties={
                "type": {"type": "string", "enum": ["FeatureCollection"]},
                "features": build_array_type(feature_component.ref)
            }
        )
        list_component = ResolvedComponent(
            name=f'{feature_component.name}List',
            type=ResolvedComponent.SCHEMA,
            object=self.target.child,
            schema=collection_schema
        )
        auto_schema.registry.register_on_missing(list_component)
        return list_component.ref


class GeometryFieldExtension(OpenApiSerializerFieldExtension):
    target_class = 'rest_framework_gis.fields.GeometryField'
    match_subclasses = True

    def map_serializer_field(self, auto_schema, direction):
        # running this extension for GeoFeatureModelSerializer's geo_field is superfluous
        # as above extension already handles that individually. We run it anyway because
        # robustly checking the proper condition is harder.
        try:
            model = self.target.parent.Meta.model
            model_field = follow_field_source(model, self.target.source.split('.'))
            return build_geo_schema(model_field)
        except:  # noqa: E722
            warn(f'Encountered an issue resolving field {self.target}. defaulting to generic object.')
            return {}
