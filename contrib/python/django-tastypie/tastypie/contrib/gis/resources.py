# See COPYING file in this directory.
# Some code originally from django-boundaryservice
import json

try:
    from urllib.parse import unquote
except ImportError:
    from urllib import unquote

from django.contrib.gis.db.models import GeometryField
from django.contrib.gis.geos import GEOSGeometry

from tastypie.fields import ApiField, CharField
from tastypie import resources


class GeometryApiField(ApiField):
    """
    Custom ApiField for dealing with data from GeometryFields (by serializing
    them as GeoJSON).
    """
    dehydrated_type = 'geometry'
    help_text = 'Geometry data.'

    def hydrate(self, bundle):
        value = super(GeometryApiField, self).hydrate(bundle)
        if value is None:
            return value
        return json.dumps(value)

    def dehydrate(self, obj, for_list=False):
        return self.convert(super(GeometryApiField, self).dehydrate(obj))

    def convert(self, value):
        if value is None:
            return None

        if isinstance(value, dict):
            return value

        # Get ready-made geojson serialization and then convert it _back_ to
        # a Python object so that tastypie can serialize it as part of the
        # bundle.
        return json.loads(value.geojson)


class ModelResource(resources.ModelResource):
    """
    ModelResource subclass that handles geometry fields as GeoJSON.
    """
    @classmethod
    def api_field_from_django_field(cls, f, default=CharField):
        """
        Overrides default field handling to support custom GeometryApiField.
        """
        if isinstance(f, GeometryField):
            return GeometryApiField

        return super(ModelResource, cls).api_field_from_django_field(f, default)

    def filter_value_to_python(self, value, field_name, filters, filter_expr,
            filter_type):
        value = super(ModelResource, self).filter_value_to_python(
            value, field_name, filters, filter_expr, filter_type)

        # If we are filtering on a GeometryApiField then we should try
        # and convert this to a GEOSGeometry object.  The conversion
        # will fail if we don't have value JSON, so in that case we'll
        # just return ``value`` as normal.
        if isinstance(self.fields[field_name], GeometryApiField):
            try:
                value = GEOSGeometry(unquote(value))
            except ValueError:
                pass
        return value
