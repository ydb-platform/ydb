from tastypie.bundle import Bundle
from tastypie.resources import ModelResource
from tastypie.exceptions import NotFound

try:
    from django.urls import resolve, Resolver404, get_script_prefix
except ImportError:
    from django.core.urlresolvers import (
        resolve,
        Resolver404,
        get_script_prefix,
    )


class GenericResource(ModelResource):
    """
    Provides a stand-in resource for GFK relations.
    """
    def __init__(self, resources, *args, **kwargs):
        self.resource_mapping = {r._meta.resource_name: r for r in resources}
        super(GenericResource, self).__init__(*args, **kwargs)

    def get_via_uri(self, uri, request=None):
        """
        This pulls apart the salient bits of the URI and populates the
        resource via a ``obj_get``.

        Optionally accepts a ``request``.

        If you need custom behavior based on other portions of the URI,
        simply override this method.
        """
        prefix = get_script_prefix()
        chomped_uri = uri

        if prefix and chomped_uri.startswith(prefix):
            chomped_uri = chomped_uri[len(prefix) - 1:]

        try:
            view, args, kwargs = resolve(chomped_uri)
            resource_name = kwargs['resource_name']
            resource_class = self.resource_mapping[resource_name]
        except (Resolver404, KeyError):
            raise NotFound("The URL provided '%s' was not a link to a valid resource." % uri)

        parent_resource = resource_class(api_name=self._meta.api_name)
        kwargs = parent_resource.remove_api_resource_names(kwargs)
        bundle = Bundle(request=request)
        return parent_resource.obj_get(bundle, **kwargs)
