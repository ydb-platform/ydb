import asyncio

from asgiref.sync import async_to_sync, sync_to_async
from django.http import Http404
from rest_framework.exceptions import ValidationError
from rest_framework.generics import GenericAPIView as DRFGenericAPIView

from adrf import mixins, views
from adrf.shortcuts import aget_object_or_404 as _aget_object_or_404


async def aget_object_or_404(queryset, *filter_args, **filter_kwargs):
    """
    Same as Django's standard shortcut, but make sure to also raise 404
    if the filter_kwargs don't match the required types.
    """
    try:
        return await _aget_object_or_404(queryset, *filter_args, **filter_kwargs)
    except (TypeError, ValueError, ValidationError):
        raise Http404


class GenericAPIView(views.APIView, DRFGenericAPIView):
    """This generic API view supports async pagination."""

    async def aget_object(self):
        """
        Returns the object the view is displaying.

        You may want to override this if you need to provide non-standard
        queryset lookups.  Eg if objects are referenced using multiple
        keyword arguments in the url conf.
        """
        queryset = await self.afilter_queryset(self.get_queryset())

        # Perform the lookup filtering.
        lookup_url_kwarg = self.lookup_url_kwarg or self.lookup_field

        assert lookup_url_kwarg in self.kwargs, (
            "Expected view %s to be called with a URL keyword argument "
            'named "%s". Fix your URL conf, or set the `.lookup_field` '
            "attribute on the view correctly."
            % (self.__class__.__name__, lookup_url_kwarg)
        )

        filter_kwargs = {self.lookup_field: self.kwargs[lookup_url_kwarg]}
        obj = await aget_object_or_404(queryset, **filter_kwargs)

        # May raise a permission denied
        await sync_to_async(self.check_object_permissions)(self.request, obj)

        return obj

    async def afilter_queryset(self, queryset):
        """
        Given a queryset, filter it with whichever filter backend is in use.
        """
        for backend in list(self.filter_backends):
            backend_instance = backend()
            if asyncio.iscoroutinefunction(backend_instance.filter_queryset):
                queryset = await backend_instance.filter_queryset(
                    self.request, queryset, self
                )
            else:
                queryset = await sync_to_async(backend_instance.filter_queryset)(
                    self.request, queryset, self
                )
        return queryset

    def paginate_queryset(self, queryset):
        """
        Return a single page of results, or `None` if pagination is disabled.
        """
        if self.paginator is None:
            return None
        if asyncio.iscoroutinefunction(self.paginator.paginate_queryset):
            return async_to_sync(self.paginator.paginate_queryset)(
                queryset, self.request, view=self
            )
        return self.paginator.paginate_queryset(queryset, self.request, view=self)

    def get_paginated_response(self, data):
        """
        Return a paginated style `Response` object for the given output data.
        """
        assert self.paginator is not None
        if asyncio.iscoroutinefunction(self.paginator.get_paginated_response):
            return async_to_sync(self.paginator.get_paginated_response)(data)
        return self.paginator.get_paginated_response(data)

    async def apaginate_queryset(self, queryset):
        """
        Return a single page of results, or `None` if pagination is disabled.
        """
        if self.paginator is None:
            return None
        if asyncio.iscoroutinefunction(self.paginator.paginate_queryset):
            return await self.paginator.paginate_queryset(
                queryset, self.request, view=self
            )
        return await sync_to_async(self.paginator.paginate_queryset)(
            queryset, self.request, view=self
        )

    async def get_apaginated_response(self, data):
        """
        Return a paginated style `Response` object for the given output data.
        """
        assert self.paginator is not None
        if asyncio.iscoroutinefunction(self.paginator.get_paginated_response):
            return await self.paginator.get_paginated_response(data)
        return await sync_to_async(self.paginator.get_paginated_response)(data)


# Concrete view classes that provide method handlers
# by composing the mixin classes with the base view.


class CreateAPIView(mixins.CreateModelMixin, GenericAPIView):
    """
    Concrete view for creating a model instance.
    """

    async def post(self, request, *args, **kwargs):
        return await self.acreate(request, *args, **kwargs)


class ListAPIView(mixins.ListModelMixin, GenericAPIView):
    """
    Concrete view for listing a queryset.
    """

    async def get(self, request, *args, **kwargs):
        return await self.alist(request, *args, **kwargs)


class RetrieveAPIView(mixins.RetrieveModelMixin, GenericAPIView):
    """
    Concrete view for retrieving a model instance.
    """

    async def get(self, request, *args, **kwargs):
        return await self.aretrieve(request, *args, **kwargs)


class DestroyAPIView(mixins.DestroyModelMixin, GenericAPIView):
    """
    Concrete view for deleting a model instance.
    """

    async def delete(self, request, *args, **kwargs):
        return await self.adestroy(request, *args, **kwargs)


class UpdateAPIView(mixins.UpdateModelMixin, GenericAPIView):
    """
    Concrete view for updating a model instance.
    """

    async def put(self, request, *args, **kwargs):
        return await self.aupdate(request, *args, **kwargs)

    async def patch(self, request, *args, **kwargs):
        return await self.partial_aupdate(request, *args, **kwargs)


class ListCreateAPIView(mixins.ListModelMixin, mixins.CreateModelMixin, GenericAPIView):
    """
    Concrete view for listing a queryset or creating a model instance.
    """

    async def get(self, request, *args, **kwargs):
        return await self.alist(request, *args, **kwargs)

    async def post(self, request, *args, **kwargs):
        return await self.acreate(request, *args, **kwargs)


class RetrieveUpdateAPIView(
    mixins.RetrieveModelMixin, mixins.UpdateModelMixin, GenericAPIView
):
    """
    Concrete view for retrieving, updating a model instance.
    """

    async def get(self, request, *args, **kwargs):
        return await self.aretrieve(request, *args, **kwargs)

    async def put(self, request, *args, **kwargs):
        return await self.aupdate(request, *args, **kwargs)

    async def patch(self, request, *args, **kwargs):
        return await self.partial_aupdate(request, *args, **kwargs)


class RetrieveDestroyAPIView(
    mixins.RetrieveModelMixin, mixins.DestroyModelMixin, GenericAPIView
):
    """
    Concrete view for retrieving or deleting a model instance.
    """

    async def get(self, request, *args, **kwargs):
        return await self.aretrieve(request, *args, **kwargs)

    async def delete(self, request, *args, **kwargs):
        return await self.adestroy(request, *args, **kwargs)


class RetrieveUpdateDestroyAPIView(
    mixins.RetrieveModelMixin,
    mixins.UpdateModelMixin,
    mixins.DestroyModelMixin,
    GenericAPIView,
):
    """
    Concrete view for retrieving, updating or deleting a model instance.
    """

    async def get(self, request, *args, **kwargs):
        return await self.aretrieve(request, *args, **kwargs)

    async def put(self, request, *args, **kwargs):
        return await self.aupdate(request, *args, **kwargs)

    async def patch(self, request, *args, **kwargs):
        return await self.partial_aupdate(request, *args, **kwargs)

    async def delete(self, request, *args, **kwargs):
        return await self.adestroy(request, *args, **kwargs)
