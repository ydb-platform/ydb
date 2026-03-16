from asgiref.sync import sync_to_async
from rest_framework import mixins, status
from rest_framework.response import Response


async def get_data(serializer):
    """Use adata if the serializer supports it, data otherwise."""
    return (
        await serializer.adata
        if hasattr(serializer, "adata")
        else await sync_to_async(lambda: serializer.data)()
    )


class CreateModelMixin(mixins.CreateModelMixin):
    """
    Create a model instance.
    """

    async def acreate(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        await sync_to_async(serializer.is_valid)(raise_exception=True)
        await self.perform_acreate(serializer)
        data = await get_data(serializer)
        headers = self.get_success_headers(data)
        return Response(data, status=status.HTTP_201_CREATED, headers=headers)

    async def perform_acreate(self, serializer):
        await serializer.asave()


class ListModelMixin(mixins.ListModelMixin):
    """
    List a queryset.
    """

    async def alist(self, *args, **kwargs):
        queryset = await self.afilter_queryset(self.get_queryset())

        page = await self.apaginate_queryset(queryset)
        if page is not None:
            serializer = self.get_serializer(page, many=True)
            data = await get_data(serializer)
            return await self.get_apaginated_response(data)

        serializer = self.get_serializer(queryset, many=True)
        data = await get_data(serializer)
        return Response(data, status=status.HTTP_200_OK)


class RetrieveModelMixin(mixins.RetrieveModelMixin):
    """
    Retrieve a model instance.
    """

    async def aretrieve(self, request, *args, **kwargs):
        instance = await self.aget_object()
        serializer = self.get_serializer(instance, many=False)
        data = await get_data(serializer)
        return Response(data, status=status.HTTP_200_OK)


class UpdateModelMixin(mixins.UpdateModelMixin):
    """
    Update a model instance.
    """

    async def aupdate(self, request, *args, **kwargs):
        partial = kwargs.pop("partial", False)
        instance = await self.aget_object()
        serializer = self.get_serializer(instance, data=request.data, partial=partial)
        await sync_to_async(serializer.is_valid)(raise_exception=True)
        await self.perform_aupdate(serializer)

        if getattr(instance, "_prefetched_objects_cache", None):
            # If 'prefetch_related' has been applied to a queryset, we need to
            # forcibly invalidate the prefetch cache on the instance.
            instance._prefetched_objects_cache = {}
        data = await get_data(serializer)

        return Response(data, status=status.HTTP_200_OK)

    async def perform_aupdate(self, serializer):
        await serializer.asave()

    async def partial_aupdate(self, request, *args, **kwargs):
        kwargs["partial"] = True
        return await self.aupdate(request, *args, **kwargs)


class DestroyModelMixin(mixins.DestroyModelMixin):
    """
    Destroy a model instance.
    """

    async def adestroy(self, request, *args, **kwargs):
        instance = await self.aget_object()
        await self.perform_adestroy(instance)
        return Response(status=status.HTTP_204_NO_CONTENT)

    async def perform_adestroy(self, instance):
        await instance.adelete()
