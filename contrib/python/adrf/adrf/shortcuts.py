from django.http import Http404
from django.shortcuts import _get_queryset

try:
    from django.shortcuts import aget_object_or_404
except ImportError:
    # NOTE aget_object_or_404 is defined since Django 5.
    # This function will be removed when support for Django 4 is dropped.
    async def aget_object_or_404(klass, *args, **kwargs):
        """See get_object_or_404()."""
        queryset = _get_queryset(klass)
        if not hasattr(queryset, "aget"):
            klass__name = (
                klass.__name__ if isinstance(klass, type) else klass.__class__.__name__
            )
            raise ValueError(
                "First argument to aget_object_or_404() must be a Model, Manager, or "
                f"QuerySet, not '{klass__name}'."
            )
        try:
            return await queryset.aget(*args, **kwargs)
        except queryset.model.DoesNotExist:
            raise Http404(
                f"No {queryset.model._meta.object_name} matches the given query."
            )
