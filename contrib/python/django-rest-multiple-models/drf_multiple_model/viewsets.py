from rest_framework.viewsets import GenericViewSet

from drf_multiple_model.mixins import FlatMultipleModelMixin, ObjectMultipleModelMixin


class FlatMultipleModelAPIViewSet(FlatMultipleModelMixin, GenericViewSet):
    def get_queryset(self):
        return None


class ObjectMultipleModelAPIViewSet(ObjectMultipleModelMixin, GenericViewSet):
    def get_queryset(self):
        return None
