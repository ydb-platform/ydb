# -*- coding: utf-8 -*-
from django.db import models

from proxy_storage.meta_backends.base import MetaBackendBase, MetaBackendObjectDoesNotExist


class ORMMetaBackend(MetaBackendBase):
    def __init__(self, model, *args, **kwargs):
        self.model = model
        super(ORMMetaBackend, self).__init__(*args, **kwargs)

    def _convert_obj_to_dict(self, obj):
        return dict(
            [(field.name, getattr(obj, field.name)) for field in self.model._meta.fields]
        )

    def _create(self, data):
        return self.model.objects.create(**data)

    def _get(self, path):
        try:
            return self.model.objects.get(path=path)
        except self.model.DoesNotExist as exc:
            raise MetaBackendObjectDoesNotExist(exc)

    def update(self, path, update_data):
        return self.model.objects.filter(path=path).update(**update_data)

    def delete(self, path):
        return self.model.objects.filter(path=path).delete()

    def exists(self, path):
        return self.model.objects.filter(path=path).exists()


class ProxyStorageModelBase(models.Model):
    path = models.CharField(max_length=255, unique=True)
    proxy_storage_name = models.CharField(max_length=50)
    original_storage_path = models.CharField(max_length=255)

    class Meta:
        abstract = True

    def get_meta_backend_obj(self):
        # todo: test me
        return ORMMetaBackend(model=type(self)).get_meta_backend_obj(self)

    def __unicode__(self):
        meta_backend_obj = self.get_meta_backend_obj()
        proxy_storage = meta_backend_obj.get_proxy_storage()
        original_storage = meta_backend_obj.get_original_storage()
        return u'{0} {1} => {2} {3}'.format(
            type(proxy_storage).__name__,
            self.path,
            type(original_storage).__name__,
            self.original_storage_path
        )


class ContentObjectFieldMixin(models.Model):
    content_type_id = models.PositiveIntegerField(blank=True, null=True)
    object_id = models.PositiveIntegerField(blank=True, null=True)
    field = models.CharField(max_length=255)

    class Meta:
        abstract = True


class OriginalStorageNameMixin(models.Model):
    original_storage_name = models.CharField(max_length=50, blank=True)

    class Meta:
        abstract = True