# -*- coding: utf-8 -*-
from django.utils.encoding import force_text
from django.db import models
from django.db.models import signals
from django.contrib.contenttypes.models import ContentType

# http://south.readthedocs.org/en/latest/customfields.html#extending-introspection
try:
    # todo: test me
    from south.modelsinspector import add_introspection_rules
    add_introspection_rules([], ["^django_proxy_storage\.db\.fields\.ProxyStorageFileField"])
except ImportError:
    pass


class ProxyStorageContentObjectFieldMixin(object):
    def contribute_to_class(self, cls, name):
        super(ProxyStorageContentObjectFieldMixin, self).contribute_to_class(cls, name)
        signals.post_save.connect(self._update_proxy_storage_content_object_field, sender=cls)

    def _update_proxy_storage_content_object_field(self, instance, force=False, *args, **kwargs):
        path = force_text(getattr(instance, self.name))
        if path:
            proxy_storage = self.storage
            meta_backend_obj = proxy_storage.meta_backend.get(path=path)
            content_type = ContentType.objects.get_for_model(type(instance))
            object_id = instance.id
            field = self.name
            if (meta_backend_obj.get('content_type_id') != content_type or
                meta_backend_obj.get('object_id') != object_id or
                meta_backend_obj.get('field') != field):
                proxy_storage.meta_backend.update(
                    path=path,
                    update_data={
                        'content_type_id': content_type.id,
                        'object_id': object_id,
                        'field': field,
                    }
                )


class ProxyStorageFileField(ProxyStorageContentObjectFieldMixin, models.FileField):
    pass