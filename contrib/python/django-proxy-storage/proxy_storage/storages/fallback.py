# -*- coding: utf-8 -*-
from proxy_storage.storages.base import MultipleOriginalStoragesMixin


class OriginalStorageFallbackMixin(object):
    fallback_exceptions = None

    def get_fallback_exceptions(self):
        return self.fallback_exceptions


class FallbackProxyStorageMixin(MultipleOriginalStoragesMixin):
    def save(self, name, content, original_storage_path=None, using=None):
        if using:
            try_original_storages = [self.original_storages_dict[using]]
        else:
            try_original_storages = self.original_storages_dict.values()

        latest_exc = None
        for original_storage in try_original_storages:
            if hasattr(original_storage, 'get_fallback_exceptions'):
                fallback_exceptions = original_storage.get_fallback_exceptions()
            else:
                fallback_exceptions = None
            save_kwargs = {
                'name': name,
                'content': content,
                'original_storage_path': original_storage_path,
                'using': self.original_storages_dict_inversed[original_storage]
            }
            if fallback_exceptions:
                try:
                    return super(FallbackProxyStorageMixin, self).save(**save_kwargs)
                except fallback_exceptions as exc:
                    latest_exc = exc
            else:
                return super(FallbackProxyStorageMixin, self).save(**save_kwargs)
        raise latest_exc  # if we here, then function hadn't returned
                          # and we should raise the latest exception