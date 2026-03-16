# -*- coding: utf-8 -*-
from collections import OrderedDict

from django.utils.encoding import force_text
from django.core.files.storage import Storage

from proxy_storage import utils
from proxy_storage.meta_backends.base import MetaBackendObjectDoesNotExist


class ProxyStorageBase(Storage):
    original_storage = None
    meta_backend = None

    def get_original_storage(self, meta_backend_obj=None):
        return self.original_storage

    def get_name(self):
        from proxy_storage.settings import proxy_storage_settings

        return proxy_storage_settings.PROXY_STORAGE_CLASSES_INVERTED[type(self)]

    def _open(self, name, mode='rb'):
        try:
            meta_backend_obj = self.meta_backend.get(path=name)
            return self.get_original_storage(meta_backend_obj=meta_backend_obj).open(
                meta_backend_obj['original_storage_path'],
                mode
            )
        except MetaBackendObjectDoesNotExist:
            raise IOError(u'No such {0} object with path: {1}'.format(type(self.meta_backend).__name__, name))

    def save(self, name, content, original_storage_path=None):
        # save file to original storage
        if not original_storage_path:
            original_storage_path = self.get_original_storage().save(name, content)

        # Get the proper name for the file, as it will actually be saved to meta backend
        name = self.get_available_name(
            utils.clean_path(  # todo: test utils.clean_path usage
                self.get_original_storage_full_path(original_storage_path)
            )
        )

        # create meta backend info
        self.meta_backend.create(data=self.get_data_for_meta_backend_save(
            path=name,
            original_storage_path=original_storage_path,
            original_name=name,
            content=content,
        ))
        return force_text(name)

    def get_data_for_meta_backend_save(self, path, original_storage_path, original_name, content):
        return {
            'path': path,
            'original_storage_path': original_storage_path,
            'proxy_storage_name': self.get_name()
        }

    def get_original_storage_full_path(self, path, meta_backend_obj=None):
        # todo: test me
        try:
            return self.get_original_storage(meta_backend_obj=meta_backend_obj).path(path)
        except NotImplementedError:
            return path

    def delete(self, name):
        try:
            meta_backend_obj = self.meta_backend.get(path=name)
            self.get_original_storage(meta_backend_obj=meta_backend_obj).delete(
                meta_backend_obj['original_storage_path']
            )
            self.meta_backend.delete(path=meta_backend_obj['path'])
        except MetaBackendObjectDoesNotExist:
            raise IOError("File not found: {0}".format(name))

    def exists(self, name):
        return self.meta_backend.exists(path=name)


class MultipleOriginalStoragesMixin(object):
    original_storages = []

    def __init__(self, *args, **kwargs):
        super(MultipleOriginalStoragesMixin, self).__init__(*args, **kwargs)
        self._init_original_storages()

    def _init_original_storages(self):
        self.original_storages_dict = OrderedDict()
        self.original_storages_dict_inversed = {}
        for i, (name, original_storage) in enumerate(self.original_storages):
            if i == 0:  # make first item as original storage
                self.original_storage = original_storage
            self.original_storages_dict[name] = original_storage
            self.original_storages_dict_inversed[original_storage] = name

    def save(self, name, content, original_storage_path=None, using=None):
        if using:
            self.original_storage = self.original_storages_dict[using]
        return super(MultipleOriginalStoragesMixin, self).save(
            name=name,
            content=content,
            original_storage_path=original_storage_path
        )

    def get_original_storage(self, meta_backend_obj=None):
        if meta_backend_obj:
            return self.original_storages_dict[
                meta_backend_obj['original_storage_name']
            ]
        else:
            return super(MultipleOriginalStoragesMixin, self).get_original_storage(meta_backend_obj=meta_backend_obj)

    def get_data_for_meta_backend_save(self, path, original_storage_path, *args, **kwargs):
        data = super(MultipleOriginalStoragesMixin, self).get_data_for_meta_backend_save(
            path=path,
            original_storage_path=original_storage_path,
            *args,
            **kwargs
        )
        data.update({
            'original_storage_name': self.original_storages_dict_inversed[self.original_storage]
        })
        return data