# -*- coding: utf-8 -*-


class MetaBackendObjectException(Exception):
    pass


class MetaBackendObjectDoesNotExist(MetaBackendObjectException):
    pass


class MetaBackendObject(dict):
    def get_original_storage(self):
        return self.get_proxy_storage().get_original_storage(
            meta_backend_obj=self
        )

    def get_proxy_storage(self):
        from proxy_storage.settings import proxy_storage_settings

        return proxy_storage_settings.PROXY_STORAGE_CLASSES[self['proxy_storage_name']]()

    def get_original_storage_full_path(self):
        return self.get_proxy_storage().get_original_storage_full_path(
            path=self['original_storage_path'],
            meta_backend_obj=self
        )


class MetaBackendBase(object):
    def create(self, data):
        return self.get_meta_backend_obj(obj=self._create(data=data))

    def get_meta_backend_obj(self, obj):
        return MetaBackendObject(
            self._convert_obj_to_dict(obj=obj)
        )

    def _convert_obj_to_dict(self, obj):
        raise NotImplementedError

    def _create(self, data):
        raise NotImplementedError

    def get(self, path):
        return self.get_meta_backend_obj(self._get(path=path))

    def _get(self, path):
        raise NotImplementedError

    def delete(self, path):
        raise NotImplementedError

    def update(self, path, update_data):
        raise NotImplementedError

    def exists(self, path):
        raise NotImplementedError