from bson import DBRef

from .abstract import BaseDataObject, I18nErrorDict
from .i18n import N_


__all__ = ('List', 'Dict', 'Reference')


class List(BaseDataObject, list):

    __slots__ = ('inner_field', '_modified')

    def __init__(self, inner_field, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._modified = False
        self.inner_field = inner_field

    def __setitem__(self, key, obj):
        obj = self.inner_field.deserialize(obj)
        super().__setitem__(key, obj)
        self.set_modified()

    def __delitem__(self, key):
        super().__delitem__(key)
        self.set_modified()

    def append(self, obj):
        obj = self.inner_field.deserialize(obj)
        ret = super().append(obj)
        self.set_modified()
        return ret

    def insert(self, i, obj):
        obj = self.inner_field.deserialize(obj)
        ret = super().insert(i, obj)
        self.set_modified()
        return ret

    def pop(self, *args, **kwargs):
        ret = super().pop(*args, **kwargs)
        self.set_modified()
        return ret

    def clear(self, *args, **kwargs):
        ret = super().clear(*args, **kwargs)
        self.set_modified()
        return ret

    def remove(self, *args, **kwargs):
        ret = super().remove(*args, **kwargs)
        self.set_modified()
        return ret

    def reverse(self, *args, **kwargs):
        ret = super().reverse(*args, **kwargs)
        self.set_modified()
        return ret

    def sort(self, *args, **kwargs):
        ret = super().sort(*args, **kwargs)
        self.set_modified()
        return ret

    def extend(self, iterable):
        iterable = [self.inner_field.deserialize(obj) for obj in iterable]
        ret = super().extend(iterable)
        self.set_modified()
        return ret

    def __repr__(self):
        return '<object %s.%s(%s)>' % (
            self.__module__, self.__class__.__name__, list(self))

    def is_modified(self):
        if self._modified:
            return True
        if self and isinstance(self[0], BaseDataObject):
            return any(obj.is_modified() for obj in self)
        return False

    def set_modified(self):
        self._modified = True

    def clear_modified(self):
        self._modified = False
        if self and isinstance(self[0], BaseDataObject):
            for obj in self:
                obj.clear_modified()


class Dict(BaseDataObject, dict):

    __slots__ = ('key_field', 'value_field', '_modified')

    def __init__(self, key_field, value_field, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._modified = False
        self.key_field = key_field
        self.value_field = value_field

    def __setitem__(self, key, obj):
        key = self.key_field.deserialize(key) if self.key_field else key
        obj = self.value_field.deserialize(obj) if self.value_field else obj
        super().__setitem__(key, obj)
        self.set_modified()

    def __delitem__(self, key):
        super().__delitem__(key)
        self.set_modified()

    def pop(self, *args, **kwargs):
        ret = super().pop(*args, **kwargs)
        self.set_modified()
        return ret

    def popitem(self, *args, **kwargs):
        ret = super().popitem(*args, **kwargs)
        self.set_modified()
        return ret

    def setdefault(self, key, obj=None):
        key = self.key_field.deserialize(key) if self.key_field else key
        obj = self.value_field.deserialize(obj) if self.value_field else obj
        ret = super().setdefault(key, obj)
        self.set_modified()
        return ret

    def update(self, other):
        new = {
            self.key_field.deserialize(k) if self.key_field else k:
            self.value_field.deserialize(v) if self.value_field else v
            for k, v in other.items()
        }
        super().update(new)
        self.set_modified()

    def __repr__(self):
        return '<object %s.%s(%s)>' % (
            self.__module__, self.__class__.__name__, dict(self))

    def is_modified(self):
        if self._modified:
            return True
        if self and any(isinstance(v, BaseDataObject) for v in self.values()):
            return any(obj.is_modified() for obj in self.values())
        return False

    def set_modified(self):
        self._modified = True

    def clear_modified(self):
        self._modified = False
        if self and any(isinstance(v, BaseDataObject) for v in self.values()):
            for obj in self.values():
                obj.clear_modified()


class Reference:

    error_messages = I18nErrorDict(not_found=N_('Reference not found for document {document}.'))

    def __init__(self, document_cls, pk):
        self.document_cls = document_cls
        self.pk = pk
        self._document = None

    def fetch(self, no_data=False, force_reload=False):
        """
        Retrieve from the database the referenced document

        :param no_data: if True, the caller is only interested in whether or
            not the document is present in database. This means the
            implementation may not retrieve document's data to save bandwidth.
        :param force_reload: if True, ignore any cached data and reload referenced
            document from database.
        """
        raise NotImplementedError
    # TODO replace no_data by `exists` function

    def __repr__(self):
        return '<object %s.%s(document=%s, pk=%r)>' % (
            self.__module__, self.__class__.__name__, self.document_cls.__name__, self.pk)

    def __eq__(self, other):
        if isinstance(other, self.document_cls):
            return other.pk == self.pk
        if isinstance(other, Reference):
            return self.pk == other.pk and self.document_cls == other.document_cls
        if isinstance(other, DBRef):
            return self.pk == other.id and self.document_cls.collection.name == other.collection
        return NotImplemented
