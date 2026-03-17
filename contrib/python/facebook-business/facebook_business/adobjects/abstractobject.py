# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.

# This source code is licensed under the license found in the
# LICENSE file in the root directory of this source tree.

from facebook_business.exceptions import (
    FacebookBadObjectError,
)
from facebook_business.typechecker import TypeChecker

try:
  # Since python 3
  import collections.abc as collections_abc
except ImportError:
  # Won't work after python 3.8
  import collections as collections_abc

import json

class AbstractObject(collections_abc.MutableMapping):

    """
    Represents an abstract object (may or may not have explicitly be a node of
    the Graph) as a MutableMapping of its data.
    """

    _default_read_fields = []
    _field_types = {}

    class Field:
        pass

    def __init__(self):
        self._data = {}
        self._field_checker = TypeChecker(self._field_types,
            self._get_field_enum_info())

    def __getitem__(self, key):
        return self._data[str(key)]

    def __setitem__(self, key, value):
        if key.startswith('_'):
            self.__setattr__(key, value)
        else:
            self._data[key] = self._field_checker.get_typed_value(key, value)
        return self

    def __eq__(self, other):
        return other is not None and hasattr(other, 'export_all_data') and \
            self.export_all_data() == other.export_all_data()

    def __delitem__(self, key):
        del self._data[key]

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)

    def __contains__(self, key):
        return key in self._data

    def __unicode__(self):
        return unicode(self._data)

    def __repr__(self):
        return "<%s> %s" % (
            self.__class__.__name__,
            json.dumps(
                self.export_value(self._data),
                sort_keys=True,
                indent=4,
                separators=(',', ': '),
            ),
        )

    #reads in data from json object
    def _set_data(self, data):
        if hasattr(data, 'items'):
            for key, value in data.items():
                self[key] = value
        else:
            raise FacebookBadObjectError("Bad data to set object data")
        self._json = data

    @classmethod
    def _get_field_enum_info(cls):
        """Returns info for fields that use enum values
        Should be implemented in subclasses
        """
        return {}

    # @deprecated get_endpoint function is deprecated
    @classmethod
    def get_endpoint(cls):
        """Returns the endpoint name.
        Raises:
            NotImplementedError if the method is not implemented in a class
                that derives from this abstract class.
        """
        raise NotImplementedError(
            "%s must have implemented get_endpoint." % cls.__name__,
        )

    @classmethod
    def get_default_read_fields(cls):
        """Returns the class's list of default fields to read."""
        return cls._default_read_fields

    @classmethod
    def set_default_read_fields(cls, fields):
        """Sets the class's list of default fields to read.
        Args:
            fields: list of field names to read by default without specifying
                them explicitly during a read operation either via EdgeIterator
                or via AbstractCrudObject.read.
        """
        cls._default_read_fields = fields

    @classmethod
    def _assign_fields_to_params(cls, fields, params):
        """Applies fields to params in a consistent manner."""
        if fields is None:
            fields = cls.get_default_read_fields()
        if fields:
            params['fields'] = ','.join(fields)

    def set_data(self, data):
        """
        For an AbstractObject, we do not need to keep history.
        """
        self._set_data(data)

    def export_value(self, data):
        if isinstance(data, AbstractObject):
            data = data.export_all_data()
        elif isinstance(data, dict):
            data = dict((k, self.export_value(v))
                        for k, v in data.items()
                        if v is not None)
        elif isinstance(data, list):
            data = [self.export_value(v) for v in data]
        return data

    def export_data(self):
        """
        Deprecated. Use export_all_data() instead.
        """
        return self.export_all_data()

    def export_all_data(self):
        return self.export_value(self._data)

    @classmethod
    def create_object(cls, api, data, target_class):
        new_object = target_class(api=api)
        new_object._set_data(data)
        return new_object
