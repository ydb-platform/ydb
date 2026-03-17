# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import copy
import re
import warnings

from collections import OrderedDict

try:
    from collections.abc import MutableMapping
except ImportError:
    # TODO Remove this to drop Python2 support
    from collections import MutableMapping
from six import iteritems, itervalues
from werkzeug.utils import cached_property

from .mask import Mask
from .errors import abort

from jsonschema import Draft4Validator
from jsonschema.exceptions import ValidationError

from .utils import not_none
from ._http import HTTPStatus


RE_REQUIRED = re.compile(r"u?\'(?P<name>.*)\' is a required property", re.I | re.U)


def instance(cls):
    if isinstance(cls, type):
        return cls()
    return cls


class ModelBase(object):
    """
    Handles validation and swagger style inheritance for both subclasses.
    Subclass must define `schema` attribute.

    :param str name: The model public name
    """

    def __init__(self, name, *args, **kwargs):
        super(ModelBase, self).__init__(*args, **kwargs)
        self.__apidoc__ = {"name": name}
        self.name = name
        self.__parents__ = []

        def instance_inherit(name, *parents):
            return self.__class__.inherit(name, self, *parents)

        self.inherit = instance_inherit

    @property
    def ancestors(self):
        """
        Return the ancestors tree
        """
        ancestors = [p.ancestors for p in self.__parents__]
        return set.union(set([self.name]), *ancestors)

    def get_parent(self, name):
        if self.name == name:
            return self
        else:
            for parent in self.__parents__:
                found = parent.get_parent(name)
                if found:
                    return found
        raise ValueError("Parent " + name + " not found")

    @property
    def __schema__(self):
        schema = self._schema

        if self.__parents__:
            refs = [
                {"$ref": "#/definitions/{0}".format(parent.name)}
                for parent in self.__parents__
            ]

            return {"allOf": refs + [schema]}
        else:
            return schema

    @classmethod
    def inherit(cls, name, *parents):
        """
        Inherit this model (use the Swagger composition pattern aka. allOf)
        :param str name: The new model name
        :param dict fields: The new model extra fields
        """
        model = cls(name, parents[-1])
        model.__parents__ = parents[:-1]
        return model

    def validate(self, data, resolver=None, format_checker=None):
        validator = Draft4Validator(
            self.__schema__, resolver=resolver, format_checker=format_checker
        )
        try:
            validator.validate(data)
        except ValidationError:
            abort(
                HTTPStatus.BAD_REQUEST,
                message="Input payload validation failed",
                errors=dict(self.format_error(e) for e in validator.iter_errors(data)),
            )

    def format_error(self, error):
        path = list(error.path)
        if error.validator == "required":
            name = RE_REQUIRED.match(error.message).group("name")
            path.append(name)
        key = ".".join(str(p) for p in path)
        return key, error.message

    def __unicode__(self):
        return "Model({name},{{{fields}}})".format(
            name=self.name, fields=",".join(self.keys())
        )

    __str__ = __unicode__


class RawModel(ModelBase):
    """
    A thin wrapper on ordered fields dict to store API doc metadata.
    Can also be used for response marshalling.

    :param str name: The model public name
    :param str mask: an optional default model mask
    :param bool strict: validation should raise error when there is param not provided in schema
    """

    wrapper = dict

    def __init__(self, name, *args, **kwargs):
        self.__mask__ = kwargs.pop("mask", None)
        self.__strict__ = kwargs.pop("strict", False)
        if self.__mask__ and not isinstance(self.__mask__, Mask):
            self.__mask__ = Mask(self.__mask__)
        super(RawModel, self).__init__(name, *args, **kwargs)

        def instance_clone(name, *parents):
            return self.__class__.clone(name, self, *parents)

        self.clone = instance_clone

    @property
    def _schema(self):
        properties = self.wrapper()
        required = set()
        discriminator = None
        for name, field in iteritems(self):
            field = instance(field)
            properties[name] = field.__schema__
            if field.required:
                required.add(name)
            if getattr(field, "discriminator", False):
                discriminator = name

        definition = {
            "required": sorted(list(required)) or None,
            "properties": properties,
            "discriminator": discriminator,
            "x-mask": str(self.__mask__) if self.__mask__ else None,
            "type": "object",
        }

        if self.__strict__:
            definition['additionalProperties'] = False

        return not_none(definition)

    @cached_property
    def resolved(self):
        """
        Resolve real fields before submitting them to marshal
        """
        # Duplicate fields
        resolved = copy.deepcopy(self)

        # Recursively copy parent fields if necessary
        for parent in self.__parents__:
            resolved.update(parent.resolved)

        # Handle discriminator
        candidates = [
            f for f in itervalues(resolved) if getattr(f, "discriminator", None)
        ]
        # Ensure the is only one discriminator
        if len(candidates) > 1:
            raise ValueError("There can only be one discriminator by schema")
        # Ensure discriminator always output the model name
        elif len(candidates) == 1:
            candidates[0].default = self.name

        return resolved

    def extend(self, name, fields):
        """
        Extend this model (Duplicate all fields)

        :param str name: The new model name
        :param dict fields: The new model extra fields

        :deprecated: since 0.9. Use :meth:`clone` instead.
        """
        warnings.warn(
            "extend is is deprecated, use clone instead",
            DeprecationWarning,
            stacklevel=2,
        )
        if isinstance(fields, (list, tuple)):
            return self.clone(name, *fields)
        else:
            return self.clone(name, fields)

    @classmethod
    def clone(cls, name, *parents):
        """
        Clone these models (Duplicate all fields)

        It can be used from the class

        >>> model = Model.clone(fields_1, fields_2)

        or from an Instanciated model

        >>> new_model = model.clone(fields_1, fields_2)

        :param str name: The new model name
        :param dict parents: The new model extra fields
        """
        fields = cls.wrapper()
        for parent in parents:
            fields.update(copy.deepcopy(parent))
        return cls(name, fields)

    def __deepcopy__(self, memo):
        obj = self.__class__(
            self.name,
            [(key, copy.deepcopy(value, memo)) for key, value in iteritems(self)],
            mask=self.__mask__,
            strict=self.__strict__,
        )
        obj.__parents__ = self.__parents__
        return obj


class Model(RawModel, dict, MutableMapping):
    """
    A thin wrapper on fields dict to store API doc metadata.
    Can also be used for response marshalling.

    :param str name: The model public name
    :param str mask: an optional default model mask
    """

    pass


class OrderedModel(RawModel, OrderedDict, MutableMapping):
    """
    A thin wrapper on ordered fields dict to store API doc metadata.
    Can also be used for response marshalling.

    :param str name: The model public name
    :param str mask: an optional default model mask
    """

    wrapper = OrderedDict


class SchemaModel(ModelBase):
    """
    Stores API doc metadata based on a json schema.

    :param str name: The model public name
    :param dict schema: The json schema we are documenting
    """

    def __init__(self, name, schema=None):
        super(SchemaModel, self).__init__(name)
        self._schema = schema or {}

    def __unicode__(self):
        return "SchemaModel({name},{schema})".format(
            name=self.name, schema=self._schema
        )

    __str__ = __unicode__
