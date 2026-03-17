# -*- coding: utf-8 -*-
"""Utilities to get schema instances/classes"""

import marshmallow


def resolve_schema_instance(schema):
    """Return schema instance for given schema (instance or class)

    :param type|Schema|str schema: instance, class or class name of marshmallow.Schema
    :return: schema instance of given schema (instance or class)
    """
    if isinstance(schema, type) and issubclass(schema, marshmallow.Schema):
        return schema()
    if isinstance(schema, marshmallow.Schema):
        return schema
    return marshmallow.class_registry.get_class(schema)()


def resolve_schema_cls(schema):
    """Return schema class for given schema (instance or class)

    :param type|Schema|str: instance, class or class name of marshmallow.Schema
    :return: schema class of given schema (instance or class)
    """
    if isinstance(schema, type) and issubclass(schema, marshmallow.Schema):
        return schema
    if isinstance(schema, marshmallow.Schema):
        return type(schema)
    return marshmallow.class_registry.get_class(schema)
