=======================
marshmallow-oneofschema
=======================

|pypi| |build-status| |marshmallow-support|

.. |pypi| image:: https://badgen.net/pypi/v/marshmallow-oneofschema
    :target: https://pypi.org/project/marshmallow-oneofschema/
    :alt: PyPI package

.. |build-status| image:: https://github.com/marshmallow-code/marshmallow-oneofschema/actions/workflows/build-release.yml/badge.svg
    :target: https://github.com/marshmallow-code/flask-marshmallow/actions/workflows/build-release.yml
    :alt: Build status

.. |marshmallow-support| image:: https://badgen.net/badge/marshmallow/3,4?list=1
    :target: https://marshmallow.readthedocs.io/en/latest/upgrading.html
    :alt: marshmallow 3|4 compatible

An extension to marshmallow to support schema (de)multiplexing.

marshmallow is a fantastic library for serialization and deserialization of data.
For more on that project see its `GitHub <https://github.com/marshmallow-code/marshmallow>`_
page or its `Documentation <http://marshmallow.readthedocs.org/en/latest/>`_.

This library adds a special kind of schema that actually multiplexes other schemas
based on object type. When serializing values, it uses get_obj_type() method
to get object type name. Then it uses ``type_schemas`` name-to-Schema mapping
to get schema for that particular object type, serializes object using that
schema and adds an extra field with name of object type. Deserialization is reverse.

Installing
----------

::

    $ pip install marshmallow-oneofschema

Example
-------

The code below demonstrates how to set up a polymorphic schema. For the full context check out the tests.
Once setup the schema should act like any other schema. If it does not then please file an Issue.

.. code:: python

    import marshmallow
    import marshmallow.fields
    from marshmallow_oneofschema import OneOfSchema


    class Foo:
        def __init__(self, foo):
            self.foo = foo


    class Bar:
        def __init__(self, bar):
            self.bar = bar


    class FooSchema(marshmallow.Schema):
        foo = marshmallow.fields.String(required=True)

        @marshmallow.post_load
        def make_foo(self, data, **kwargs):
            return Foo(**data)


    class BarSchema(marshmallow.Schema):
        bar = marshmallow.fields.Integer(required=True)

        @marshmallow.post_load
        def make_bar(self, data, **kwargs):
            return Bar(**data)


    class MyUberSchema(OneOfSchema):
        type_schemas = {"foo": FooSchema, "bar": BarSchema}

        def get_obj_type(self, obj):
            if isinstance(obj, Foo):
                return "foo"
            elif isinstance(obj, Bar):
                return "bar"
            else:
                raise Exception("Unknown object type: {}".format(obj.__class__.__name__))


    MyUberSchema().dump([Foo(foo="hello"), Bar(bar=123)], many=True)
    # => [{'type': 'foo', 'foo': 'hello'}, {'type': 'bar', 'bar': 123}]

    MyUberSchema().load(
        [{"type": "foo", "foo": "hello"}, {"type": "bar", "bar": 123}], many=True
    )
    # => [Foo('hello'), Bar(123)]

By default get_obj_type() returns obj.__class__.__name__, so you can just reuse that
to save some typing:

.. code:: python

    class MyUberSchema(OneOfSchema):
        type_schemas = {"Foo": FooSchema, "Bar": BarSchema}

You can customize type field with `type_field` class property:

.. code:: python

    class MyUberSchema(OneOfSchema):
        type_field = "object_type"
        type_schemas = {"Foo": FooSchema, "Bar": BarSchema}


    MyUberSchema().dump([Foo(foo="hello"), Bar(bar=123)], many=True)
    # => [{'object_type': 'Foo', 'foo': 'hello'}, {'object_type': 'Bar', 'bar': 123}]

You can use resulting schema everywhere marshmallow.Schema can be used, e.g.

.. code:: python

    import marshmallow as m
    import marshmallow.fields as f


    class MyOtherSchema(m.Schema):
        items = f.List(f.Nested(MyUberSchema))

License
-------

MIT licensed. See the bundled `LICENSE <https://github.com/marshmallow-code/marshmallow-oneofschema/blob/master/LICENSE>`_ file for more details.
