====================================
Django REST Framework JSON CamelCase
====================================

.. image:: https://travis-ci.org/vbabiy/djangorestframework-camel-case.svg?branch=master
        :target: https://travis-ci.org/vbabiy/djangorestframework-camel-case

.. image:: https://badge.fury.io/py/djangorestframework-camel-case.svg
    :target: https://badge.fury.io/py/djangorestframework-camel-case

Camel case JSON support for Django REST framework.

============
Installation
============

At the command line::

    $ pip install djangorestframework-camel-case

Add the render and parser to your django settings file.

.. code-block:: python

    # ...
    REST_FRAMEWORK = {

        'DEFAULT_RENDERER_CLASSES': (
            'djangorestframework_camel_case.render.CamelCaseJSONRenderer',
            'djangorestframework_camel_case.render.CamelCaseBrowsableAPIRenderer',
            # Any other renders
        ),

        'DEFAULT_PARSER_CLASSES': (
            # If you use MultiPartFormParser or FormParser, we also have a camel case version
            'djangorestframework_camel_case.parser.CamelCaseFormParser',
            'djangorestframework_camel_case.parser.CamelCaseMultiPartParser',
            'djangorestframework_camel_case.parser.CamelCaseJSONParser',
            # Any other parsers
        ),
    }
    # ...

Add query param middleware to django settings file.

.. code-block:: python

    # ...
    MIDDLEWARE = [
        # Any other middleware
        'djangorestframework_camel_case.middleware.CamelCaseMiddleWare',
    ]
    # ...

=================
Swapping Renderer
=================

By default the package uses `rest_framework.renderers.JSONRenderer`. If you want
to use another renderer, the two possible are:

`drf_orjson_renderer.renderers.ORJSONRenderer` or
`rest_framework.renderers.UnicodeJSONRenderer` for DRF < 3.0,specify it in your django
settings file.

.. code-block:: python

    # ...
    JSON_CAMEL_CASE = {
        'RENDERER_CLASS': 'drf_orjson_renderer.renderers.ORJSONRenderer'
    }
    # ...

=====================
Underscoreize Options
=====================


**No Underscore Before Number**


As raised in `this comment <https://github.com/krasa/StringManipulation/issues/8#issuecomment-121203018>`_
there are two conventions of snake case.

.. code-block:: text

    # Case 1 (Package default)
    v2Counter -> v_2_counter
    fooBar2 -> foo_bar_2

    # Case 2
    v2Counter -> v2_counter
    fooBar2 -> foo_bar2


By default, the package uses the first case. To use the second case, specify it in your django settings file.

.. code-block:: python

    REST_FRAMEWORK = {
        # ...
        'JSON_UNDERSCOREIZE': {
            'no_underscore_before_number': True,
        },
        # ...
    }

Alternatively, you can change this behavior on a class level by setting `json_underscoreize`:

.. code-block:: python

    from djangorestframework_camel_case.parser import CamelCaseJSONParser
    from rest_framework.generics import CreateAPIView

    class NoUnderscoreBeforeNumberCamelCaseJSONParser(CamelCaseJSONParser):
        json_underscoreize = {'no_underscore_before_number': True}

    class MyView(CreateAPIView):
        queryset = MyModel.objects.all()
        serializer_class = MySerializer
        parser_classes = (NoUnderscoreBeforeNumberCamelCaseJSONParser,)

=============
Ignore Fields
=============

You can also specify fields which should not have their data changed.
The specified field(s) would still have their name change, but there would be no recursion.
For example:

.. code-block:: python

    data = {"my_key": {"do_not_change": 1}}

Would become:

.. code-block:: python

    {"myKey": {"doNotChange": 1}}

However, if you set in your settings:

.. code-block:: python

    REST_FRAMEWORK = {
        # ...
        "JSON_UNDERSCOREIZE": {
            # ...
            "ignore_fields": ("my_key",),
            # ...
        },
        # ...
    }

The `my_key` field would not have its data changed:

.. code-block:: python

    {"myKey": {"do_not_change": 1}}

===========
Ignore Keys
===========

You can also specify keys which should *not* be renamed.
The specified field(s) would still change (even recursively).
For example:

.. code-block:: python

    data = {"unchanging_key": {"change_me": 1}}

Would become:

.. code-block:: python

    {"unchangingKey": {"changeMe": 1}}

However, if you set in your settings:

.. code-block:: python

    REST_FRAMEWORK = {
        # ...
        "JSON_UNDERSCOREIZE": {
            # ...
            "ignore_keys": ("unchanging_key",),
            # ...
        },
        # ...
    }

The `unchanging_key` field would not be renamed:

.. code-block:: python

    {"unchanging_key": {"changeMe": 1}}

ignore_keys and ignore_fields can be applied to the same key if required.

=============
Running Tests
=============

To run the current test suite, execute the following from the root of he project::

    $ python -m unittest discover


=======
License
=======

* Free software: BSD license
