Django REST - FlexFields
========================

Flexible, dynamic fields and nested models for Django REST Framework
serializers. Works with both Python 2 and 3.

Overview
========

FlexFields (DRF-FF) for `Django REST
Framework <https://django-rest-framework.org>`__ is a package designed
to provide a common baseline of functionality for dynamically setting
fields and nested models within DRF serializers. To remove unneeded
fields, you can dynamically set fields, including nested fields, via URL
parameters ``(?fields=name,address.zip)`` or when configuring
serializers. Additionally, you can dynamically expand fields from simple
values to complex nested models, or treat fields as "deferred", and
expand them on an as-needed basis.

This package is designed for simplicity and provides two classes - a
viewset class and a serializer class (or mixin) - with minimal magic and
entanglement with DRF's foundational classes. Unless DRF makes
significant changes to its serializers, you can count on this package to
work (and if major changes are made, this package will be updated
shortly thereafter). If you are familar with Django REST Framework, it
shouldn't take you long to read over the code and see how it works.

There are similar packages, such as the powerful `Dynamic
REST <https://github.com/AltSchool/dynamic-rest>`__, which does what
this package does and more, but you may not need all those bells and
whistles. There is also the more basic `Dynamic Fields
Mixin <https://github.com/dbrgn/drf-dynamic-fields>`__, but it lacks
functionality for field expansion and dot-notation field customiziation.

Table of Contents:

-  `Installation <#installation>`__
-  `Requirements <#requirements>`__
-  `Basics <#basics>`__
-  `Dynamic Field Expansion <#dynamic-field-expansion>`__
-  `Deferred Fields <#deferred-fields>`__
-  `Deep, Nested Expansion <#deep-nested-expansion>`__
-  `Configuration from Serializer
   Options <#configuration-from-serializer-options>`__
-  `Field Expansion on "List" Views <#field-expansion-on-list-views>`__
-  `Use "~all" to Expand All Available
   Fields <#use-all-to-expand-all-available-fields>`__
-  `Dynamically Setting Fields/Sparse
   Fieldsets <#dynamically-setting-fields>`__
-  `From URL Parameters <#from-url-parameters>`__
-  `From Serializer Options <#from-serializer-options>`__
-  `Combining Dynamically-Set Fields and Field
   Expansion <#combining-dynamically-set-fields-and-field-expansion>`__
-  `Serializer Introspection <#serializer-introspection>`__
-  `Lazy evaluation of serializer <#lazy-evaluation-of-serializer>`__
-  `Query optimization
   (experimental) <#query-optimization-experimental>`__
-  `Change Log <#changelog>`__
-  `Testing <#testing>`__
-  `License <#license>`__

Installation
============

::

    pip install drf-flex-fields

Requirements
============

-  Python >= 2.7
-  Django >= 1.8

Basics
======

To use this package's functionality, your serializers need to subclass
``FlexFieldsModelSerializer`` or use the provided
``FlexFieldsSerializerMixin``. If you would like built-in protection for
controlling when clients are allowed to expand resources when listing
resource collections, your viewsets need to subclass
``FlexFieldsModelViewSet``.

.. code:: python

    from rest_flex_fields import FlexFieldsModelViewSet, FlexFieldsModelSerializer

    class PersonViewSet(FlexFieldsModelViewSet):
        queryset = models.Person.objects.all()
        serializer_class = PersonSerializer
        # Whitelist fields that can be expanded when listing resources
        permit_list_expands = ['country']

    class CountrySerializer(FlexFieldsModelSerializer):
        class Meta:
            model = Country
            fields = ('id', 'name', 'population')

    class PersonSerializer(FlexFieldsModelSerializer):
        class Meta:
            model = Person
            fields = ('id', 'name', 'country', 'occupation')

            expandable_fields = {
                'country': (CountrySerializer, {'source': 'country'})
            }

Now you can make requests like
``GET /person?expand=country&fields=id,name,country`` to dynamically
manipulate which fields are included, as well as expand primitive fields
into nested objects. You can also use dot notation to control both the
``fields`` and ``expand`` settings at arbitrary levels of depth in your
serialized responses. Read on to learn the details and see more complex
examples.

:heavy\_check\_mark: The examples below subclass
``FlexFieldsModelSerializer``, but the same can be accomplished by
mixing in ``FlexFieldsSerializerMixin``, which is also importable from
the same ``rest_flex_fields`` package.

Dynamic Field Expansion
=======================

To define an expandable field, add it to the ``expandable_fields``
within your serializer:

.. code:: python

    class CountrySerializer(FlexFieldsModelSerializer):
        class Meta:
            model = Country
            fields = ['name', 'population']


    class PersonSerializer(FlexFieldsModelSerializer):
        country = serializers.PrimaryKeyRelatedField(read_only=True)

        class Meta:
            model = Person
            fields = ['id', 'name', 'country', 'occupation']

            expandable_fields = {
                'country': (CountrySerializer, {'source': 'country', 'fields': ['name']})
            }

If the default serialized response is the following:

.. code:: json

    {
      "id" : 13322,
      "name" : "John Doe",
      "country" : 12,
      "occupation" : "Programmer",
    }

When you do a ``GET /person/13322?expand=country``, the response will
change to:

.. code:: json

    {
      "id" : 13322,
      "name" : "John Doe",
      "country" : {
        "name" : "United States"
      },
      "occupation" : "Programmer",
    }

Notice how ``population`` was ommitted from the nested ``country``
object. This is because ``fields`` was set to ``['name']`` when passed
to the embedded ``CountrySerializer``. You will learn more about this
later on.

Deferred Fields
---------------

Alternatively, you could treat ``country`` as a "deferred" field by not
defining it among the default fields. To make a field deferred, only
define it within the serializer's ``expandable_fields``.

Deep, Nested Expansion
----------------------

Let's say you add ``StateSerializer`` as serializer nested inside the
country serializer above:

.. code:: python

    class StateSerializer(FlexFieldsModelSerializer):
        class Meta:
            model = State
            fields = ['name', 'population']


    class CountrySerializer(FlexFieldsModelSerializer):
        class Meta:
            model = Country
            fields = ['name', 'population']

            expandable_fields = {
                'states': (StateSerializer, {'source': 'states', 'many': True})
            }

    class PersonSerializer(FlexFieldsModelSerializer):
        country = serializers.PrimaryKeyRelatedField(read_only=True)

        class Meta:
            model = Person
            fields = ['id', 'name', 'country', 'occupation']

            expandable_fields = {
                'country': (CountrySerializer, {'source': 'country', 'fields': ['name']})
            }

Your default serialized response might be the following for ``person``
and ``country``, respectively:

.. code:: json

    {
      "id" : 13322,
      "name" : "John Doe",
      "country" : 12,
      "occupation" : "Programmer",
    }

    {
      "id" : 12,
      "name" : "United States",
      "states" : "http://www.api.com/countries/12/states"
    }

But if you do a ``GET /person/13322?expand=country.states``, it would
be:

.. code:: json

    {
      "id" : 13322,
      "name" : "John Doe",
      "occupation" : "Programmer",
      "country" : {
        "id" : 12,
        "name" : "United States",
        "states" : [
          {
            "name" : "Ohio",
            "population": 11000000
          }
        ]
      }
    }

Please be kind to your database, as this could incur many additional
queries. Though, you can mitigate this impact through judicious use of
``prefetch_related`` and ``select_related`` when defining the queryset
for your viewset.

Configuration from Serializer Options
-------------------------------------

You could accomplish the same result (expanding the ``states`` field
within the embedded country serializer) by explicitly passing the
``expand`` option within your serializer:

.. code:: python

    class PersonSerializer(FlexFieldsModelSerializer):

        class Meta:
            model = Person
            fields = ['id', 'name', 'country', 'occupation']

            expandable_fields = {
                'country': (CountrySerializer, {'source': 'country', 'expand': ['states']})
            }

Field Expansion on "List" Views
-------------------------------

By default, when subclassing ``FlexFieldsModelViewSet``, you can only
expand fields when you are retrieving single resources, in order to
protect yourself from careless clients. However, if you would like to
make a field expandable even when listing collections, you can add the
field's name to the ``permit_list_expands`` property on the viewset.
Just make sure you are wisely using ``select_related`` and
``prefetch_related`` in the viewset's queryset. You can take advantage
of a utility function, ``is_expanded()`` to adjust the queryset
accordingly.

Example:

.. code:: python

    from drf_flex_fields import is_expanded

    class PersonViewSet(FlexFieldsModelViewSet):
        permit_list_expands = ['employer']
        serializer_class = PersonSerializer

        def get_queryset(self):
            queryset = models.Person.objects.all()
            if is_expanded(self.request, 'employer'):
                queryset = queryset.select_related('employer')
            return queryset

Use "~all" to Expand All Available Fields
-----------------------------------------

You can set ``expand=~all`` to automatically expand all fields that are
available for expansion. This will take effect only for the top-level
serializer; if you need to also expand fields that are present on deeply
nested models, then you will need to explicitly pass their values using
dot notation.

Dynamically Setting Fields (Sparse Fields)
==========================================

You can use either they ``fields`` or ``omit`` keywords to declare only
the fields you want to include or to specify fields that should be
excluded.

From URL Parameters
-------------------

You can dynamically set fields, with the configuration originating from
the URL parameters or serializer options.

Consider this as a default serialized response:

.. code:: json

    {
      "id" : 13322,
      "name" : "John Doe",
      "country" : {
        "name" : "United States",
        "population": 330000000
      },
      "occupation" : "Programmer",
      "hobbies" : ["rock climbing", "sipping coffee"]
    }

To whittle down the fields via URL parameters, simply add
``?fields=id,name,country`` to your requests to get back:

.. code:: json

    {
      "id" : 13322,
      "name" : "John Doe",
      "country" : {
        "name" : "United States",
        "population: 330000000
      }
    }

Or, for more specificity, you can use dot-notation,
``?fields=id,name,country.name``:

.. code:: json

    {
      "id" : 13322,
      "name" : "John Doe",
      "country" : {
        "name" : "United States",
      }
    }

Or, if you want to leave out the nested country object, do
``?omit=country``:

.. code:: json

    {
      "id" : 13322,
      "name" : "John Doe",
      "occupation" : "Programmer",
      "hobbies" : ["rock climbing", "sipping coffee"]
    }

From Serializer Options
-----------------------

You could accomplish the same outcome as the example above by passing
options to your serializers. With this approach, you lose runtime
dynamism, but gain the ability to re-use serializers, rather than
creating a simplified copy of a serializer for the purposes of embedding
it. The example below uses the ``fields`` keyword, but you can also pass
in keyword argument for ``omit`` to exclude specific fields.

.. code:: python

    from rest_flex_fields import FlexFieldsModelSerializer

    class CountrySerializer(FlexFieldsModelSerializer):
        class Meta:
            model = Country
            fields = ['id', 'name', 'population']

    class PersonSerializer(FlexFieldsModelSerializer):
        country: CountrySerializer(fields=['name'])
        class Meta:
            model = Person
            fields = ['id', 'name', 'country', 'occupation', 'hobbies']


    serializer = PersonSerializer(person, fields=["id", "name", "country.name"])
    print(serializer.data)

    >>>{
      "id": 13322,
      "name": "John Doe",
      "country": {
        "name": "United States",
      }
    }

Combining Dynamically Set Fields and Field Expansion
====================================================

You may be wondering how things work if you use both the ``expand`` and
``fields`` option, and there is overlap. For example, your serialized
person model may look like the following by default:

.. code:: json

    {
      "id": 13322,
      "name": "John Doe",
      "country": {
        "name": "United States",
      }
    }

However, you make the following request
``HTTP GET /person/13322?include=id,name&expand=country``. You will get
the following back:

.. code:: json

    {
      "id": 13322,
      "name": "John Doe"
    }

The ``include`` field takes precedence over ``expand``. That is, if a
field is not among the set that is explicitly alllowed, it cannot be
expanded. If such a conflict occurs, you will not pay for the extra
database queries - the expanded field will be silently abandoned.

Serializer Introspection
========================

When using an instance of ``FlexFieldsModelSerializer``, you can examine
the property ``expanded_fields`` to discover which fields, if any, have
been dynamically expanded.

Lazy evaluation of serializer
=============================

If you want to lazily evaluate the reference to your nested serializer
class from a string inside expandable\_fields, you need to use this
syntax:

.. code:: python

    expandable_fields = {
        'record_set': ('<app_name>.RelatedSerializer', {'source': 'related_set', 'many': True})
    }

Substitute the name of your Django app where the serializer is found for
``<app_name>``.

This allows to reference a serializer that has not yet been defined.

Query optimization (experimental)
=================================

An experimental filter backend is available to help you automatically
reduce the number of SQL queries and their transfer size. *This feature
has not been tested thorougly and any help testing and reporting bugs is
greatly appreciated.* You can add FlexFieldFilterBackend to
``DEFAULT_FILTER_BACKENDS`` in the settings:

.. code:: python

    # settings.py

    REST_FRAMEWORK = {
        'DEFAULT_FILTER_BACKENDS': (
            'rest_flex_fields.filter_backends.FlexFieldsFilterBackend',
            # ...        
        ),
        # ...
    }

It will automatically call ``select_related`` and ``prefetch_related``
on the current QuerySet by determining which fields are needed from
many-to-many and foreign key-related models. For sparse fields requests
(``?omit=fieldX,fieldY`` or ``?fields=fieldX,fieldY``), the backend will
automatically call ``only(*field_names)`` using only the fields needed
for serialization.

**WARNING:** The optimization currently works only for one nesting
level.

Changelog 
==========

0.6.0 (May 2019)
----------------

-  Adds experimental support for automatically SQL query optimization
   via a ``FlexFieldsFilterBackend``. Thanks ADR-007!
-  Adds CircleCI config file. Thanks mikeIFTS!
-  Moves declaration of ``expandable_fields`` to ``Meta`` class on
   serialzer for consistency with DRF (will continue to support
   declaration as class property)

0.5.0 (April 2019)
------------------

-  Added support for ``omit`` keyword for field exclusion. Code clean up
   and improved test coverage.

0.3.4 (May 2018)
----------------

-  Handle case where ``request`` is ``None`` when accessing request
   object from serializer. Thanks @jsatt!

0.3.3 (April 2018)
------------------

-  Exposes ``FlexFieldsSerializerMixin`` in addition to
   ``FlexFieldsModelSerializer``. Thanks @jsatt!

Testing
=======

Tests are found in a simplified DRF project in the ``/tests`` folder.
Install the project requirements and do ``./manage.py test`` to run
them.

License
=======

See `License <LICENSE.md>`__.
