Django Rest Framework Filters
=============================

.. image:: https://travis-ci.org/philipn/django-rest-framework-filters.svg?branch=master
  :target: https://travis-ci.org/philipn/django-rest-framework-filters

.. image:: https://codecov.io/gh/philipn/django-rest-framework-filters/branch/master/graph/badge.svg
  :target: https://codecov.io/gh/philipn/django-rest-framework-filters

.. image:: https://img.shields.io/pypi/v/djangorestframework-filters.svg
  :target: https://pypi.python.org/pypi/djangorestframework-filters

.. image:: https://img.shields.io/pypi/pyversions/djangorestframework-filters.svg
  :target: https://pypi.org/project/djangorestframework-filters/

.. image:: https://img.shields.io/pypi/l/tox-factor.svg
  :target: https://pypi.org/project/djangorestframework-filters/


``django-rest-framework-filters`` is an extension to `Django REST framework`_ and `Django filter`_
that makes it easy to filter across relationships. Historically, this extension also provided a
number of additional features and fixes, however the number of features has shrunk as they are
merged back into ``django-filter``.

.. _`Django REST framework`: https://github.com/tomchristie/django-rest-framework
.. _`Django filter`: https://github.com/carltongibson/django-filter

Using ``django-rest-framework-filters``, we can easily do stuff like::

    /api/article?author__first_name__icontains=john
    /api/article?is_published!=true

----

**!** These docs pertain to the upcoming 1.0 release. Current docs can be found `here`_.

.. _`here`: https://github.com/philipn/django-rest-framework-filters/blob/v0.10.2/README.rst

----

**!** The 1.0 pre-release is compatible with django-filter 2.x and can be installed with
``pip install --pre``.

----

.. contents::
    **Table of Contents**
    :local:
    :depth: 2
    :backlinks: none

Features
--------

* Easy filtering across relationships.
* Support for method filtering across relationships.
* Automatic filter negation with a simple ``param!=value`` syntax.
* Backend for complex operations on multiple filtered querysets. eg, ``q1 | q2``.


Requirements
------------

* **Python**: 3.5, 3.6, 3.7, 3.8
* **Django**: 1.11, 2.0, 2.1, 2.2, 3.0, 3.1
* **DRF**: 3.11
* **django-filter**: 2.1, 2.2 (Django 2.0+)


Installation
------------

Install with pip, or your preferred package manager:

.. code-block:: bash

    $ pip install djangorestframework-filters


Add to your ``INSTALLED_APPS`` setting:

.. code-block:: python

    INSTALLED_APPS = [
        'rest_framework_filters',
        ...
    ]


``FilterSet`` usage
-------------------

Upgrading from ``django-filter`` to ``django-rest-framework-filters`` is straightforward:

* Import from ``rest_framework_filters`` instead of from ``django_filters``
* Use the ``rest_framework_filters`` backend instead of the one provided by ``django_filter``.

.. code-block:: python

    # django-filter
    from django_filters.rest_framework import FilterSet, filters

    class ProductFilter(FilterSet):
        manufacturer = filters.ModelChoiceFilter(queryset=Manufacturer.objects.all())
        ...


    # django-rest-framework-filters
    import rest_framework_filters as filters

    class ProductFilter(filters.FilterSet):
        manufacturer = filters.ModelChoiceFilter(queryset=Manufacturer.objects.all())
        ...


To use the django-rest-framework-filters backend, add the following to your settings:

.. code-block:: python

    REST_FRAMEWORK = {
        'DEFAULT_FILTER_BACKENDS': (
            'rest_framework_filters.backends.RestFrameworkFilterBackend', ...
        ),
        ...


Once configured, you can continue to use all of the filters found in ``django-filter``.


Filtering across relationships
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can easily traverse multiple relationships when filtering by using ``RelatedFilter``:

.. code-block:: python

    from rest_framework import viewsets
    import rest_framework_filters as filters


    class ManagerFilter(filters.FilterSet):
        class Meta:
            model = Manager
            fields = {'name': ['exact', 'in', 'startswith']}


    class DepartmentFilter(filters.FilterSet):
        manager = filters.RelatedFilter(ManagerFilter, field_name='manager', queryset=Manager.objects.all())

        class Meta:
            model = Department
            fields = {'name': ['exact', 'in', 'startswith']}


    class CompanyFilter(filters.FilterSet):
        department = filters.RelatedFilter(DepartmentFilter, field_name='department', queryset=Department.objects.all())

        class Meta:
            model = Company
            fields = {'name': ['exact', 'in', 'startswith']}


    # company viewset
    class CompanyView(viewsets.ModelViewSet):
        filter_class = CompanyFilter
        ...

Example filter calls:

.. code-block::

    /api/companies?department__name=Accounting
    /api/companies?department__manager__name__startswith=Bob

``queryset`` callables
""""""""""""""""""""""

Since ``RelatedFilter`` is a subclass of ``ModelChoiceFilter``, the ``queryset`` argument supports callable behavior.
In the following example, the set of departments is restricted to those in the user's company.

.. code-block:: python

    def departments(request):
        company = request.user.company
        return company.department_set.all()

    class EmployeeFilter(filters.FilterSet):
        department = filters.RelatedFilter(filterset=DepartmentFilter, queryset=departments)
        ...

Recursive & Circular relationships
""""""""""""""""""""""""""""""""""

Recursive relations are also supported. Provide the module path as a string in place of the filterset class.

.. code-block:: python

    class PersonFilter(filters.FilterSet):
        name = filters.AllLookupsFilter(field_name='name')
        best_friend = filters.RelatedFilter('people.views.PersonFilter', field_name='best_friend', queryset=Person.objects.all())

        class Meta:
            model = Person


This feature is also useful for circular relationships, where a related filterset may not yet be created. Note that
you can pass the related filterset by name if it's located in the same module as the parent filterset.

.. code-block:: python

    class BlogFilter(filters.FilterSet):
        post = filters.RelatedFilter('PostFilter', queryset=Post.objects.all())

    class PostFilter(filters.FilterSet):
        blog = filters.RelatedFilter('BlogFilter', queryset=Blog.objects.all())


Supporting ``Filter.method``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``django_filters.MethodFilter`` has been deprecated and reimplemented as the ``method`` argument
to all filter classes. It incorporates some of the implementation details of the old
``rest_framework_filters.MethodFilter``, but requires less boilerplate and is simpler to write.

* It is no longer necessary to perform empty/null value checking.
* You may use any filter class (``CharFilter``, ``BooleanFilter``, etc...) which will
  validate input values for you.
* The argument signature has changed from ``(name, qs, value)`` to ``(qs, name, value)``.

.. code-block:: python

    class PostFilter(filters.FilterSet):
        # Note the use of BooleanFilter, the original model field's name, and the method argument.
        is_published = filters.BooleanFilter(field_name='date_published', method='filter_is_published')

        class Meta:
            model = Post
            fields = ['title', 'content']

        def filter_is_published(self, qs, name, value):
            """
            `is_published` is based on the `date_published` model field.
            If the publishing date is null, then the post is not published.
            """
            # incoming value is normalized as a boolean by BooleanFilter
            isnull = not value
            lookup_expr = LOOKUP_SEP.join([name, 'isnull'])

            return qs.filter(**{lookup_expr: isnull})

    class AuthorFilter(filters.FilterSet):
        posts = filters.RelatedFilter('PostFilter', queryset=Post.objects.all())

        class Meta:
            model = Author
            fields = ['name']

The above would enable the following filter calls:

.. code-block::

    /api/posts?is_published=true
    /api/authors?posts__is_published=true


In the first API call, the filter method receives a queryset of posts. In the second,
it receives a queryset of users. The filter method in the example modifies the lookup
name to work across the relationship, allowing you to find published posts, or authors
who have published posts.

Automatic Filter Negation/Exclusion
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

FilterSets support automatic exclusion using a simple ``param!=value`` syntax. This syntax
internally sets the ``exclude`` property on the filter.

.. code-block::

    /api/page?title!=The%20Park

This syntax supports regular filtering combined with exclusion filtering. For example, the
following would search for all articles containing "Hello" in the title, while excluding
those containing "World".

.. code-block::

    /api/articles?title__contains=Hello&title__contains!=World

Note that most filters only accept a single query parameter. In the above, ``title__contains``
and ``title__contains!`` are interpreted as two separate query parameters. The following would
probably be invalid, although it depends on the specifics of the individual filter class:

.. code-block::

    /api/articles?title__contains=Hello&title__contains!=World&title_contains!=Friend


Allowing any lookup type on a field
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you need to enable several lookups for a field, django-filter provides the dict-syntax for
``Meta.fields``.

.. code-block:: python

    class ProductFilter(filters.FilterSet):
        class Meta:
            model = Product
            fields = {
                'price': ['exact', 'lt', 'gt', ...],
            }

``django-rest-framework-filters`` also allows you to enable all possible lookups for any field.
This can be achieved through the use of ``AllLookupsFilter`` or using the ``'__all__'`` value in
the ``Meta.fields`` dict-style syntax. Generated filters (``Meta.fields``, ``AllLookupsFilter``)
will never override your declared filters.

Note that using all lookups comes with the same admonitions as enabling ``'__all__'`` fields in
django forms (`docs`_). Exposing all lookups may allow users to construct queries that
inadvertently leak data. Use this feature responsibly.

.. _`docs`: https://docs.djangoproject.com/en/1.10/topics/forms/modelforms/#selecting-the-fields-to-use

.. code-block:: python

    class ProductFilter(filters.FilterSet):
        # Not overridden by `__all__`
        price__gt = filters.NumberFilter(field_name='price', lookup_expr='gt', label='Minimum price')

        class Meta:
            model = Product
            fields = {
                'price': '__all__',
            }

    # or

    class ProductFilter(filters.FilterSet):
        price = filters.AllLookupsFilter()

        # Not overridden by `AllLookupsFilter`
        price__gt = filters.NumberFilter(field_name='price', lookup_expr='gt', label='Minimum price')

        class Meta:
            model = Product

You cannot combine ``AllLookupsFilter`` with ``RelatedFilter`` as the filter names would clash.

.. code-block:: python

    class ProductFilter(filters.FilterSet):
        manufacturer = filters.RelatedFilter('ManufacturerFilter', queryset=Manufacturer.objects.all())
        manufacturer = filters.AllLookupsFilter()

To work around this, you have the following options:

.. code-block:: python

    class ProductFilter(filters.FilterSet):
        manufacturer = filters.RelatedFilter('ManufacturerFilter', queryset=Manufacturer.objects.all())

        class Meta:
            model = Product
            fields = {
                'manufacturer': '__all__',
            }

    # or

    class ProductFilter(filters.FilterSet):
        manufacturer = filters.RelatedFilter('ManufacturerFilter', queryset=Manufacturer.objects.all(), lookups='__all__')  # `lookups` also accepts a list

        class Meta:
            model = Product


Can I mix and match ``django-filter`` and ``django-rest-framework-filters``?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes you can. ``django-rest-framework-filters`` is simply an extension of ``django-filter``. Note
that ``RelatedFilter`` and other ``django-rest-framework-filters`` features are designed to work
with ``rest_framework_filters.FilterSet`` and will not function on a ``django_filters.FilterSet``.
However, the target ``RelatedFilter.filterset`` may point to a ``FilterSet`` from either package,
and both ``FilterSet`` implementations are compatible with the other's DRF backend.

.. code-block:: python

    # valid
    class VanillaFilter(django_filters.FilterSet):
        ...

    class DRFFilter(rest_framework_filters.FilterSet):
        vanilla = rest_framework_filters.RelatedFilter(filterset=VanillaFilter, queryset=...)


    # invalid
    class DRFFilter(rest_framework_filters.FilterSet):
        ...

    class VanillaFilter(django_filters.FilterSet):
        drf = rest_framework_filters.RelatedFilter(filterset=DRFFilter, queryset=...)


Caveats & Limitations
~~~~~~~~~~~~~~~~~~~~~

``MultiWidget`` is incompatible
"""""""""""""""""""""""""""""""

djangorestframework-filters is not compatible with form widgets that parse query names that differ from the filter's
attribute name. Although this only practically applies to ``MultiWidget``, it is a general limitation that affects
custom widgets that also have this behavior. Affected filters include ``RangeFilter``, ``DateTimeFromToRangeFilter``,
``DateFromToRangeFilter``, ``TimeRangeFilter``, and ``NumericRangeFilter``.

To demonstrate the incompatiblity, take the following filterset:

.. code-block:: python

    class PostFilter(FilterSet):
        publish_date = filters.DateFromToRangeFilter()

The above filter allows users to perform a ``range`` query on the publication date. The filter class internally uses
``MultiWidget`` to separately parse the upper and lower bound values. The incompatibility lies in that ``MultiWidget``
appends an index to its inner widget names. Instead of parsing ``publish_date``, it expects ``publish_date_0`` and
``publish_date_1``. It is possible to fix this by including the attribute name in the querystring, although this is
not recommended.

.. code-block::

    ?publish_date_0=2016-01-01&publish_date_1=2016-02-01&publish_date=

``MultiWidget`` is also discouraged since:

* ``core-api`` field introspection fails for similar reasons
* ``_0`` and ``_1`` are less API-friendly than ``_min`` and ``_max``

The recommended solutions are to either:

* Create separate filters for each of the sub-widgets (such as ``publish_date_min`` and ``publish_date_max``).
* Use a CSV-based filter such as those derived from ``BaseCSVFilter``/``BaseInFilter``/``BaseRangeFilter``. eg,

.. code-block::

    ?publish_date__range=2016-01-01,2016-02-01


Complex Operations
------------------

The ``ComplexFilterBackend`` defines a custom querystring syntax and encoding process that enables the expression of
`complex queries`_. This syntax extends standard querystrings with the ability to define multiple sets of parameters
and operators for how the queries should be combined.

.. _`complex queries`: https://docs.djangoproject.com/en/2.0/topics/db/queries/#complex-lookups-with-q-objects

----

**!** Note that this feature is experimental. Bugs may be encountered, and the backend is subject to change.

----

To understand the backend more fully, consider a query to find all articles that contain titles starting with either
"Who" or "What". The underlying query could be represented with the following:

.. code-block:: python

    q1 = Article.objects.filter(title__startswith='Who')
    q2 = Article.objects.filter(title__startswith='What')
    return q1 | q2

Now consider the query, but modified with upper and lower date bounds:

.. code-block:: python

    q1 = Article.objects.filter(title__startswith='Who').filter(publish_date__lte='2005-01-01')
    q2 = Article.objects.filter(title__startswith='What').filter(publish_date__gte='2010-01-01')
    return q1 | q2

Using just a ``FilterSet``, it is certainly feasible to represent the former query by writing a custom filter class.
However, it is less feasible with the latter query, where multiple sets of varying data types and lookups need to be
validated. In contrast, the ``ComplexFilterBackend`` can create this complex query through the arbitrary combination
of a simple filter. To support the above, the querystring needs to be created with minimal changes. Unencoded example:

.. code-block::

    (title__startswith=Who&publish_date__lte=2005-01-01) | (title__startswith=What&publish_date__gte=2010-01-01)

By default, the backend combines queries with both ``&`` (AND) and ``|`` (OR), and supports unary negation ``~``. E.g.,

.. code-block::

    (param1=value1) & (param2=value2) | ~(param3=value3)

The backend supports both standard and complex queries. To perform complex queries, the query must be encoded and set
as the value of the ``complex_filter_param`` (defaults to ``filters``). To perform standard queries, use the backend
in the same manner as the ``RestFrameworkFilterBackend``.


Configuring ``ComplexFilterBackend``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Similar to other backends, ``ComplexFilterBackend`` must be added to a view's ``filter_backends`` atribute. Either add
it to the ``DEFAULT_FILTER_BACKENDS`` setting, or set it as a backend on the view class.

.. code-block:: python

    REST_FRAMEWORK = {
        'DEFAULT_FILTER_BACKENDS': (
            'rest_framework_filters.backends.ComplexFilterBackend',
        ),
    }

    # or

    class MyViewSet(generics.ListAPIView):
        filter_backends = (rest_framework_filters.backends.ComplexFilterBackend, )
        ...

You may customize how queries are combined by subclassing ``ComplexFilterBackend`` and overriding the ``operators``
attribute. ``operators`` is a map of operator symbols to functions that combine two querysets. For example, the map
can be overridden to use the ``QuerySet.intersection()`` and ``QuerySet.union()`` instead of ``&`` and ``|``.

.. code-block:: python

    class CustomizedBackend(ComplexFilterBackend):
        operators = {
            '&': QuerySet.intersection,
            '|': QuerySet.union,
            '-': QuerySet.difference,
        }

Unary ``negation`` relies on ORM internals and may be buggy in certain circumstances. If there are issues with this
feature, it can be disabled by setting the ``negation`` attribute to ``False`` on the backend class. If you do
experience bugs, please open an issue on the `bug tracker`_.

.. _`bug tracker`: https://github.com/philipn/django-rest-framework-filters/issues/


Complex querystring encoding
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Below is the procedure for encoding a complex query:

* Convert the query paramaters into individual querystrings.
* URL-encode the individual querystrings.
* Wrap the encoded strings in parentheses, and join with operators.
* URL-encode the entire querystring.
* Set as the value to the complex filter param (e.g., ``?filters=<complex querystring>``).

Note that ``filters`` is the default parameter name and can be overridden in the backend class.


Using the first example, these steps can be visualized as so:

* ``title__startswith=Who``, ``title__startswith=What``
* ``title__startswith%3DWho``, ``title__startswith%3DWhat``
* ``(title__startswith%3DWho) | (title__startswith%3DWhat)``
* ``%28title__startswith%253DWho%29%20%7C%20%28title__startswith%253DWhat%29``
* ``filters=%28title__startswith%253DWho%29%20%7C%20%28title__startswith%253DWhat%29``


Error handling
~~~~~~~~~~~~~~

``ComplexFilterBackend`` will raise any decoding errors under the complex filtering parameter name. For example,

.. code-block:: json

    {
        "filters": [
            "Invalid querystring operator. Matched: 'foo'."
        ]
    }

When filtering the querysets, filterset validation errors will be collected and raised under the complex filtering
parameter name, then under the filterset's decoded querystring. For a complex query like ``(a=1&b=2) | (c=3&d=4)``,
errors would be raised like so:

.. code-block:: json

    {
        "filters": {
            "a=1&b=2": {
                "a": ["..."]
            },
            "c=3&d=4": {
                "c": ["..."]
            }
        }
    {


Migrating to 1.0
----------------

Backend renamed, provides new templates
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The backend has been renamed from ``DjangoFilterBackend`` to ``RestFrameworkFilterBackend`` and now uses its own
template paths, located under ``rest_framework_filters`` instead of ``django_filters/rest_framework``.

To load the included templates, it is necessary to add ``rest_framework_filters`` to the ``INSTALLED_APPS`` setting.

``RelatedFilter.queryset`` now required
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The related filterset's model is no longer used to provide the default value for ``RelatedFilter.queryset``. This
change reduces the chance of unintentionally exposing data in the rendered filter forms. You must now explicitly
provide the ``queryset`` argument, or override the ``get_queryset()`` method (see `queryset callables`_).


``get_filters()`` renamed to ``get_request_filters()``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

django-filter has add a ``get_filters()`` classmethod to it's API, so this method has been renamed.


Publishing
----------

.. code-block:: bash

    $ pip install -U twine setuptools wheel
    $ rm -rf dist/ build/
    $ python setup.py sdist bdist_wheel
    $ twine upload dist/*


Copyright & License
-------------------

Copyright (c) 2013-2015 Philip Neustrom & 2016-2019 Ryan P Kilby. See `LICENSE`_ for details.

.. _`LICENSE`: https://github.com/philipn/django-rest-framework-filters/blob/master/LICENSE
