====================
django-widget-tweaks
====================

.. image:: https://jazzband.co/static/img/badge.svg
   :target: https://jazzband.co/
   :alt: Jazzband

.. image:: https://img.shields.io/pypi/v/django-widget-tweaks.svg
   :target: https://pypi.python.org/pypi/django-widget-tweaks
   :alt: PyPI Version

.. image:: https://github.com/jazzband/django-widget-tweaks/workflows/Test/badge.svg
   :target: https://github.com/jazzband/django-widget-tweaks/actions
   :alt: GitHub Actions

.. image:: https://codecov.io/gh/jazzband/django-widget-tweaks/branch/master/graph/badge.svg
   :target: https://app.codecov.io/gh/jazzband/django-widget-tweaks
   :alt: Coverage

Tweak the form field rendering in templates, not in python-level
form definitions. Altering CSS classes and HTML attributes is supported.

That should be enough for designers to customize field presentation (using
CSS and unobtrusive javascript) without touching python code.

License is MIT.

Installation
============

You can get Django Widget Tweaks by using pip::

    $ pip install django-widget-tweaks

To enable `widget_tweaks` in your project you need to add it to `INSTALLED_APPS` in your projects
`settings.py` file:

.. code-block:: python

    INSTALLED_APPS += [
        'widget_tweaks',
    ]

Usage
=====

This app provides two sets of tools that may be used together or standalone:

1. a ``render_field`` template tag for customizing form fields by using an
   HTML-like syntax.
2. several template filters for customizing form field HTML attributes and CSS
   classes

The ``render_field`` tag should be easier to use and should make form field
customizations much easier for designers and front-end developers.

The template filters are more powerful than the ``render_field`` tag, but they
use a more complex and less HTML-like syntax.

render_field
------------

This is a template tag that can be used as an alternative to aforementioned
filters.  This template tag renders a field using a syntax similar to plain
HTML attributes.

Example:

.. code-block:: html+django

    {% load widget_tweaks %}

    <!-- change input type (e.g. to HTML5) -->
    {% render_field form.search_query type="search" %}

    <!-- add/change several attributes -->
    {% render_field form.text rows="20" cols="20" title="Hello, world!" %}

    <!-- append to an attribute -->
    {% render_field form.title class+="css_class_1 css_class_2" %}

    <!-- template variables can be used as attribute values -->
    {% render_field form.text placeholder=form.text.label %}

    <!-- double colon -->
    {% render_field form.search_query v-bind::class="{active:isActive}" %}


For fields rendered with ``{% render_field %}`` tag it is possible
to set error class and required fields class by using
``WIDGET_ERROR_CLASS`` and  ``WIDGET_REQUIRED_CLASS`` template variables:

.. code-block:: html+django

    {% with WIDGET_ERROR_CLASS='my_error' WIDGET_REQUIRED_CLASS='my_required' %}
        {% render_field form.field1 %}
        {% render_field form.field2 %}
        {% render_field form.field3 %}
    {% endwith %}

You can be creative with these variables: e.g. a context processor could
set a default CSS error class on all fields rendered by
``{% render_field %}``.

attr
----
Adds or replaces any single html attribute for the form field.

Examples:

.. code-block:: html+django

    {% load widget_tweaks %}

    <!-- change input type (e.g. to HTML5) -->
    {{ form.search_query|attr:"type:search" }}

    <!-- add/change several attributes -->
    {{ form.text|attr:"rows:20"|attr:"cols:20"|attr:"title:Hello, world!" }}

    <!-- attributes without parameters -->
    {{ form.search_query|attr:"autofocus" }}


    <!-- attributes with double colon Vuejs output: v-bind:class="{active:ValueEnabled}" -->
    {{ form.search_query|attr:"v-bind::class:{active:ValueEnabled}" }}

add_class
---------

Adds CSS class to field element. Split classes by whitespace in order to add
several classes at once.

Example:

.. code-block:: html+django

    {% load widget_tweaks %}

    <!-- add 2 extra css classes to field element -->
    {{ form.title|add_class:"css_class_1 css_class_2" }}

set_data
--------

Sets HTML5 data attribute ( https://johnresig.com/blog/html-5-data-attributes/ ).
Useful for unobtrusive javascript. It is just a shortcut for 'attr' filter
that prepends attribute names with 'data-' string.

Example:

.. code-block:: html+django

    {% load widget_tweaks %}

    <!-- data-filters:"OverText" will be added to input field -->
    {{ form.title|set_data:"filters:OverText" }}

append_attr
-----------

Appends attribute value with extra data.

Example:

.. code-block:: html+django

    {% load widget_tweaks %}

    <!-- add 2 extra css classes to field element -->
    {{ form.title|append_attr:"class:css_class_1 css_class_2" }}

'add_class' filter is just a shortcut for 'append_attr' filter that
adds values to the 'class' attribute.

remove_attr
-----------
Removes any single html attribute for the form field.

Example:

.. code-block:: html+django

    {% load widget_tweaks %}

    <!-- removes autofocus attribute from field element -->
    {{ form.title|remove_attr:"autofocus" }}

add_label_class
---------------

The same as `add_class` but adds css class to form labels.

Example:

.. code-block:: html+django

    {% load widget_tweaks %}

    <!-- add 2 extra css classes to field label element -->
    {{ form.title|add_label_class:"label_class_1 label_class_2" }}

add_error_class
---------------

The same as 'add_class' but adds css class only if validation failed for
the field (field.errors is not empty).

Example:

.. code-block:: html+django

    {% load widget_tweaks %}

    <!-- add 'error-border' css class on field error -->
    {{ form.title|add_error_class:"error-border" }}

add_error_attr
--------------

The same as 'attr' but sets an attribute only if validation failed for
the field (field.errors is not empty). This can be useful when dealing
with accessibility:

.. code-block:: html+django

    {% load widget_tweaks %}

    <!-- add aria-invalid="true" attribute on field error -->
    {{ form.title|add_error_attr:"aria-invalid:true" }}

add_required_class
------------------

The same as 'add_error_class' adds css class only for required field.

Example:

.. code-block:: html+django

    {% load widget_tweaks %}

    <!-- add 'is-required' css class on field required -->
    {{ form.title|add_required_class:"is-required" }}

field_type and widget_type
--------------------------

``'field_type'`` and ``'widget_type'`` are template filters that return
field class name and field widget class name (in lower case).

Example:

.. code-block:: html+django

    {% load widget_tweaks %}

    <div class="field {{ field|field_type }} {{ field|widget_type }} {{ field.html_name }}">
        {{ field }}
    </div>

Output:

.. code-block:: html+django

    <div class="field charfield textinput name">
        <input id="id_name" type="text" name="name" maxlength="100" />
    </div>

Fields with multiple widgets
============================

Some fields may render as a `MultiWidget`, composed of multiple subwidgets
(for example, a `ChoiceField` using `RadioSelect`). You can use the same tags
and filters, but your template code will need to include a for loop for fields
like this:

.. code-block:: html+django

    {% load widget_tweaks %}

    {% for widget in form.choice %}
        {{ widget|add_class:"css_class_1 css_class_2" }}
    {% endfor %}

Mixing render_field and filters
===============================

The render_field tag and filters mentioned above can be mixed.

Example:

.. code-block:: html+django

    {% render_field form.category|append_attr:"readonly:readonly" type="text" placeholder="Category" %}


returns:

.. code-block:: html+django

    <input name="category" placeholder="Profession" readonly="readonly" type="text">

Filter chaining
===============

The order django-widget-tweaks filters apply may seem counter-intuitive
(leftmost filter wins):

.. code-block:: html+django

    {{ form.simple|attr:"foo:bar"|attr:"foo:baz" }}

returns:

.. code-block:: html+django

    <input foo="bar" type="text" name="simple" id="id_simple" />

It is not a bug, it is a feature that enables creating reusable templates
with overridable defaults.

Reusable field template example:

.. code-block:: html+django

    {# inc/field.html #}
    {% load widget_tweaks %}
    <div>{{ field|attr:"foo:default_foo" }}</div>

Example usage:

.. code-block:: html+django

    {# my_template.html #}
    {% load widget_tweaks %}
    <form method='POST' action=''> {% csrf_token %}
        {% include "inc/field.html" with field=form.title %}
        {% include "inc/field.html" with field=form.description|attr:"foo:non_default_foo" %}
    </form>

With 'rightmost filter wins' rule it wouldn't be possible to override
``|attr:"foo:default_foo"`` in main template.

Rendering form error messages
=============================

This app can render the following form error messages:

1. Field related errors
2. Non-field related errors
3. All form errors - Displays all field and Non-field related errors. If related to a specific field the name is displayed above the error, if the error is a general form error, displays __all__

Field related errors
--------------------

To render field related errors in your form:

Example:

.. code-block:: html+django

    {% load widget_tweaks %}
    {% for error in field.errors %}
    <span class="text-danger">{{ error }}</span>
    {% endfor %}

Example usage:

.. code-block:: html+django

    {% for field in form.visible_fields %}
    {{ field }}
    <label for="{{ field.id_for_label }}">{{ field.label }}</label>
    {% for error in field.errors %}
    <span class="text-danger">{{ error }}</span>
    {% endfor %}
    {% endfor %}

Non-field related errors
------------------------

Render general form errors:

Example:

.. code-block:: html+django

    {% load widget_tweaks %}
    {% if form.non_field_errors %}
    <span class="text-danger"> {{ form.non_field_errors  }}</span>
    {% endif %}

Example usage:

.. code-block:: html+django

    {% for field in form.visible_fields %}
    {{ field }}
    <label for="{{ field.id_for_label }}">{{ field.label }}</label>
    {% for error in field.errors %}
    <span class="text-danger">{{ error }}</span>
    {% endfor %}
    {% endfor %}

All form errors
---------------

Render all form errors:

Example:

.. code-block:: html+django

    {% load widget_tweaks %}
    {{ form.errors }}

Contributing
============

If you've found a bug, implemented a feature or have a suggestion,
do not hesitate to contact me, fire an issue or send a pull request.

* Source code: https://github.com/jazzband/django-widget-tweaks/
* Bug tracker: https://github.com/jazzband/django-widget-tweaks/issues

Testing
-------

Make sure you have `tox <https://tox.wiki/>`_ installed, then type

::

    tox

from the source checkout.
