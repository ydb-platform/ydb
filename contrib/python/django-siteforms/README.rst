django-siteforms
================
https://github.com/idlesign/django-siteforms

|release| |lic| |coverage|

.. |release| image:: https://img.shields.io/pypi/v/django-siteforms.svg
    :target: https://pypi.python.org/pypi/django-siteforms

.. |lic| image:: https://img.shields.io/pypi/l/django-siteforms.svg
    :target: https://pypi.python.org/pypi/django-siteforms

.. |coverage| image:: https://img.shields.io/coveralls/idlesign/django-siteforms/master.svg
    :target: https://coveralls.io/r/idlesign/django-siteforms


Description
-----------

*Django reusable app to simplify form construction*

For those who consider maintaining templates-based forms solutions for Django a burden.

Features:

* Full form rendering support, including prolog and submit button
* Subforms support (represent entire other form as a form field): JSON, Foreign Key, Many-to-Many
* Field groups
* Declarative attributes for elements
* Simplified declarative forms layout, allowing fields ordering
* Simple ways to make fields hidden, disabled, readonly
* Support for fields from model's properties
* Aria-friendly (Accessible Rich Internet Applications)
* Complex widgets (e.g. using values from multiple fields) support
* Filter-forms (use form for queryset filtering)

Supported styling:

* No CSS
* Bootstrap 4
* Bootstrap 5


Usage
-----

To render a form in templates just address a variable, e.g. ``<div>{{ form }}</div>``.

.. note:: By default there's no need to add a submit button and wrap it all into ``<form>``.

Basic
~~~~~

Let's show how to build a simple form.

.. code-block:: python

    from django.shortcuts import render
    from siteforms.composers.bootstrap5 import Bootstrap5
    from siteforms.toolbox import ModelForm


    class MyForm(ModelForm):
        """This form will show us how siteforms works."""
        
        disabled_fields = {'somefield'}  # Declarative way of disabling fields.
        hidden_fields = {'otherfield'}  # Declarative way of hiding fields.
        readonly_fields = {'anotherfield'}  # Declarative way of making fields readonly.

        class Composer(Bootstrap5):
            """This will instruct siteforms to compose this
            form using Bootstrap 5 styling.

            """
        class Meta:
            model = MyModel  # Suppose you have a model class already.
            fields = '__all__'

    def my_view(request):
        # Initialize form using data from POST.
        my_form = MyForm(request=request, src='POST')
        is_valid = form.is_valid()
        return render(request, 'mytemplate.html', {'form': my_form})


Composer options
~~~~~~~~~~~~~~~~

Now let's see how to tune our form.

.. code-block:: python

    from siteforms.composers.bootstrap5 import Bootstrap5, FORM, ALL_FIELDS

    class Composer(Bootstrap5):

        opt_size='sm'  # Bootstrap 5 has sizes, so let's make our form small.

        # Element (fields, groups, form, etc.) attributes are ruled by `attrs`.
        # Let's add rows=2 to our `contents` model field.
        attrs={'contents': {'rows': 2}}

        # To group fields into named groups describe them in `groups`.
        groups={
            'basic': 'Basic attributes',
            'other': 'Other fields',
        }

        # We apply custom layout to our form.
        layout = {
            FORM: {
                'basic': [  # First we place `basic` group.
                    # The following three fields are in the same row -
                    # two fields in the right column are stacked.
                    ['title', ['date_created',
                               'date_updated']],
                    'contents',  # This one field goes into a separate row.
                ],
                # We place all the rest fields into `other` group.
                'other': ALL_FIELDS,
            }
        }


Documentation
-------------

https://django-siteforms.readthedocs.org/
