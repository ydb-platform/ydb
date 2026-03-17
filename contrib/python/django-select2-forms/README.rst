django-select2-forms
####################

.. image:: https://travis-ci.org/theatlantic/django-select2-forms.svg?branch=master
    :target: https://travis-ci.org/theatlantic/django-select2-forms

**django-select2-forms** is a project that makes available Django form
fields that use the `Select2 javascript
plugin <http://ivaynberg.github.com/select2/>`_. It was created by
developers at `The Atlantic <http://www.theatlantic.com/>`_.

.. contents:: Table of Contents:

Support
=======

django-select2-forms should work with all currently supported releases as well
as the two most recent unsupported LTS versions.

Local Development & Testing
===========================

The following steps should only need to be done once when you first begin:

Install ``pyenv``
-----------------

These instructions assume that you have `Homebrew <https://brew.sh/>`_ installed,
but not ``pyenv``.

.. code:: bash

    brew install pyenv
    touch ~/.bash_profile

Add the following line to your ``~/bash_profile`` or ``.zshrc``::

    eval "$(pyenv init -)"

Reload your shell:

.. code:: bash

    . ~/.bash_profile

or

.. code:: bash

    . ~/.zshrc

Python Repository Setup
-----------------------

First, clone the repository and prep your Python environment:

.. code:: bash

    git clone https://github.com/theatlantic/django-select2-forms.git
    pyenv install 3.7.2
    pyenv install 3.8.0
    pyenv install 3.9.0
    pyenv local 3.7.2 3.8.0 3.9.0
    python -V

The output of the previous command should be ``Python 3.7.2``.

Finally:

.. code:: bash

    python -m venv venv

Activate Your Environment
-------------------------

From the base directory:

.. code:: bash

    deactivate  # ignore: -bash: deactivate: command not found
    . venv/bin/activate
    pip install -U tox

Running tests
-------------

If you have not already done so, set up your environment by chromedriver:

.. code:: bash

    brew install --cask chromedriver

Run all tests:

.. code:: bash

    tox -- --selenosis-driver=chrome-headless

Show all available ``tox`` commands:

.. code:: bash

    tox -av

Run only a specific environment:

.. code:: bash

    tox -e <environment-name> -- --selenosis-driver=chrome-headless  # example: tox -e py37-django22

Only run a specific test:

.. code:: bash

    tox -- pytest -k test_something --selenosis-driver=chrome-headless

Run an arbitrary command in a specific environment:

.. code:: bash

    tox -e py37-django22 -- python  # runs the Python REPL in that environment

Setup a development environment:

.. code:: bash

    tox -e <pyXX-DjangoYY> --develop -r
    . .tox/<pyXX-DjangoYY>/bin/activate

Installation
============

The recommended way to install is with pip::

    pip install django-select2-forms

or, to install with pip from source::

        pip install -e git+git://github.com/theatlantic/django-select2-forms.git#egg=django-select2-forms

If the source is already checked out, use setuptools::

        python setup.py develop

Configuration
=============

``django-select2-forms`` serves static assets using
`django.contrib.staticfiles <https://docs.djangoproject.com/en/1.8/howto/static-files/>`_,
and so requires that ``"select2"`` be added to your settings'
``INSTALLED_APPS``:

.. code-block:: python

    INSTALLED_APPS = (
        # ...
        'select2',
    )

To use django-select2-forms' ajax support, ``'select2.urls'`` must be
included in your urls.py ``urlpatterns``:

.. code-block:: python

    urlpatterns = patterns('',
        # ...
        url(r'^select2/', include('select2.urls')),
    )

Usage
=====

The simplest way to use ``django-select2-forms`` is to use
``select2.fields.ForeignKey`` and ``select2.fields.ManyToManyField`` in
place of ``django.db.models.ForeignKey`` and
``django.db.models.ManyToManyField``, respectively. These fields extend
their django equivalents and take the same arguments, along with extra
optional keyword arguments.

select2.fields.ForeignKey examples
----------------------------------

In the following two examples, an "entry" is associated with only one
author. The example below does not use ajax, but instead performs
autocomplete filtering on the client-side using the ``<option>``
elements (the labels of which are drawn from ``Author.__str__()``)
in an html ``<select>``.

.. code-block:: python

    @python_2_unicode_compatible
    class Author(models.Model):
        name = models.CharField(max_length=100)

        def __str__(self):
            return self.name

    class Entry(models.Model):
        author = select2.fields.ForeignKey(Author,
            overlay="Choose an author...",
            on_delete=models.CASCADE)

This more advanced example autocompletes via ajax using the
``Author.name`` field and limits the autocomplete search to
``Author.objects.filter(active=True)``

.. code-block:: python

    class Author(models.Model):
        name = models.CharField(max_length=100)
        active = models.BooleanField()

    class Entry(models.Model):
        author = select2.fields.ForeignKey(Author,
            limit_choices_to=models.Q(active=True),
            ajax=True,
            search_field='name',
            overlay="Choose an author...",
            js_options={
                'quiet_millis': 200,
            },
            on_delete=models.CASCADE)

select2.fields.ManyToManyField examples
---------------------------------------

In the following basic example, entries can have more than one author.
This example does not do author name lookup via ajax, but populates
``<option>`` elements in a ``<select>`` with ``Author.__unicode__()``
for labels.

.. code-block:: python

    @python_2_unicode_compatible
    class Author(models.Model):
        name = models.CharField(max_length=100)

        def __str__(self):
            return self.name

    class Entry(models.Model):
        authors = select2.fields.ManyToManyField(Author)

The following "kitchen sink" example allows authors to be ordered, and
uses ajax to autocomplete on two variants of an author's name.

.. code-block:: python

    from django.db import models
    from django.db.models import Q
    import select2.fields
    import select2.models

    class Author(models.Model):
        name = models.CharField(max_length=100)
        alt_name = models.CharField(max_length=100, blank=True, null=True)

    class Entry(models.Model):
        categories = select2.fields.ManyToManyField(Author,
            through='EntryAuthors',
            ajax=True,
            search_field=lambda q: Q(name__icontains=q) | Q(alt_name__icontains=q),
            sort_field='position',
            js_options={'quiet_millis': 200})

form field example
------------------

If you don't need to use the ajax features of ``django-select2-forms``
it is possible to use select2 on django forms without modifying your
models. The select2 formfields exist in the ``select2.fields`` module
and have the same class names as their standard django counterparts
(``ChoiceField``, ``MultipleChoiceField``, ``ModelChoiceField``,
``ModelMultipleChoiceField``). Here is the first ``ForeignKey`` example
above, done with django formfields.

.. code-block:: python

    class AuthorManager(models.Manager):
        def as_choices(self):
            for author in self.all():
                yield (author.pk, force_text(author))

    @python_2_unicode_compatible
    class Author(models.Model):
        name = models.CharField(max_length=100)
        objects = AuthorManager()

        def __str__(self):
            return self.name

    class Entry(models.Model):
        author = models.ForeignKey(Author, on_delete=models.CASCADE)

    class EntryForm(forms.ModelForm):
        author = select2.fields.ChoiceField(
            choices=Author.objects.as_choices(),
            overlay="Choose an author...")

        class Meta:
            model = Entry

License
=======

The django code is licensed under the `Simplified BSD
License <http://opensource.org/licenses/BSD-2-Clause>`_ and is
copyright The Atlantic Media Company. View the ``LICENSE`` file under
the root directory for complete license and copyright information.

The Select2 javascript library included is licensed under the `Apache
Software Foundation License Version
2.0 <http://www.apache.org/licenses/LICENSE-2.0>`_. View the file
``select2/static/select2/select2/LICENSE`` for complete license and
copyright information about the Select2 javascript library.
