Django Render Block
###################

.. image:: https://img.shields.io/pypi/v/django-render-block.svg
    :target: https://pypi.org/project/django-render-block/

.. image:: https://github.com/clokep/django-render-block/actions/workflows/main.yml/badge.svg
    :target: https://github.com/clokep/django-render-block/actions/workflows/main.yml

Render the content of a specific block tag from a Django template. Works for
arbitrary template inheritance, even if a block is defined in the child template
but not in the parent. Generally it works like ``render_to_string`` from Django,
but allows you to specify a block to render.

Features
========

*   Render a specific block from a template
*   Fully supports the Django templating engine
*   Partially supports the `Jinja2 <http://jinja.pocoo.org/>`__ engine: it does
    not currently process the ``extends`` tag.

Requirements
============

Django Render Block supports Django 4.2, 5.1, and 5.2 on Python 3.9, 3.10, 3.11,
3.12, and 3.13 (see the `Django documentation <https://docs.djangoproject.com/en/dev/faq/install/#what-python-version-can-i-use-with-django>`_
for which versions of Python are supported by particular Django versions).

Examples
========

In ``test1.html``:

.. code-block:: jinja

    {% block block1 %}block1 from test1{% endblock %}
    {% block block2 %}block2 from test1{% endblock %}

In ``test2.html``:

.. code-block:: jinja

    {% extends 'test1.html' %}
    {% block block1 %}block1 from test2{% endblock %}

And from the Python shell:

.. code-block:: python

    >>> from render_block import render_block_to_string
    >>> print(render_block_to_string('test2.html', 'block1'))
    'block1 from test2'
    >>> print(render_block_to_string('test2.html', 'block2'))
    'block2 from test1'

It can also accept a context as a ``dict`` (just like ``render_to_string``), in
``test3.html``:

.. code-block:: jinja

    {% block block3 %}Render this {{ variable }}!{% endblock %}

And from Python:

.. code-block:: python

    >>> print(render_block_to_string('test3.html', 'block3', {'variable': 'test'}))
    'Render this test!'

API Reference
=============

The API is simple and attempts to mirror the built-in ``render_to_string`` and ``render`` API.

``render_block_to_string(template_name, block_name, context=None, request=None)``

    ``template_name``
        The name of the template to load and render. If it’s a list of template
        names, Django uses ``select_template()`` instead of ``get_template()``
        to find the template.

    ``block_name``
        The name of the block to render from the above template.

    ``context``
        A ``dict`` to be used as the template’s context for rendering. A ``Context``
        object can be provided for Django templates.

        ``context`` is optional. If not provided, an empty context will be used.

    ``request``
        The request object used to render the template.

        ``request`` is optional and works only for Django templates. If both context and request
        are provided, a ``RequestContext`` will be used instead of a ``Context``.

Similarly there is a ``render_block`` function which returns an `HttpResponse` with
the content sent to the result of ``render_block_to_string`` with the same parameters.

``render_block(request, template_name, block_name, context=None, content_type="text/html", status=200)``

    ``request``
        The request object used to render the template.

    ``template_name``
        The name of the template to load and render. If it’s a list of template
        names, Django uses ``select_template()`` instead of ``get_template()``
        to find the template.

    ``block_name``
        The name of the block to render from the above template.

    ``context``
        A ``dict`` to be used as the template’s context for rendering. A ``Context``
        object can be provided for Django templates.

        ``context`` is optional. If not provided, an empty context will be used.

    ``content_type``
        A ``str`` content type for the HTTP response.

    ``status``
        An ``int`` HTTP status code for the HTTP response.

Exceptions
----------

Like ``render_to_string`` this will raise the following exceptions:

    ``TemplateDoesNotExists``
        Raised if the template(s) specified by ``template_name`` cannot be
        loaded.

    ``TemplateSyntaxError``
        Raised if the loaded template contains invalid syntax.

There are also two additional errors that can be raised:

    ``BlockNotFound``
        Raised if the block given by ``block_name`` does not exist in the
        template.

    ``UnsupportedEngine``
        Raised if a template backend besides the Django backend is used.

Contributing
============

If you find a bug or have an idea for an improvement to Django Render Block,
please
`file an issue <https://github.com/clokep/django-render-block/issues/new>`_ or
provide a pull request! Check the
`list of issues <https://github.com/clokep/django-render-block/issues/>`_ for
ideas of what to work on.

Attribution
===========

This is based on a few sources:

* Originally `Django Snippet 769 <https://djangosnippets.org/snippets/769/>`__
* Updated version `Django Snippet 942 <https://djangosnippets.org/snippets/942/>`__
* A version of the snippets was ported as `Django-Block-Render <https://github.com/uniphil/Django-Block-Render/>`_
* Additionally inspired by part of `django-templated-email <https://github.com/BradWhittington/django-templated-email/blob/master/templated_email/utils.py>`_
* Also based on a `StackOverflow answer 2687173 <http://stackoverflow.com/questions/2687173/django-how-can-i-get-a-block-from-a-template>`_
