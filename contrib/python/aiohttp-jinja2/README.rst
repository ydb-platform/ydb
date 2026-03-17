aiohttp_jinja2
==============
.. image:: https://github.com/aio-libs/aiohttp-jinja2/workflows/CI/badge.svg
    :target: https://github.com/aio-libs/aiohttp-jinja2/actions?query=workflow%3ACI
.. image:: https://codecov.io/gh/aio-libs/aiohttp-jinja2/branch/master/graph/badge.svg
    :target: https://codecov.io/gh/aio-libs/aiohttp-jinja2
.. image:: https://img.shields.io/pypi/v/aiohttp-jinja2.svg
    :target: https://pypi.python.org/pypi/aiohttp-jinja2
.. image:: https://readthedocs.org/projects/aiohttp-jinja2/badge/?version=latest
    :target: http://aiohttp-jinja2.readthedocs.io/en/latest/?badge=latest


jinja2_ template renderer for `aiohttp.web`__.


.. _jinja2: http://jinja.pocoo.org

.. _aiohttp_web: https://aiohttp.readthedocs.io/en/latest/web.html

__ aiohttp_web_

Installation
------------
Install from PyPI::

    pip install aiohttp-jinja2


Developing
----------

Install requirement and launch tests::

    pip install -r requirements-dev.txt
    pytest tests


Usage
-----

Before template rendering you have to setup *jinja2 environment* first:

.. code-block:: python

    app = web.Application()
    aiohttp_jinja2.setup(app,
        loader=jinja2.FileSystemLoader('/path/to/templates/folder'))

Import:

.. code-block:: python

    import aiohttp_jinja2
    import jinja2

After that you may to use template engine in your *web-handlers*. The
most convenient way is to decorate a *web-handler*.

Using the function based web handlers:

.. code-block:: python

    @aiohttp_jinja2.template('tmpl.jinja2')
    def handler(request):
        return {'name': 'Andrew', 'surname': 'Svetlov'}

Or for `Class Based Views
<https://aiohttp.readthedocs.io/en/stable/web_quickstart.html#class-based-views>`:

.. code-block:: python

    class Handler(web.View):
        @aiohttp_jinja2.template('tmpl.jinja2')
        async def get(self):
            return {'name': 'Andrew', 'surname': 'Svetlov'}


On handler call the ``aiohttp_jinja2.template`` decorator will pass
returned dictionary ``{'name': 'Andrew', 'surname': 'Svetlov'}`` into
template named ``tmpl.jinja2`` for getting resulting HTML text.

If you need more complex processing (set response headers for example)
you may call ``render_template`` function.

Using a function based web handler:

.. code-block:: python

    async def handler(request):
        context = {'name': 'Andrew', 'surname': 'Svetlov'}
        response = aiohttp_jinja2.render_template('tmpl.jinja2',
                                                  request,
                                                  context)
        response.headers['Content-Language'] = 'ru'
        return response

Or, again, a class based view:

.. code-block:: python

    class Handler(web.View):
        async def get(self):
            context = {'name': 'Andrew', 'surname': 'Svetlov'}
            response = aiohttp_jinja2.render_template('tmpl.jinja2',
                                                      self.request,
                                                      context)
            response.headers['Content-Language'] = 'ru'
            return response


License
-------

``aiohttp_jinja2`` is offered under the Apache 2 license.
