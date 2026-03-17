pytest-flask
============

.. image:: https://img.shields.io/pypi/v/pytest-flask.svg
    :target: https://pypi.python.org/pypi/pytest-flask
    :alt: PyPi version

.. image:: https://img.shields.io/conda/vn/conda-forge/pytest-flask.svg
    :target: https://anaconda.org/conda-forge/pytest-flask
    :alt: conda-forge version

.. image:: https://github.com/pytest-dev/pytest-flask/workflows/build/badge.svg
    :target: https://github.com/pytest-dev/pytest-flask/actions
    :alt: CI status

.. image:: https://img.shields.io/pypi/pyversions/pytest-flask.svg
    :target: https://pypi.org/project/pytest-flask
    :alt: PyPi downloads

.. image:: https://readthedocs.org/projects/pytest-flask/badge/?version=latest
   :target: https://pytest-flask.readthedocs.org/en/latest/
   :alt: Documentation status

.. image:: https://img.shields.io/maintenance/yes/2022?color=blue
    :target: https://github.com/pytest-dev/pytest-flask
    :alt: Maintenance

.. image:: https://img.shields.io/github/last-commit/pytest-dev/pytest-flask?color=blue
    :target: https://github.com/pytest-dev/pytest-flask/commits/master
    :alt: GitHub last commit

.. image:: https://img.shields.io/github/issues-pr-closed-raw/pytest-dev/pytest-flask?color=blue
    :target: https://github.com/pytest-dev/pytest-flask/pulls?q=is%3Apr+is%3Aclosed
    :alt: GitHub closed pull requests

.. image:: https://img.shields.io/github/issues-closed/pytest-dev/pytest-flask?color=blue
    :target: https://github.com/pytest-dev/pytest-flask/issues?q=is%3Aissue+is%3Aclosed
    :alt: GitHub closed issues

.. image:: https://img.shields.io/pypi/dm/pytest-flask?color=blue
    :target: https://pypi.org/project/pytest-flask/
    :alt: PyPI - Downloads

.. image:: https://img.shields.io/github/languages/code-size/pytest-dev/pytest-flask?color=blue
    :target: https://github.com/pytest-dev/pytest-flask
    :alt: Code size

.. image:: https://img.shields.io/badge/license-MIT-blue.svg?color=blue
   :target: https://github.com/pytest-dev/pytest-flask/blob/master/LICENSE
   :alt: License

.. image:: https://img.shields.io/github/issues-raw/pytest-dev/pytest-flask.svg?color=blue
   :target: https://github.com/pytest-dev/pytest-flask/issues
   :alt: Issues

.. image:: https://img.shields.io/badge/code%20style-black-000000.svg
   :target: https://github.com/ambv/black
   :alt: style

An extension of `pytest`_ test runner which
provides a set of useful tools to simplify testing and development
of the Flask extensions and applications.

To view a more detailed list of extension features and examples go to
the `PyPI`_ overview page or
`package documentation`_.

How to start?
-------------

Considering the minimal flask `application factory`_ below in ``myapp.py`` as an example:

.. code-block:: python

   from flask import Flask

   def create_app():
      # create a minimal app
      app = Flask(__name__)

      # simple hello world view
      @app.route('/hello')
      def hello():
         return 'Hello, World!'

      return app

You first need to define your application fixture in ``conftest.py``:

.. code-block:: python

    from myapp import create_app

    @pytest.fixture
    def app():
        app = create_app()
        return app

Finally, install the extension with dependencies and run your test suite::

    $ pip install pytest-flask
    $ pytest

Contributing
------------

Don’t hesitate to create a `GitHub issue`_ for any bug or
suggestion. For more information check our contribution `guidelines`_.

.. _pytest: https://docs.pytest.org/en/stable/
.. _PyPI: https://pypi.python.org/pypi/pytest-flask
.. _Github issue: https://github.com/vitalk/pytest-flask/issues
.. _package documentation: http://pytest-flask.readthedocs.org/en/latest/
.. _guidelines: https://github.com/pytest-dev/pytest-flask/blob/master/CONTRIBUTING.rst
.. _application factory: https://flask.palletsprojects.com/patterns/appfactories/
