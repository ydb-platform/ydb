WTForms-SQLAlchemy
==================

.. image:: https://github.com/wtforms/wtforms-sqlalchemy/actions/workflows/tests.yaml/badge.svg
    :target: https://github.com/wtforms/wtforms-sqlalchemy/actions/workflows/tests.yaml
.. image:: https://readthedocs.org/projects/wtforms-sqlalchemy/badge/?version=latest&style=flat
    :target: https://wtforms-sqlalchemy.readthedocs.io

WTForms-SQLAlchemy is a fork of the ``wtforms.ext.sqlalchemy`` package from WTForms.
The package has been renamed to ``wtforms_sqlalchemy`` but otherwise should
function the same as ``wtforms.ext.sqlalchemy`` did.

to install::

    pip install WTForms-SQLAlchemy

An example using Flask is included in ``examples/flask``.

Features
--------

1. Provide ``SelectField`` integration with SQLAlchemy models

   - ``wtforms_sqlalchemy.fields.QuerySelectField``
   - ``wtforms_sqlalchemy.fields.QuerySelectMultipleField``

2. Generate forms from SQLAlchemy models using
   ``wtforms_sqlalchemy.orm.model_form``

Rationale
---------

The reasoning for splitting out this package is that WTForms 2.0 has
deprecated all its ``wtforms.ext.<library>`` packages and they will
not receive any further feature updates. The authors feel that packages
for companion libraries work better with their own release schedule and
the ability to diverge more from WTForms.
