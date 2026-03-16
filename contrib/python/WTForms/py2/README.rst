WTForms
=======

WTForms is a flexible forms validation and rendering library for Python
web development. It can work with whatever web framework and template
engine you choose. It supports data validation, CSRF protection,
internationalization (I18N), and more. There are various community
libraries that provide closer integration with popular frameworks.


Installation
------------

Install and update using pip:

.. code-block:: text

    pip install -U WTForms


Third-Party Library Integrations
--------------------------------

WTForms is designed to work with any web framework and template engine.
There are a number of community-provided libraries that make integrating
with frameworks even better.

-   `Flask-WTF`_ integrates with the Flask framework. It can
    automatically load data from the request, uses Flask-Babel to
    translate based on user-selected locale, provides full-application
    CSRF, and more.
-   `WTForms-Alchemy`_ provides rich support for generating forms from
    SQLAlchemy models, including an expanded set of fields and
    validators.
-   `WTForms-SQLAlchemy`_ provides ORM-backed fields and form generation
    from SQLAlchemy models.
-   `WTForms-AppEngine`_ provides ORM-backed fields and form generation
    from AppEnding db/ndb schema
-   `WTForms-AppEngine`_ provides ORM-backed fields and form generation
    from Django models, as well as integration with Django's I18N
    support.

.. _Flask-WTF: https://flask-wtf.readthedocs.io/
.. _WTForms-Alchemy: https://wtforms-alchemy.readthedocs.io/
.. _WTForms-SQLAlchemy: https://github.com/wtforms/wtforms-sqlalchemy
.. _WTForms-AppEngine: https://github.com/wtforms/wtforms-appengine
.. _WTForms-Django: https://github.com/wtforms/wtforms-django


Links
-----

-   Documentation: https://wtforms.readthedocs.io/
-   Releases: https://pypi.org/project/WTForms/
-   Code: https://github.com/wtforms/wtforms
-   Issue tracker: https://github.com/wtforms/wtforms/issues
-   Test status: https://travis-ci.org/wtforms/wtforms
-   Test coverage: https://coveralls.io/github/wtforms/wtforms
-   Discord Chat: https://discord.gg/F65P7Z9
