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
-   `WTForms-Django`_ provides ORM-backed fields and form generation
    from Django models, as well as integration with Django's I18N
    support.
-   `WTForms-Bootstrap5`_ provides Bootstrap 5 favor renderer with
    great customizability.
-   `Starlette-WTF`_ integrates with Starlette and the FastAPI
    framework, based on the features of Flask-WTF.
-   `Bootstrap-Flask`_ Bootstrap-Flask is a collection of Jinja macros
    for Bootstrap 4 & 5 and Flask using Flask-WTF.

.. _Flask-WTF: https://flask-wtf.readthedocs.io/
.. _WTForms-Alchemy: https://wtforms-alchemy.readthedocs.io/
.. _WTForms-SQLAlchemy: https://github.com/wtforms/wtforms-sqlalchemy
.. _WTForms-AppEngine: https://github.com/wtforms/wtforms-appengine
.. _WTForms-Django: https://github.com/wtforms/wtforms-django
.. _WTForms-Bootstrap5: https://github.com/LaunchPlatform/wtforms-bootstrap5
.. _Starlette-WTF: https://github.com/muicss/starlette-wtf
.. _Bootstrap-Flask: https://github.com/helloflask/bootstrap-flask


Links
-----

-   Documentation: https://wtforms.readthedocs.io/
-   Releases: https://pypi.org/project/WTForms/
-   Code: https://github.com/wtforms/wtforms
-   Issue tracker: https://github.com/wtforms/wtforms/issues
-   Discord Chat: https://discord.gg/F65P7Z9
-   Translation: https://hosted.weblate.org/projects/wtforms/wtforms/
