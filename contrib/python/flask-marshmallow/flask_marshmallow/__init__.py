"""
flask_marshmallow
~~~~~~~~~~~~~~~~~

Integrates the marshmallow serialization/deserialization library
with your Flask application.
"""

import typing
import warnings

from marshmallow import exceptions

try:
    # Available in marshmallow 3 only
    from marshmallow import pprint  # noqa: F401
except ImportError:
    _has_pprint = False
else:
    _has_pprint = True
from marshmallow import fields as base_fields

from . import fields
from .schema import Schema

if typing.TYPE_CHECKING:
    from flask import Flask

has_sqla = False
try:
    import flask_sqlalchemy  # noqa: F401
except ImportError:
    has_sqla = False
else:
    try:
        from . import sqla
    except ImportError:
        warnings.warn(
            "Flask-SQLAlchemy integration requires "
            "marshmallow-sqlalchemy to be installed.",
            stacklevel=2,
        )
    else:
        has_sqla = True

__all__ = [
    "EXTENSION_NAME",
    "Marshmallow",
    "Schema",
    "fields",
    "exceptions",
]
if _has_pprint:
    __all__.append("pprint")

EXTENSION_NAME = "flask-marshmallow"


def _attach_fields(obj):
    """Attach all the marshmallow fields classes to ``obj``, including
    Flask-Marshmallow's custom fields.
    """
    for attr in base_fields.__all__:
        if not hasattr(obj, attr):
            setattr(obj, attr, getattr(base_fields, attr))
    for attr in fields.__all__:
        setattr(obj, attr, getattr(fields, attr))


class Marshmallow:
    """Wrapper class that integrates Marshmallow with a Flask application.

    To use it, instantiate with an application::

        from flask import Flask

        app = Flask(__name__)
        ma = Marshmallow(app)

    The object provides access to the :class:`Schema` class,
    all fields in :mod:`marshmallow.fields`, as well as the Flask-specific
    fields in :mod:`flask_marshmallow.fields`.

    You can declare schema like so::

        class BookSchema(ma.Schema):
            id = ma.Integer(dump_only=True)
            title = ma.String(required=True)
            author = ma.Nested(AuthorSchema)

            links = ma.Hyperlinks(
                {
                    "self": ma.URLFor("book_detail", values=dict(id="<id>")),
                    "collection": ma.URLFor("book_list"),
                }
            )


    In order to integrate with Flask-SQLAlchemy, this extension must be initialized
    *after* `flask_sqlalchemy.SQLAlchemy`. ::

            db = SQLAlchemy(app)
            ma = Marshmallow(app)

    This gives you access to `ma.SQLAlchemySchema` and `ma.SQLAlchemyAutoSchema`, which
    generate marshmallow `~marshmallow.Schema` classes
    based on the passed in model or table. ::

        class AuthorSchema(ma.SQLAlchemyAutoSchema):
            class Meta:
                model = Author

    :param Flask app: The Flask application object.
    """

    def __init__(self, app: typing.Optional["Flask"] = None):
        self.Schema = Schema
        if has_sqla:
            self.SQLAlchemySchema = sqla.SQLAlchemySchema
            self.SQLAlchemyAutoSchema = sqla.SQLAlchemyAutoSchema
            self.auto_field = sqla.auto_field
            self.HyperlinkRelated = sqla.HyperlinkRelated
        _attach_fields(self)
        if app is not None:
            self.init_app(app)

    def init_app(self, app: "Flask"):
        """Initializes the application with the extension.

        :param Flask app: The Flask application object.
        """
        app.extensions = getattr(app, "extensions", {})

        # If using Flask-SQLAlchemy, attach db.session to SQLAlchemySchema
        if has_sqla and "sqlalchemy" in app.extensions:
            db = app.extensions["sqlalchemy"]
            SQLAlchemySchemaOpts = typing.cast(
                sqla.SQLAlchemySchemaOpts, self.SQLAlchemySchema.OPTIONS_CLASS
            )
            SQLAlchemySchemaOpts.session = db.session
            SQLAlchemyAutoSchemaOpts = typing.cast(
                sqla.SQLAlchemyAutoSchemaOpts, self.SQLAlchemySchema.OPTIONS_CLASS
            )
            SQLAlchemyAutoSchemaOpts.session = db.session
        app.extensions[EXTENSION_NAME] = self
