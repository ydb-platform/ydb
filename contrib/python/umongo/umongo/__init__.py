from marshmallow import ValidationError, missing  # noqa, republishing

from .instance import Instance

from .document import (
    Document,
    pre_load,
    post_load,
    pre_dump,
    post_dump,
    validates_schema
)
from .exceptions import (
    UMongoError,
    UpdateError,
    DeleteError,
    AlreadyCreatedError,
    NotCreatedError,
    NoneReferenceError,
    UnknownFieldInDBError,
)
from . import fields, validate
from .data_objects import Reference
from .embedded_document import EmbeddedDocument
from .mixin import MixinDocument
from .expose_missing import ExposeMissing, RemoveMissingSchema
from .i18n import set_gettext


__author__ = 'Emmanuel Leblond, Jérôme Lafréchoux'
__email__ = 'jerome@jolimont.fr'
__version__ = '3.1.0'
__all__ = (
    'missing',

    'Instance',

    'Document',
    'pre_load',
    'post_load',
    'pre_dump',
    'post_dump',
    'validates_schema',
    'EmbeddedDocument',
    'MixinDocument',
    'ExposeMissing',
    'RemoveMissingSchema',

    'UMongoError',
    'ValidationError',
    'UpdateError',
    'DeleteError',
    'AlreadyCreatedError',
    'NotCreatedError',
    'NoneReferenceError',
    'UnknownFieldInDBError',

    'fields',

    'Reference',

    'set_gettext',

    'validate'
)
