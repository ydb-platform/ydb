# -*- coding: utf-8 -*-
from distutils.version import LooseVersion
from marshmallow.utils import missing

# Make marshmallow's validation functions importable from webargs
from marshmallow import validate

from webargs.core import ValidationError
from webargs.dict2schema import dict2schema
from webargs import fields

__version__ = "5.5.3"
__version_info__ = tuple(LooseVersion(__version__).version)
__all__ = ("dict2schema", "ValidationError", "fields", "missing", "validate")
