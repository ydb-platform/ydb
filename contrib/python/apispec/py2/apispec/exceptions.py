# -*- coding: utf-8 -*-
"""Exception classes."""
import warnings

class APISpecError(Exception):
    """Base class for all apispec-related errors."""

class PluginError(APISpecError):
    """Raised when a plugin cannot be found or is invalid."""

class PluginMethodNotImplementedError(APISpecError, NotImplementedError):
    """Raised when calling an unimplemented helper method in a plugin"""

class OpenAPIError(APISpecError):
    """Raised when a OpenAPI spec validation fails."""

class SwaggerError(OpenAPIError):
    """
    .. deprecated:: 0.38.0
        Use `apispec.exceptions.OpenAPIError` instead.
    """
    def __init__(self, *args, **kwargs):
        warnings.warn(
            'SwaggerError is deprecated. Use OpenAPIError instead.',
            DeprecationWarning,
        )
        super(SwaggerError, self).__init__(*args, **kwargs)
