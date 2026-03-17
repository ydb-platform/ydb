"""
Prance implements parsers for Swagger/OpenAPI 2.0 and 3.0.0 API specs.

See https://openapis.org/ for details on the specification.

Included is a BaseParser that reads and validates swagger specs, and a
ResolvingParser that additionally resolves any $ref references.
"""

__author__ = "Jens Finkhaeuser"
__copyright__ = "Copyright (c) 2016-2021 Jens Finkhaeuser"
__license__ = "MIT"
__all__ = ("util", "mixins", "cli", "convert")
import sys

from packaging.version import Version

try:
    from prance._version import version as __version__
except ImportError:
    # todo: better gussing
    __version__ = "0.20.0+unknown"

from . import mixins


# Define our own error class
class ValidationError(Exception):
    pass


# Placeholder for when no URL is specified for the main spec file

if sys.platform == "win32":  # pragma: nocover
    # Placeholder must be absolute
    _PLACEHOLDER_URL = "file:///c:/__placeholder_url__.yaml"
else:
    _PLACEHOLDER_URL = "file:///__placeholder_url__.yaml"


class BaseParser(mixins.YAMLMixin, mixins.JSONMixin):
    """
    The BaseParser loads, parses and validates OpenAPI 2.0 and 3.0.0 specs.

    Uses :py:class:`YAMLMixin` and :py:class:`JSONMixin` for additional
    functionality.
    """

    BACKENDS = {
        "flex": ((2,), "_validate_flex"),
        "swagger-spec-validator": ((2,), "_validate_swagger_spec_validator"),
        "openapi-spec-validator": ((2, 3), "_validate_openapi_spec_validator"),
    }

    SPEC_VERSION_2_PREFIX = "Swagger/OpenAPI"
    SPEC_VERSION_3_PREFIX = "OpenAPI"

    def __init__(self, url=None, spec_string=None, lazy=False, **kwargs):
        """
        Load, parse and validate specs.

        You can either provide a URL or a spec string, but not both.

        :param str url: The URL of the file to load. URLs missing a scheme are
          assumed to be file URLs.
        :param str spec_string: The specifications to parse.
        :param bool lazy: If true, do not load or parse anything. Instead wait for
          the parse function to be invoked.
        :param str backend: [optional] one of 'flex', 'swagger-spec-validator' or
          'openapi-spec-validator'.
          Determines the validation backend to use. Defaults to the first installed
          backend in the ordered list obtained from util.validation_backends().
        :param bool strict: [optional] Applies only to the 'swagger-spec-validator'
          backend. If False, accepts non-String keys by stringifying them before
          validation. Defaults to True.
        :param str encoding: [optional] For local URLs, use the given file encoding
          instead of auto-detecting. Defaults to None.
        """
        assert url or spec_string and not (url and spec_string), (
            "You must provide either a URL to read, or a spec string to "
            "parse, but not both!"
        )

        # Keep the parameters around for later use
        self.url = None
        if url:
            from .util.url import absurl
            from .util.fs import abspath
            import os

            self.url = absurl(url, abspath(os.getcwd()))
        else:
            self.url = _PLACEHOLDER_URL

        self._spec_string = spec_string

        # Initialize variables we're filling later
        self.specification = None
        self.version = None
        self.version_name = None
        self.version_parsed = ()
        self.valid = False

        # Add kw args as options
        self.options = kwargs

        # Verify backend
        from .util import default_validation_backend

        self.backend = self.options.get("backend", default_validation_backend())
        if self.backend not in BaseParser.BACKENDS.keys():
            raise ValueError(
                f"Backend may only be one of {BaseParser.BACKENDS.keys()}!"
            )

        # Start parsing if lazy mode is not requested.
        if not lazy:
            self.parse()

    def parse(self):  # noqa: F811
        """
        When the BaseParser was lazily created, load and parse now.

        You can use this function to re-use an existing parser for parsing
        multiple files by setting its url property and then invoking this
        function.
        """
        strict = self.options.get("strict", True)

        # If we have a file name, we need to read that in.
        if self.url and self.url != _PLACEHOLDER_URL:
            from .util.url import fetch_url

            encoding = self.options.get("encoding", None)
            self.specification = fetch_url(self.url, encoding=encoding, strict=strict)

        # If we have a spec string, try to parse it.
        if self._spec_string:
            from .util.formats import parse_spec

            self.specification = parse_spec(self._spec_string, self.url)

        # If we have a parsed spec, convert it to JSON. Then we can validate
        # the JSON. At this point, we *require* a parsed specification to exist,
        # so we might as well assert.
        assert self.specification, "No specification parsed, cannot validate!"

        self._validate()

    def _validate(self):
        # Ensure specification is a mapping
        from collections.abc import Mapping

        if not isinstance(self.specification, Mapping):
            raise ValidationError("Could not parse specifications!")

        # Ensure the selected backend supports the given spec version
        versions, validator_name = BaseParser.BACKENDS[self.backend]

        # Fetch the spec version. Note that this is the spec version the spec
        # *claims* to be; we later set the one we actually could validate as.
        spec_version = None
        if spec_version is None:
            spec_version = self.specification.get("openapi", None)
        if spec_version is None:
            spec_version = self.specification.get("swagger", None)
        if spec_version is None:
            raise ValidationError(
                "Could not determine specification schema " "version!"
            )

        # Try parsing the spec version, examine the first component.
        import packaging.version

        parsed = packaging.version.parse(spec_version)
        if parsed.major not in versions:
            raise ValidationError(
                'Version mismatch: selected backend "%s"'
                " does not support specified version %s!" % (self.backend, spec_version)
            )

        # Validate the parsed specs, using the given validation backend.
        validator = getattr(self, validator_name)

        # Set valid flag according to whether validator succeeds
        self.valid = False
        validator(parsed)
        self.valid = True

    def __set_version(self, prefix, version: Version):
        self.version_name = prefix
        self.version_parsed = version.release

        stringified = str(version)
        if prefix == BaseParser.SPEC_VERSION_2_PREFIX:
            stringified = "%d.%d" % (version.major, version.minor)
        self.version = f"{self.version_name} {stringified}"

    def _validate_flex(self, spec_version: Version):  # pragma: nocover
        # Set the version independently of whether validation succeeds
        self.__set_version(BaseParser.SPEC_VERSION_2_PREFIX, spec_version)

        from flex.exceptions import ValidationError as JSEValidationError
        from flex.core import parse as validate

        try:
            validate(self.specification)
        except JSEValidationError as ex:
            from .util.exceptions import raise_from

            raise_from(ValidationError, ex)

    def _validate_swagger_spec_validator(
        self, spec_version: Version
    ):  # pragma: nocover
        # Set the version independently of whether validation succeeds
        self.__set_version(BaseParser.SPEC_VERSION_2_PREFIX, spec_version)

        from swagger_spec_validator.common import SwaggerValidationError as SSVErr
        from swagger_spec_validator.validator20 import validate_spec

        try:
            validate_spec(self.specification)
        except SSVErr as ex:
            from .util.exceptions import raise_from

            raise_from(ValidationError, ex)

    def _validate_openapi_spec_validator(
        self, spec_version: Version
    ):  # pragma: nocover
        from openapi_spec_validator import validate_spec
        from jsonschema.exceptions import ValidationError as JSEValidationError
        from jsonschema.exceptions import RefResolutionError

        # Validate according to detected version. Unsupported versions are
        # already caught outside of this function.
        from .util.exceptions import raise_from

        if spec_version.major == 3:
            # Set the version independently of whether validation succeeds
            self.__set_version(BaseParser.SPEC_VERSION_3_PREFIX, spec_version)
        elif spec_version.major == 2:
            # Set the version independently of whether validation succeeds
            self.__set_version(BaseParser.SPEC_VERSION_2_PREFIX, spec_version)

        try:
            validate_spec(self.specification)
        except TypeError as type_ex:  # pragma: nocover
            raise_from(ValidationError, type_ex, self._strict_warning())
        except JSEValidationError as v2_ex:
            raise_from(ValidationError, v2_ex)
        except RefResolutionError as ref_ex:
            raise_from(ValidationError, ref_ex)

    def _strict_warning(self):
        """Return a warning if strict mode is off."""
        if self.options.get("strict", True):
            return (
                "Strict mode enabled (the default), so this could be due to an "
                "integer key, such as an HTTP status code."
            )
        return (
            "Strict mode disabled. Prance cannot help you narrow this further "
            "down, sorry."
        )


class ResolvingParser(BaseParser):
    """The ResolvingParser extends BaseParser with resolving references by inlining."""

    def __init__(self, url=None, spec_string=None, lazy=False, **kwargs):
        """
        See :py:class:`BaseParser`.

        Resolves JSON pointers/references (i.e. '$ref' keys) before validating the
        specs. The implication is that self.specification is fully resolved, and
        does not contain any references.

        Additional parameters, see :py::class:`util.RefResolver`.
        """
        # Create a reference cache
        self.__reference_cache = {}

        BaseParser.__init__(self, url=url, spec_string=spec_string, lazy=lazy, **kwargs)

    def _validate(self):
        # We have a problem with the BaseParser's validate function: the
        # jsonschema implementation underlying it does not accept relative
        # path references, but the Swagger specs allow them:
        # http://swagger.io/specification/#referenceObject
        # We therefore use our own resolver first, and validate later.
        from .util.resolver import RefResolver

        forward_arg_names = (
            "encoding",
            "recursion_limit",
            "recursion_limit_handler",
            "resolve_types",
            "resolve_method",
            "strict",
        )
        forward_args = {
            k: v for (k, v) in self.options.items() if k in forward_arg_names
        }
        resolver = RefResolver(
            self.specification,
            self.url,
            reference_cache=self.__reference_cache,
            **forward_args,
        )
        resolver.resolve_references()
        self.specification = resolver.specs

        # Now validate - the BaseParser knows the specifics
        BaseParser._validate(self)


# Underscored to allow some time for the public API to be stabilized.
class _TranslatingParser(BaseParser):
    def _validate(self):
        from .util.translator import _RefTranslator

        translator = _RefTranslator(self.specification, self.url)
        translator.translate_references()
        self.specification = translator.specs

        BaseParser._validate(self)
