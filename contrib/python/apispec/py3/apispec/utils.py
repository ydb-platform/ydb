"""Various utilities for parsing OpenAPI operations from docstrings and validating against
the OpenAPI spec.
"""
import re
import json

import setuptools
from distutils import version

from apispec import exceptions


COMPONENT_SUBSECTIONS = {
    2: {
        "schema": "definitions",
        "response": "responses",
        "parameter": "parameters",
        "security_scheme": "securityDefinitions",
    },
    3: {
        "schema": "schemas",
        "response": "responses",
        "parameter": "parameters",
        "example": "examples",
        "security_scheme": "securitySchemes",
    },
}


def build_reference(component_type, openapi_major_version, component_name):
    """Return path to reference

    :param str component_type: Component type (schema, parameter, response, security_scheme)
    :param int openapi_major_version: OpenAPI major version (2 or 3)
    :param str component_name: Name of component to reference
    """
    return {
        "$ref": "#/{}{}/{}".format(
            "components/" if openapi_major_version >= 3 else "",
            COMPONENT_SUBSECTIONS[openapi_major_version][component_type],
            component_name,
        )
    }


def validate_spec(spec):
    """Validate the output of an :class:`APISpec` object against the
    OpenAPI specification.

    Note: Requires installing apispec with the ``[validation]`` extras.
    ::

        pip install 'apispec[validation]'

    :raise: apispec.exceptions.OpenAPIError if validation fails.
    """
    try:
        import prance
    except ImportError as error:  # re-raise with a more verbose message
        exc_class = type(error)
        raise exc_class(
            "validate_spec requires prance to be installed. "
            "You can install all validation requirements using:\n"
            "    pip install 'apispec[validation]'"
        )
    parser_kwargs = {}
    if spec.openapi_version.version[0] == 3:
        parser_kwargs["backend"] = "openapi-spec-validator"
    try:
        prance.BaseParser(spec_string=json.dumps(spec.to_dict()), **parser_kwargs)
    except prance.ValidationError as err:
        raise exceptions.OpenAPIError(*err.args)
    else:
        return True


class OpenAPIVersion(version.LooseVersion):
    """OpenAPI version

    :param str|OpenAPIVersion openapi_version: OpenAPI version

    Parses an OpenAPI version expressed as string. Provides shortcut to digits
    (major, minor, patch).

        Example: ::

            ver = OpenAPIVersion('3.0.2')
            assert ver.major == 3
            assert ver.minor == 0
            assert ver.patch == 1
            assert ver.vstring == '3.0.2'
            assert str(ver) == '3.0.2'
    """

    MIN_INCLUSIVE_VERSION = version.LooseVersion("2.0")
    MAX_EXCLUSIVE_VERSION = version.LooseVersion("4.0")

    def __init__(self, openapi_version):
        if isinstance(openapi_version, version.LooseVersion):
            openapi_version = openapi_version.vstring
        if (
            not self.MIN_INCLUSIVE_VERSION
            <= openapi_version
            < self.MAX_EXCLUSIVE_VERSION
        ):
            raise exceptions.APISpecError(
                "Not a valid OpenAPI version number: {}".format(openapi_version)
            )
        super().__init__(openapi_version)

    @property
    def major(self):
        return self.version[0]

    @property
    def minor(self):
        return self.version[1]

    @property
    def patch(self):
        return self.version[2]


# from django.contrib.admindocs.utils
def trim_docstring(docstring):
    """Uniformly trims leading/trailing whitespace from docstrings.

    Based on http://www.python.org/peps/pep-0257.html#handling-docstring-indentation
    """
    if not docstring or not docstring.strip():
        return ""
    # Convert tabs to spaces and split into lines
    lines = docstring.expandtabs().splitlines()
    indent = min(len(line) - len(line.lstrip()) for line in lines if line.lstrip())
    trimmed = [lines[0].lstrip()] + [line[indent:].rstrip() for line in lines[1:]]
    return "\n".join(trimmed).strip()


# from rest_framework.utils.formatting
def dedent(content):
    """
    Remove leading indent from a block of text.
    Used when generating descriptions from docstrings.
    Note that python's `textwrap.dedent` doesn't quite cut it,
    as it fails to dedent multiline docstrings that include
    unindented text on the initial line.
    """
    whitespace_counts = [
        len(line) - len(line.lstrip(" "))
        for line in content.splitlines()[1:]
        if line.lstrip()
    ]

    # unindent the content if needed
    if whitespace_counts:
        whitespace_pattern = "^" + (" " * min(whitespace_counts))
        content = re.sub(re.compile(whitespace_pattern, re.MULTILINE), "", content)

    return content.strip()


# http://stackoverflow.com/a/8310229
def deepupdate(original, update):
    """Recursively update a dict.

    Subdict's won't be overwritten but also updated.
    """
    for key, value in original.items():
        if key not in update:
            update[key] = value
        elif isinstance(value, dict):
            deepupdate(value, update[key])
    return update
