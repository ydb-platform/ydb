"""Test OpenAPI specs examples."""

__author__ = "Jens Finkhaeuser"
__copyright__ = "Copyright (c) 2018 Jens Finkhaeuser"
__license__ = "MIT"
__all__ = ()

import pytest

from prance import ResolvingParser
from prance import ValidationError
from prance.util.fs import FileNotFoundError

import yatest.common as yc


def make_name(path, parser, backend, version, file_format, entry):
    import os.path

    basename = os.path.splitext(entry)[0]
    basename = basename.replace("-", "_")
    version = version.replace(".", "")
    backend = backend.replace("-", "_")

    name = "_".join(["test", parser, backend, version, file_format, basename])
    return name


# Generate test cases at import.
# One case per combination of:
# - parser (base, resolving)
# - validation backend (flex, swagger-spec-validator) (skip ssv if importing it fails)
# - spec version
# - file format
# - file
# That gives >50 test cases

import os, os.path

base = yc.test_source_path(os.path.join("OpenAPI-Specification", "examples"))


def iter_entries(parser, backend, version, file_format, path):
    if version == "v3.0" and backend != "openapi-spec-validator":
        return

    for entry in os.listdir(path):
        full = os.path.join(path, entry)
        testcase_name = None
        if os.path.isfile(full):
            testcase_name = make_name(
                full, parser, backend, version, file_format, entry
            )
        elif os.path.isdir(full):
            if parser == "BaseParser":
                continue  # skip separate files for the BaseParser
            full = os.path.join(full, "spec", "swagger.%s" % (file_format))
            if os.path.isfile(full):
                testcase_name = make_name(
                    full, parser, backend, version, file_format, entry
                )

        if testcase_name:
            dirname = os.path.dirname(full)
            dirname = dirname.replace("\\", "\\\\")
            from prance.util import url

            absurl = url.absurl(os.path.abspath(full)).geturl()
            code = f"""
@pytest.mark.xfail()
def {testcase_name}():
  import os
  cur = os.getcwd()

  os.chdir('{dirname}')

  from prance import {parser}
  try:
    parser = {parser}('{absurl}', backend = '{backend}')
  finally:
    os.chdir(cur)
"""
            print(code)
            exec(code, globals())


for parser in ("BaseParser", "ResolvingParser"):
    from prance.util import validation_backends

    for backend in validation_backends():
        for version in os.listdir(base):
            version_dir = os.path.join(base, version)
            for file_format in os.listdir(version_dir):
                format_dir = os.path.join(version_dir, file_format)

                if not os.path.isdir(format_dir):  # Assume YAML
                    iter_entries(parser, backend, version, "yaml", version_dir)
                else:
                    for entry in os.listdir(format_dir):
                        iter_entries(parser, backend, version, file_format, format_dir)
