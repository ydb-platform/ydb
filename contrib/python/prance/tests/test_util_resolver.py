"""Test suite for prance.util.resolver ."""

__author__ = "Jens Finkhaeuser"
__copyright__ = "Copyright (c) 2016-2021 Jens Finkhaeuser"
__license__ = "MIT"
__all__ = ()

import yatest.common as yc

import pytest
from unittest.mock import patch

from prance.util import fs
from prance.util import resolver
from prance.util.url import ResolutionError

from . import none_of


def get_specs(fname):
    specs = fs.read_file(fname)

    from prance.util import formats

    specs = formats.parse_spec(specs, fname)

    return specs


def mock_get_petstore(*args, **kwargs):
    from .mock_response import MockResponse, PETSTORE_YAML

    return MockResponse(text=PETSTORE_YAML)


def recursion_limit_handler_none(limit, refstring, recursions):
    return None


@pytest.fixture
def externals_file():
    return get_specs(yc.test_source_path("specs/with_externals.yaml"))


@pytest.fixture
def recursive_objs_file():
    return get_specs(yc.test_source_path("specs/recursive_objs.yaml"))


@pytest.fixture
def recursive_files_file():
    return get_specs(yc.test_source_path("specs/recursive_files.yaml"))


@pytest.fixture
def recursion_limit_file():
    return get_specs(yc.test_source_path("specs/recursion_limit.yaml"))


@pytest.fixture
def recursion_limit_files_file():
    return get_specs(yc.test_source_path("specs/recursion_limit_files.yaml"))


@pytest.fixture
def missing_file():
    return get_specs(yc.test_source_path("specs/missing_reference.yaml"))


def test_resolver_noname(externals_file):
    res = resolver.RefResolver(externals_file)
    # Can't build a fragment URL without reference
    with pytest.raises(ResolutionError):
        res.resolve_references()


@patch("requests.get")
def test_resolver_named(mock_get, externals_file):
    mock_get.side_effect = mock_get_petstore

    import os.path
    from prance.util import fs

    res = resolver.RefResolver(
        externals_file, fs.abspath(yc.test_source_path("specs/with_externals.yaml"))
    )
    res.resolve_references()


@patch("requests.get")
def test_resolver_missing_reference(mock_get, missing_file):
    mock_get.side_effect = mock_get_petstore

    import os.path

    res = resolver.RefResolver(
        missing_file, fs.abspath(yc.test_source_path("specs/missing_reference.yaml"))
    )
    with pytest.raises(ResolutionError) as exc:
        res.resolve_references()

    assert str(exc.value).startswith("Cannot resolve")


@patch("requests.get")
def test_resolver_recursive_objects(mock_get, recursive_objs_file):
    mock_get.side_effect = mock_get_petstore

    # Recursive references to objects are a problem
    import os.path

    res = resolver.RefResolver(
        recursive_objs_file, fs.abspath(yc.test_source_path("specs/recursive_objs.yaml"))
    )
    with pytest.raises(ResolutionError) as exc:
        res.resolve_references()

    assert str(exc.value).startswith("Recursion reached limit")


@patch("requests.get")
def test_resolver_recursive_files(mock_get, recursive_files_file):
    mock_get.side_effect = mock_get_petstore

    # Recursive references to files are not a problem
    import os.path

    res = resolver.RefResolver(
        recursive_files_file, fs.abspath(yc.test_source_path("specs/recursive_files.yaml"))
    )
    res.resolve_references()


def test_recursion_limit_do_not_recurse_raise(recursion_limit_file):
    # Expect the default behaviour to raise.
    import os.path

    res = resolver.RefResolver(
        recursion_limit_file, fs.abspath(yc.test_source_path("specs/recursion_limit.yaml"))
    )
    with pytest.raises(ResolutionError) as exc:
        res.resolve_references()

    assert str(exc.value).startswith("Recursion reached limit of 1")


def test_recursion_limit_do_not_recurse_ignore(recursion_limit_file):
    # If we overload the handler, we should not get an error but should
    # also simply not have the 'next' field - or it should be None
    import os.path

    res = resolver.RefResolver(
        recursion_limit_file,
        fs.abspath(yc.test_source_path("specs/recursion_limit.yaml")),
        recursion_limit_handler=recursion_limit_handler_none,
    )
    res.resolve_references()

    from prance.util import formats

    contents = formats.serialize_spec(res.specs, "foo.yaml")

    # The effect of returning None on recursion limit should be that
    # despite having recursion, the outermost reference to
    # definitions/Pet should get resolved.
    assert (
        "properties" in res.specs["paths"]["/pets"]["get"]["responses"]["200"]["schema"]
    )

    # However, the 'next' field should not be resolved.
    assert (
        res.specs["paths"]["/pets"]["get"]["responses"]["200"]["schema"]["properties"][
            "next"
        ]["schema"]
        is None
    )


def test_recursion_limit_set_limit_ignore(recursion_limit_file):
    # If we overload the handler, and set the recursion limit higher,
    # we should get nested Pet objects a few levels deep.

    import os.path

    res = resolver.RefResolver(
        recursion_limit_file,
        fs.abspath(yc.test_source_path("specs/recursion_limit.yaml")),
        recursion_limit=2,
        recursion_limit_handler=recursion_limit_handler_none,
    )
    res.resolve_references()

    from prance.util import formats

    contents = formats.serialize_spec(res.specs, "foo.yaml")

    # The effect of returning None on recursion limit should be that
    # despite having recursion, the outermost reference to
    # definitions/Pet should get resolved.
    assert (
        "properties" in res.specs["paths"]["/pets"]["get"]["responses"]["200"]["schema"]
    )

    # However, the 'next' field should be resolved due to the higher recursion limit
    next_field = res.specs["paths"]["/pets"]["get"]["responses"]["200"]["schema"][
        "properties"
    ]["next"]["schema"]
    assert next_field is not None

    # But the 'next' field of the 'next' field should not be resolved.
    assert next_field["properties"]["next"]["schema"] is None


def test_recursion_limit_do_not_recurse_raise_files(recursion_limit_files_file):
    # Expect the default behaviour to raise.
    import os.path

    res = resolver.RefResolver(
        recursion_limit_files_file, fs.abspath(yc.test_source_path("specs/recursion_limit_files.yaml"))
    )
    with pytest.raises(ResolutionError) as exc:
        res.resolve_references()

    assert str(exc.value).startswith("Recursion reached limit of 1")


def test_recursion_limit_do_not_recurse_ignore_files(recursion_limit_files_file):
    # If we overload the handler, we should not get an error but should
    # also simply not have the 'next' field - or it should be None
    import os.path

    res = resolver.RefResolver(
        recursion_limit_files_file,
        fs.abspath(yc.test_source_path("specs/recursion_limit_files.yaml")),
        recursion_limit_handler=recursion_limit_handler_none,
    )
    res.resolve_references()

    from prance.util import formats

    contents = formats.serialize_spec(res.specs, "foo.yaml")

    # The effect of returning None on recursion limit should be that
    # despite having recursion, the outermost reference to
    # definitions/Pet should get resolved.
    assert (
        "properties" in res.specs["paths"]["/pets"]["get"]["responses"]["200"]["schema"]
    )

    # However, the 'next' field should not be resolved.
    assert (
        res.specs["paths"]["/pets"]["get"]["responses"]["200"]["schema"]["properties"][
            "next"
        ]["schema"]
        is None
    )


def test_recursion_limit_set_limit_ignore_files(recursion_limit_files_file):
    # If we overload the handler, and set the recursion limit higher,
    # we should get nested Pet objects a few levels deep.

    import os.path

    res = resolver.RefResolver(
        recursion_limit_files_file,
        fs.abspath(yc.test_source_path("specs/recursion_limit_files.yaml")),
        recursion_limit=2,
        recursion_limit_handler=recursion_limit_handler_none,
    )
    res.resolve_references()

    from prance.util import formats

    contents = formats.serialize_spec(res.specs, "foo.yaml")

    # The effect of returning None on recursion limit should be that
    # despite having recursion, the outermost reference to
    # definitions/Pet should get resolved.
    assert (
        "properties" in res.specs["paths"]["/pets"]["get"]["responses"]["200"]["schema"]
    )

    # However, the 'next' field should be resolved due to the higher recursion limit
    next_field = res.specs["paths"]["/pets"]["get"]["responses"]["200"]["schema"][
        "properties"
    ]["next"]["schema"]
    assert next_field is not None

    # But the 'next' field of the 'next' field should not be resolved.
    assert next_field["properties"]["next"]["schema"] is None


@patch("requests.get")
def test_issue_22_empty_path(mock_get, externals_file):
    mock_get.side_effect = mock_get_petstore

    # The raw externals file must have unresolved data
    assert len(externals_file["paths"]["/pets/{petId}"]["get"]["parameters"]) == 1
    param = externals_file["paths"]["/pets/{petId}"]["get"]["parameters"][0]

    assert "overwritten" in param
    assert "$ref" in param

    import os.path
    from prance.util import fs

    res = resolver.RefResolver(
        externals_file, fs.abspath(yc.test_source_path("specs/with_externals.yaml"))
    )
    res.resolve_references()

    # The tests should resolve the reference, but the reference overwrites
    # all else.
    assert len(res.specs["paths"]["/pets/{petId}"]["get"]["parameters"]) == 1
    param = res.specs["paths"]["/pets/{petId}"]["get"]["parameters"][0]

    # Dereferenced keys must exist here
    assert "type" in param
    assert "description" in param
    assert "required" in param
    assert "in" in param
    assert "name" in param

    # Previously defined keys must not
    assert "overwritten" not in param
    assert "$ref" not in param


def test_issue_38_tilde_one():
    specs = get_specs(yc.test_source_path("specs/issue_38_a.yaml"))
    res = resolver.RefResolver(specs, fs.abspath(yc.test_source_path("specs/issue_38_a.yaml")))
    res.resolve_references()

    path = res.specs["paths"]["/api/v2/vms"]
    assert "get" in path
    assert "operationId" in path["get"]
    assert "description" in path["get"]


@patch("requests.get")
def test_issue_23_partial_resolution_all(mock_get):
    mock_get.side_effect = mock_get_petstore

    specs = get_specs(yc.test_source_path("specs/with_externals.yaml"))
    res = resolver.RefResolver(specs, fs.abspath(yc.test_source_path("specs/with_externals.yaml")))
    res.resolve_references()

    # By default, all externals need to be resolved.
    from prance.util.path import path_get

    val = path_get(res.specs, ("paths", "/pets", "get", "responses", "200", "schema"))
    assert "$ref" not in val

    val = path_get(
        res.specs, ("paths", "/pets", "get", "responses", "default", "schema")
    )
    assert "$ref" not in val

    val = path_get(
        res.specs, ("paths", "/pets", "post", "responses", "default", "schema")
    )
    assert "$ref" not in val

    val = path_get(res.specs, ("paths", "/pets/{petId}", "get", "parameters", 0))
    assert "$ref" not in val

    val = path_get(
        res.specs, ("paths", "/pets/{petId}", "get", "responses", "200", "schema")
    )
    assert "$ref" not in val

    val = path_get(
        res.specs, ("paths", "/pets/{petId}", "get", "responses", "203", "schema")
    )
    assert "$ref" not in val

    val = path_get(
        res.specs, ("paths", "/pets/{petId}", "get", "responses", "default", "schema")
    )
    assert "$ref" not in val


def test_issue_23_partial_resolution_internal():
    specs = get_specs(yc.test_source_path("specs/with_externals.yaml"))
    res = resolver.RefResolver(
        specs,
        fs.abspath(yc.test_source_path("specs/with_externals.yaml")),
        resolve_types=resolver.RESOLVE_INTERNAL,
    )
    res.resolve_references()

    # By default, all externals need to be resolved.
    from prance.util.path import path_get

    val = path_get(res.specs, ("paths", "/pets", "get", "responses", "200", "schema"))
    assert "$ref" in val

    val = path_get(
        res.specs, ("paths", "/pets", "get", "responses", "default", "schema")
    )
    assert "$ref" in val

    val = path_get(
        res.specs, ("paths", "/pets", "post", "responses", "default", "schema")
    )
    assert "$ref" in val

    val = path_get(res.specs, ("paths", "/pets/{petId}", "get", "parameters", 0))
    assert "$ref" in val

    val = path_get(
        res.specs, ("paths", "/pets/{petId}", "get", "responses", "200", "schema")
    )
    assert "$ref" in val

    val = path_get(
        res.specs, ("paths", "/pets/{petId}", "get", "responses", "203", "schema")
    )
    assert "$ref" in val

    val = path_get(
        res.specs, ("paths", "/pets/{petId}", "get", "responses", "default", "schema")
    )
    assert "$ref" not in val


def test_issue_23_partial_resolution_files():
    specs = get_specs(yc.test_source_path("specs/with_externals.yaml"))
    res = resolver.RefResolver(
        specs,
        fs.abspath(yc.test_source_path("specs/with_externals.yaml")),
        resolve_types=resolver.RESOLVE_FILES,
    )
    res.resolve_references()

    # By default, all externals need to be resolved.
    from prance.util.path import path_get

    val = path_get(res.specs, ("paths", "/pets", "get", "responses", "200", "schema"))
    assert "$ref" not in val

    val = path_get(
        res.specs, ("paths", "/pets", "get", "responses", "default", "schema")
    )
    assert "$ref" not in val

    val = path_get(
        res.specs, ("paths", "/pets", "post", "responses", "default", "schema")
    )
    assert "$ref" not in val

    val = path_get(res.specs, ("paths", "/pets/{petId}", "get", "parameters", 0))
    assert "$ref" not in val

    val = path_get(
        res.specs, ("paths", "/pets/{petId}", "get", "responses", "200", "schema")
    )
    assert "$ref" not in val

    val = path_get(
        res.specs, ("paths", "/pets/{petId}", "get", "responses", "203", "schema")
    )
    assert "$ref" not in val

    val = path_get(
        res.specs, ("paths", "/pets/{petId}", "get", "responses", "default", "schema")
    )
    assert "$ref" in val


@pytest.mark.skipif(none_of("openapi-spec-validator"), reason="Missing backends")
def test_issue_65_partial_resolution_files():
    specs = """openapi: "3.0.0"
info:
  title: ''
  version: '1.0.0'
paths: {}
components:
    schemas:
        SampleArray:
            type: array
            items:
              $ref: '#/components/schemas/ItemType'

        ItemType:
          type: integer
"""
    from prance.util import formats

    specs = formats.parse_spec(specs, "issue_65.yaml")

    res = resolver.RefResolver(
        specs, fs.abspath("issue_65.yaml"), resolve_types=resolver.RESOLVE_FILES
    )
    res.resolve_references()

    from prance.util.path import path_get

    val = path_get(res.specs, ("components", "schemas", "SampleArray", "items"))
    assert "$ref" in val


@patch("requests.get")
def test_issue_23_partial_resolution_http(mock_get):
    mock_get.side_effect = mock_get_petstore

    specs = get_specs(yc.test_source_path("specs/with_externals.yaml"))
    res = resolver.RefResolver(
        specs,
        fs.abspath(yc.test_source_path("specs/with_externals.yaml")),
        resolve_types=resolver.RESOLVE_HTTP,
    )
    res.resolve_references()

    # By default, all externals need to be resolved.
    from prance.util.path import path_get

    val = path_get(res.specs, ("paths", "/pets", "get", "responses", "200", "schema"))
    assert "$ref" in val

    val = path_get(
        res.specs, ("paths", "/pets", "get", "responses", "default", "schema")
    )
    assert "$ref" in val

    val = path_get(
        res.specs, ("paths", "/pets", "post", "responses", "default", "schema")
    )
    assert "$ref" in val

    val = path_get(res.specs, ("paths", "/pets/{petId}", "get", "parameters", 0))
    assert "$ref" in val

    val = path_get(
        res.specs, ("paths", "/pets/{petId}", "get", "responses", "200", "schema")
    )
    assert "$ref" in val

    val = path_get(
        res.specs, ("paths", "/pets/{petId}", "get", "responses", "203", "schema")
    )
    assert "$ref" in val

    val = path_get(
        res.specs, ("paths", "/pets/{petId}", "get", "responses", "default", "schema")
    )
    assert "$ref" in val


def test_issue_23_partial_resolution_invalid_scheme():
    specs = {"$ref": "foo://cannot-do-anything"}
    res = resolver.RefResolver(specs, fs.abspath(yc.test_source_path("specs/with_externals.yaml")))

    with pytest.raises(ValueError):
        res.resolve_references()


def test_issue_69_urlparse_error():
    # XXX depending on python version, the error may not actually come from
    #     parsing the URL, but from trying to load a local file that does
    #     not exist. See test_issue_72_* for a test case that specifically
    #     tries for a nonexistent file url.
    specs = {"$ref": "file://a\u2100b/bad/netloc"}
    res = resolver.RefResolver(specs, fs.abspath(yc.test_source_path("specs/with_externals.yaml")))

    with pytest.raises(ResolutionError) as exinfo:
        res.resolve_references()

    assert "bad/netloc" in str(exinfo.value)


def test_issue_72_nonexistent_file_error():
    specs = {"$ref": "file://does/not/exist"}
    res = resolver.RefResolver(specs, fs.abspath(yc.test_source_path("specs/with_externals.yaml")))

    with pytest.raises(ResolutionError) as exinfo:
        res.resolve_references()

    assert "not/exist" in str(exinfo.value)


@pytest.mark.skip
@pytest.mark.skipif(none_of("openapi-spec-validator"), reason="Missing backends")
def test_issue_78_resolve_internal_bug():
    specs = ""
    with open(yc.test_source_path("specs/issue_78/openapi.json")) as fh:
        specs = fh.read()

    from prance.util import formats

    specs = formats.parse_spec(specs, "openapi.json")

    res = resolver.RefResolver(
        specs, fs.abspath("openapi.json"), resolve_types=resolver.RESOLVE_FILES
    )
    res.resolve_references()

    from prance.util.path import path_get

    val = path_get(res.specs, ("paths", "/endpoint", "post", "requestBody", "content"))

    # Reference to file is resolved
    assert "application/json" in val
    # Internal reference within file is NOT resolved
    assert "$ref" in val["application/json"]


@pytest.mark.skip
@pytest.mark.skipif(none_of("openapi-spec-validator"), reason="Missing backends")
def test_issue_77_translate_external():
    specs = ""
    with open(yc.test_source_path("specs/issue_78/openapi.json")) as fh:
        specs = fh.read()

    from prance.util import formats

    specs = formats.parse_spec(specs, "openapi.json")

    res = resolver.RefResolver(
        specs,
        fs.abspath("openapi.json"),
        resolve_types=resolver.RESOLVE_FILES,
        resolve_method=resolver.TRANSLATE_EXTERNAL,
    )
    res.resolve_references()

    from prance.util.path import path_get

    val = path_get(res.specs, ("components", "schemas", "_schemas.json_Body"))

    # Reference to file is translated in components/schemas
    assert "content" in val
    assert "application/json" in val["content"]

    # Reference url is updated
    val = path_get(res.specs, ("paths", "/endpoint", "post", "requestBody", "$ref"))
    assert val == "#/components/schemas/_schemas.json_Body"


@pytest.mark.skip
@pytest.mark.skipif(none_of("openapi-spec-validator"), reason="Missing backends")
def test_issue_77_translate_external_refs_internal():
    specs = ""
    with open(yc.test_source_path("specs/issue_78/openapi.json")) as fh:
        specs = fh.read()

    from prance.util import formats

    specs = formats.parse_spec(specs, "openapi.json")

    res = resolver.RefResolver(
        specs,
        fs.abspath("openapi.json"),
        resolve_types=resolver.RESOLVE_FILES | resolver.RESOLVE_INTERNAL,
        resolve_method=resolver.TRANSLATE_EXTERNAL,
    )
    res.resolve_references()

    from prance.util.path import path_get

    val = path_get(res.specs, ("components", "schemas", "_schemas.json_Body"))

    # Reference to file is translated in components/schemas
    assert "content" in val
    assert "application/json" in val["content"]

    # Internal Reference links updated
    assert (
        "#/components/schemas/_schemas.json_Something"
        == val["content"]["application/json"]["$ref"]
    )

    # Internal references is copied to components/schemas separately
    val = path_get(res.specs, ("components", "schemas", "_schemas.json_Something"))
    assert "type" in val

    # File reference url is updated as well
    val = path_get(res.specs, ("paths", "/endpoint", "post", "requestBody", "$ref"))
    assert val == "#/components/schemas/_schemas.json_Body"


@pytest.mark.skip
@pytest.mark.skipif(none_of("openapi-spec-validator"), reason="Missing backends")
def test_issue_77_internal_refs_unresolved():
    specs = ""
    with open(yc.test_source_path("specs/issue_78/openapi.json")) as fh:
        specs = fh.read()

    from prance.util import formats

    specs = formats.parse_spec(specs, "openapi.json")

    res = resolver.RefResolver(
        specs,
        fs.abspath("openapi.json"),
        resolve_types=resolver.RESOLVE_FILES,
        resolve_method=resolver.TRANSLATE_EXTERNAL,
    )
    res.resolve_references()

    from prance.util.path import path_get

    val = path_get(res.specs, ("components", "schemas"))

    # File reference resolved
    assert "_schemas.json_Body" in val

    # Internal file reference not resolved
    assert "_schemas.json_Something" not in val


@pytest.mark.xfail(reason="https://github.com/RonnyPfannschmidt/prance/issues/87")
def test_issue_205_swagger_resolution_failure():
    specs = ""
    with open(yc.test_source_path("specs/kubernetes_api_docs.json")) as fh:
        specs = fh.read()

    from prance.util import formats

    specs = formats.parse_spec(specs, "kubernetes_api_docs.json")

    res = resolver.RefResolver(
        specs,
        fs.abspath("kubernetes_api_docs.json"),
        resolve_types=resolver.RESOLVE_FILES,
        resolve_method=resolver.TRANSLATE_EXTERNAL,
    )
    # test will raise an exception
    res.resolve_references()


@patch("requests.get")
def test_resolve_package_ref(mock_get):
    mock_get.side_effect = mock_get_petstore

    specs = get_specs(yc.test_source_path("specs/with_externals.yaml"))
    res = resolver.RefResolver(
        specs,
        fs.abspath(yc.test_source_path("specs/with_externals.yaml")),
        resolve_types=resolver.RESOLVE_FILES,
    )
    res.resolve_references()

    from prance.util.path import path_get

    val = path_get(
        res.specs, ("paths", "/pets/{petId}", "get", "responses", "203", "schema")
    )
    assert "required" in val
