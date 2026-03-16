from os.path import join
from re import match

from pytest import fixture

from prance import _TranslatingParser


@fixture
def tester(request):
    pattern = r"test_(.+)"
    test_function_name = request.node.name
    name_match = match(pattern, test_function_name)
    test_name = name_match[1]

    file_name = f"{test_name}.spec.yaml"
    path = join("tests", "specs", "translating_parser", file_name)
    return SpecificationTester(path)


class SpecificationTester:
    """
    Helper class that tests correct reference translation. Loads a specification file from tests/specs, parses it with
    the TranslatingParser and provides assertion methods:
    * assert_schemas to check what’s in /components/schemas.
    * assert_path_ref to check the $ref of the only operation response in the specification.
    * assert_schema_ref to check the $ref of a schema in /components/schemas.

    Typical usage:
    tester = SpecificationTester("my_spec_file")
    tester.assert_path_ref("some_other_refd_file.spec.yaml_SomeObject")
    tester.assert_schemas(
        {"some_other_refd_file.spec.yaml_SomeObject", "some_other_refd_file.spec.yaml_SomeOtherObject"}
    )
    tester.assert_schema_ref(
        "some_other_refd_file.spec.yaml_SomeObject", "some_other_refd_file.spec.yaml_SomeOtherObject",
    )
    """

    @staticmethod
    def _assert_ref(schema, ref):
        assert "$ref" in schema
        assert schema["$ref"] == f"#/components/schemas/{ref}"

    def __init__(self, url):
        parser = _TranslatingParser(url)
        parser.parse()
        self.specification = parser.specification

    def assert_schemas(self, keys):
        """
        Asserts schema names in /components/schemas.
        """
        assert self.specification["components"]["schemas"].keys() == keys

    def assert_path_ref(self, ref):
        """
        Asserts $ref value in /paths/hosts/get/responses/default/content/application/json/schema.
        """
        responses = self.specification["paths"]["/hosts"]["get"]["responses"]
        schema = responses["default"]["content"]["application/json"]["schema"]
        self._assert_ref(schema, ref)

    def assert_schema_ref(self, key, ref, getter=lambda schema: schema):
        """
        Asserts $ref value in /components/schemas/{key}. May use a custom getter function to find the $ref deeper in
        the schema.

        Example:
            tester.assert_schema_ref(
                "SomeComplexObject",
                "schemas.spec.yaml_SomeOtherObject",
                lambda schema: schema["properties"]["some_property"]
            )
        """
        schemas = self.specification["components"]["schemas"]
        assert key in schemas
        schema = getter(schemas[key])
        self._assert_ref(schema, ref)


def test_local_reference_from_root(tester):
    tester.assert_path_ref("PlainObject")
    tester.assert_schemas({"PlainObject"})


def test_file_reference_from_root(tester):
    tester.assert_path_ref("file_reference_from_root_schemas.spec.yaml_PlainObject")
    tester.assert_schemas({"file_reference_from_root_schemas.spec.yaml_PlainObject"})


def test_local_reference_from_file(tester):
    tester.assert_path_ref("local_reference_from_file_schemas.spec.yaml_RefObject")
    tester.assert_schemas(
        {
            "local_reference_from_file_schemas.spec.yaml_RefObject",
            "local_reference_from_file_schemas.spec.yaml_PlainObject",
        }
    )
    tester.assert_schema_ref(
        "local_reference_from_file_schemas.spec.yaml_RefObject",
        "local_reference_from_file_schemas.spec.yaml_PlainObject",
    )


def test_same_file_reference_from_file(tester):
    tester.assert_path_ref("same_file_reference_from_file_schemas.spec.yaml_RefObject")
    tester.assert_schemas(
        {
            "same_file_reference_from_file_schemas.spec.yaml_RefObject",
            "same_file_reference_from_file_schemas.spec.yaml_PlainObject",
        }
    )
    tester.assert_schema_ref(
        "same_file_reference_from_file_schemas.spec.yaml_RefObject",
        "same_file_reference_from_file_schemas.spec.yaml_PlainObject",
    )


def test_different_file_reference_from_file(tester):
    tester.assert_path_ref(
        "different_file_reference_from_file_schemas1.spec.yaml_RefObject"
    )
    tester.assert_schemas(
        {
            "different_file_reference_from_file_schemas1.spec.yaml_RefObject",
            "different_file_reference_from_file_schemas2.spec.yaml_PlainObject",
        }
    )
    tester.assert_schema_ref(
        "different_file_reference_from_file_schemas1.spec.yaml_RefObject",
        "different_file_reference_from_file_schemas2.spec.yaml_PlainObject",
    )


def test_root_file_reference_from_file(tester):
    tester.assert_path_ref("root_file_reference_from_file_schemas.spec.yaml_RefObject")
    tester.assert_schemas(
        {"PlainObject", "root_file_reference_from_file_schemas.spec.yaml_RefObject"}
    )
    tester.assert_schema_ref(
        "root_file_reference_from_file_schemas.spec.yaml_RefObject", "PlainObject"
    )


def test_root_file_reference_from_root(tester):
    tester.assert_path_ref("PlainObject")
    tester.assert_schemas({"PlainObject"})


def test_recursive_reference_in_root(tester):
    tester.assert_schema_ref(
        "RecursiveObject",
        "RecursiveObject",
        lambda schema: schema["additionalProperties"],
    )


def test_recursive_reference_in_file(tester):
    tester.assert_path_ref(
        "recursive_reference_in_file_schemas.spec.yaml_RecursiveObject"
    )
    tester.assert_schemas(
        {"recursive_reference_in_file_schemas.spec.yaml_RecursiveObject"}
    )
    tester.assert_schema_ref(
        "recursive_reference_in_file_schemas.spec.yaml_RecursiveObject",
        "recursive_reference_in_file_schemas.spec.yaml_RecursiveObject",
        lambda schema: schema["additionalProperties"],
    )


def test_nested_recursive_reference_in_file(tester):
    tester.assert_path_ref("Response")
    tester.assert_schemas(
        {
            "Response",
            "ResultsItem",
            "ReferenceObject",
            "nested_recursive_reference_in_file_schemas.spec.yaml_ComplexObject",
            "nested_recursive_reference_in_file_schemas.spec.yaml_ComplexObjectProperty",
            "nested_recursive_reference_in_file_schemas.spec.yaml_RecursiveObject",
        }
    )
    tester.assert_schema_ref(
        "nested_recursive_reference_in_file_schemas.spec.yaml_ComplexObject",
        "nested_recursive_reference_in_file_schemas.spec.yaml_ComplexObjectProperty",
        lambda schema: schema["properties"]["property"],
    )
    tester.assert_schema_ref(
        "nested_recursive_reference_in_file_schemas.spec.yaml_ComplexObjectProperty",
        "nested_recursive_reference_in_file_schemas.spec.yaml_RecursiveObject",
        lambda schema: schema["properties"]["recursive"],
    )
    tester.assert_schema_ref(
        "nested_recursive_reference_in_file_schemas.spec.yaml_RecursiveObject",
        "nested_recursive_reference_in_file_schemas.spec.yaml_RecursiveObject",
        lambda schema: schema["additionalProperties"]["oneOf"][0],
    )
