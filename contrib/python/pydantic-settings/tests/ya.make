PY3TEST()

TEST_SRCS(
    conftest.py
    # test_docs.py  # needs pytest-examples
    test_precedence_and_merging.py
    test_settings.py
    # test_source_aws_secrets_manager.py
    # test_source_azure_key_vault.py
    test_source_cli.py
    # test_source_gcp_secret_manager.py
    test_source_json.py
    test_source_nested_secrets.py
    test_source_pyproject_toml.py
    test_source_toml.py
    test_source_yaml.py
    test_utils.py
)

DATA(
    arcadia/contrib/python/pydantic-settings/tests
)

PEERDIR(
    contrib/python/PyYAML
    contrib/python/annotated-types
    contrib/python/pydantic-settings
    contrib/python/pytest-mock
    contrib/python/tomli
)

NO_LINT()

END()
