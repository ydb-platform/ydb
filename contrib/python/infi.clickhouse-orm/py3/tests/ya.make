PY3TEST()

SIZE(MEDIUM)

INCLUDE(${ARCADIA_ROOT}/library/recipes/clickhouse/recipe.inc)

NO_LINT()

TEST_SRCS(
    base_test_with_data.py
    common.py
    test_alias_fields.py
    test_array_fields.py
    test_buffer.py
    test_compressed_fields.py
    test_constraints.py
    test_custom_fields.py
    test_database.py
    test_datetime_fields.py
    test_decimal_fields.py
    test_dictionaries.py
    test_engines.py
    test_enum_fields.py
    test_fixed_string_fields.py
    test_funcs.py
    test_indexes.py
    test_inheritance.py
    test_ip_fields.py
    test_join.py
    test_materialized_fields.py
    test_models.py
    test_mutations.py
    test_nullable_fields.py
    test_querysets.py
    test_readonly.py
    test_server_errors.py
    test_simple_fields.py
    test_system_models.py
    test_uuid_fields.py
)

PEERDIR(
    contrib/python/infi.clickhouse-orm
)

END()
