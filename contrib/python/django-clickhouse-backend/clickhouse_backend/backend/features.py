from django.db import InterfaceError
from django.db.backends.base.features import BaseDatabaseFeatures
from django.utils.functional import cached_property


class DatabaseFeatures(BaseDatabaseFeatures):
    # TODO: figure out compatible clickhouse versions.
    minimum_database_version = None
    # Use this class attribute control whether using fake transaction.
    # Fake transaction is used in test, prevent other database such as postgresql
    # from flush at the end of each testcase. Only use this feature when you are
    # aware of the effect in TransactionTestCase.
    fake_transaction = False
    # Clickhouse do support Geo Data Types, but they are based on GeoJSON instead GIS.
    # https://clickhouse.com/docs/en/sql-reference/data-types/geo/
    # gis_enabled = False

    # There is no dedicated LOB type in clickhouse_backend, LOB is stored as string type.
    # https://clickhouse.com/docs/en/sql-reference/data-types/string/
    # allows_group_by_lob = True

    # There is no unique constraint in clickhouse_backend.
    # supports_nullable_unique_constraints = True
    # supports_partially_nullable_unique_constraints = True
    # supports_deferrable_unique_constraints = False

    # Clickhouse only supports limited transaction.
    # https://clickhouse.com/docs/en/sql-reference/ansi/
    # https://github.com/ClickHouse/ClickHouse/issues/32513
    # https://clickhouse.com/docs/en/guides/developer/transactional
    @cached_property
    def uses_savepoints(self):
        return self.fake_transaction

    can_release_savepoints = False

    # Is there a true datatype for uuid?
    has_native_uuid_field = True

    # Clickhouse use re2 syntax which does not support backreference.
    # https://clickhouse.com/docs/en/sql-reference/functions/string-search-functions#matchhaystack-pattern
    # https://github.com/google/re2/wiki/Syntax
    supports_regex_backreferencing = False

    # Can date/datetime lookups be performed using a string?
    supports_date_lookup_using_string = True

    # Confirm support for introspected foreign keys
    # Every database can do this reliably, except MySQL,
    # which can't do it for MyISAM tables
    can_introspect_foreign_keys = False

    # Map fields which some backends may not be able to differentiate to the
    # field it's introspected as.

    introspected_field_types = {
        "AutoField": "Int64Field",
        "BigAutoField": "Int64Field",
        "BigIntegerField": "Int64Field",
        "BinaryField": "StringField",
        "BooleanField": "BoolField",
        "CharField": "FixedStringField",
        # "DurationField": "DurationField",
        "GenericIPAddressField": "IPv6Field",
        "IntegerField": "Int32Field",
        "PositiveBigIntegerField": "UInt64Field",
        "PositiveIntegerField": "UInt32Field",
        "PositiveSmallIntegerField": "UInt16Field",
        "SmallAutoField": "Int64Field",
        "SmallIntegerField": "Int16Field",
        # "TimeField": "TimeField",
    }

    # https://clickhouse.com/docs/en/sql-reference/statements/alter/index/
    # Index manipulation is supported only for tables with *MergeTree* engine (including replicated variants).
    # Not btree index, don't support ordering.
    supports_index_column_ordering = False

    # Does the backend support introspection of materialized views?
    can_introspect_materialized_views = True

    # Support for the DISTINCT ON clause
    can_distinct_on_fields = True

    # Does the backend prevent running SQL queries in broken transactions?
    atomic_transactions = False

    # Can we issue more than one ALTER COLUMN clause in an ALTER TABLE?
    supports_combined_alters = True

    # Does it support foreign keys?
    supports_foreign_keys = False

    # Does it support CHECK constraints?
    supports_column_check_constraints = False
    supports_table_check_constraints = True
    # Does the backend support introspection of CHECK constraints?
    can_introspect_check_constraints = True

    # What kind of error does the backend throw when accessing closed cursor?
    closed_cursor_error_class = InterfaceError

    # https://clickhouse.com/docs/en/sql-reference/statements/insert-into/#constraints
    supports_ignore_conflicts = False

    # Does the backend support partial indexes (CREATE INDEX ... WHERE ...)?
    supports_partial_indexes = False

    # Does the backend support JSONField?
    supports_json_field = True
    # Can the backend introspect a JSONField?
    can_introspect_json_field = True
    # Does the backend support primitives in JSONField?
    supports_primitives_in_json_field = False
    # Is there a true datatype for JSON?
    has_native_json_field = True
    # Does the backend use PostgreSQL-style JSON operators like '->'?
    has_json_operators = False
    # Does the backend support __contains and __contained_by lookups for
    # a JSONField?
    supports_json_field_contains = False
    # Does value__d__contains={'f': 'g'} (without a list around the dict) match
    # {'d': [{'f': 'g'}]}?
    json_key_contains_list_matching_requires_list = False
    # Does the backend support JSONObject() database function?
    has_json_object_function = False

    # Does the backend support column collations?
    supports_collation_on_charfield = False
    supports_collation_on_textfield = False
    # Does the backend support non-deterministic collations?
    supports_non_deterministic_collations = False

    # Does the backend support column and table comments?
    supports_comments = True
    # Does the backend support column comments in ADD COLUMN statements?
    supports_comments_inline = True

    # SQL template override for tests.aggregation.tests.NowUTC
    test_now_utc_template = "now64()"

    # Does the database support SQL 2003 FILTER (WHERE ...) in aggregate
    # expressions?
    supports_aggregate_filter_clause = True

    # Does the backend support window expressions (expression OVER (...))?
    supports_over_clause = True
    supports_frame_range_fixed_distance = True
    only_supports_unbounded_with_preceding_and_following = False
    insert_test_table_with_defaults = (
        "INSERT INTO {} VALUES (DEFAULT, DEFAULT, DEFAULT)"
    )

    @cached_property
    def supports_transactions(self):
        return self.fake_transaction

    @cached_property
    def django_test_skips(self):
        skips = {}
        version = self.connection.get_database_version()
        if version >= (25, 11):
            skips.update(
                {
                    "ClickHouse 25.11 remove deprecated Object('json') type.": {
                        "clickhouse_fields.test_jsonfield.JsonFieldTests.test_query",
                        "expressions_window.tests.WindowFunctionTests.test_key_transform",
                    }
                }
            )
        if version < (25, 1):
            skips.update(
                {
                    "ClickHouse 25.1 add generateSerialID function.": {
                        "clickhouse_functions.test_other.OtherTests.test_generateSerialID",
                    }
                }
            )
        return skips

    @cached_property
    def django_test_expected_failures(self):
        # DB::Exception: Cannot convert column 'height' from nullable type Nullable(UInt32) to non-nullable type UInt32.
        # Please specify `DEFAULT` expression in ALTER MODIFY COLUMN statement.
        # A bug in ClickHouse 25.11, they force a `DEFAULT` expression when alter column type from nullable to non-nullable.
        if self.connection.get_database_version() >= (25, 11):
            return {
                "schema.tests.SchemaTests.test_alter_null_to_not_null_keeping_default",
                "schema.tests.SchemaTests.test_alter",
            }
        return set()
