# -*- coding: utf-8 -*-
import codecs
import decimal
import time
import json
import os

import pytest
from hamcrest import any_of, assert_that, calling, equal_to, is_, none, raises, less_than, has_length, not_
from tornado import ioloop, gen
from google.protobuf import text_format
import logging

from datetime import date, datetime
from ydb.public.api.protos import ydb_table_pb2
from ydb.public.api.protos import ydb_scheme_pb2
from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.oss.ydb_sdk_import import ydb
from ydb.public.api.protos import ydb_status_codes_pb2


logger = logging.getLogger(__name__)


@gen.coroutine
def execute_in_transaction(table_client, statements):
    session = yield table_client.session().async_create()

    tx = yield session.transaction().async_begin()

    for statement in statements:
        try:
            yield tx.async_execute(statement)
        except ydb.Aborted:
            continue

    try:

        yield tx.async_commit()

    finally:
        yield session.async_delete()


def wrap(select, result):
    return select, result


SIMPLE_TABLE_PROFILE_SETTINGS = """preset_name: "some_preset"
compaction_policy {
  preset_name: "some_compaction_policy"
}
partitioning_policy {
  preset_name: "best_partitioning"
  uniform_partitions: 32
  auto_partitioning: AUTO_SPLIT
}
replication_policy {
  preset_name: "replication!"
  create_per_availability_zone: ENABLED
  allow_promotion: ENABLED
}
execution_policy {
  preset_name: "execution!"
}
caching_policy {
  preset_name: "super_caching"
}
storage_policy {
  preset_name: "my storage preset"
  syslog {
    media: "ssd"
  }
  log {
    media: "ssd"
  }
  data {
    media: "ssd"
  }
  external {
    media: "hdd"
  }
  keep_in_memory: ENABLED
}
"""


class Base(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = kikimr_cluster_factory(
            KikimrConfigGenerator(
                additional_log_configs={
                    'TENANT_POOL': LogLevels.DEBUG,
                    'KQP_PROXY': LogLevels.DEBUG
                }
            )
        )
        cls.cluster.start()

        cls.database_name = "/Root"
        cls.connection_params = ydb.DriverConfig("localhost:%d" % cls.cluster.nodes[1].port)
        cls.connection_params.set_database(cls.database_name)
        cls.driver = ydb.Driver(cls.connection_params)
        cls.driver.wait()

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'cluster'):
            cls.cluster.stop()

        if hasattr(cls, 'driver'):
            cls.driver.stop()


class WithTenant(Base):
    @classmethod
    def setup_class(cls):
        cls.cluster = kikimr_cluster_factory()
        cls.cluster.start()
        cls.database_name = "/Root/tenant"
        cls.cluster.create_database(
            cls.database_name,
            storage_pool_units_count={'hdd': 1}
        )
        cls.cluster.register_and_start_slots(cls.database_name, count=1)
        cls.cluster.wait_tenant_up(cls.database_name)
        cls.connection_params = ydb.DriverConfig(endpoint="localhost:%d" % cls.cluster.nodes[1].port, database=cls.database_name)
        cls.driver = ydb.Driver(cls.connection_params)
        cls.driver.wait()

    @classmethod
    def teardown_class(cls):
        cls.cluster.remove_database(cls.database_name)
        super(WithTenant, cls).teardown_class()


class TestExplain(Base):
    def test_explain_data_query(self):
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())
        explain_response = session.explain('select 1 as cnt;')
        assert_that(
            explain_response.query_ast,
            is_(
                not_(
                    none()
                )
            )
        )


class TestCRUDOperations(Base):
    def test_prepared_query_pipeline(self):
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())

        query = session.prepare('select 1 as cnt;')

        res = session.transaction().execute(query, commit_tx=True)
        assert_that(res[0].rows[0].cnt, is_(1))

        yet_another_prepared = session.prepare(
            'declare $key as Int32; select $key as cnt;'
        )

        res = session.transaction().execute(yet_another_prepared, {'$key': 5}, commit_tx=True)
        assert_that(res[0].rows[0].cnt, is_(5))

    def test_scheme_client_ops(self):
        # useful method that waits for operation and verifies it is done
        # let's create a directory
        self.driver.scheme_client.make_directory(
            '{}/test_scheme_client_ops'.format(self.database_name)
        )

        # let's try to describe created directory
        response = self.driver.scheme_client.list_directory(
            '{}/test_scheme_client_ops'.format(self.database_name))
        # make sure name is right
        assert_that(response.name, is_('test_scheme_client_ops'))

        table_description = ydb.TableDescription()
        table_description = table_description.with_primary_key('key')
        table_description = table_description.with_columns(
            ydb.Column('key', ydb.OptionalType(ydb.PrimitiveType.Int32)),
            ydb.Column('value', ydb.OptionalType(ydb.PrimitiveType.Utf8))
        )

        # let's create table
        self.driver.table_client.session().create().create_table(
            '{}/test_scheme_client_ops/test_scheme_client_ops'.format(self.database_name),
            table_description
        )

        ioloop.IOLoop.current().run_sync(
            lambda: execute_in_transaction(
                self.driver.table_client,
                [
                    'select 1 as cnt;',
                    'select 2 as cnt;',
                    'upsert into `test_scheme_client_ops/test_scheme_client_ops` '
                    '(key, value) values (1, "5");'
                ]
            )
        )

        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())
        # now let's describe the table
        response = session.describe_table(
            '{}/test_scheme_client_ops/test_scheme_client_ops'.format(self.database_name))

        pk = [pk_e for pk_e in response.primary_key]
        assert_that(response.is_table(), is_(True))

        for col in response.columns:
            assert_that(
                isinstance(col.type, ydb.OptionalType), is_(
                    True
                )
            )

        assert_that(
            sorted([str(col.type) for col in response.columns]),
            equal_to(
                ['Int32?', 'Utf8?'],
            )
        )

        # primary key has single column
        assert_that(pk, is_(['key']))

        # and table name is valid
        response = self.driver.scheme_client.describe_path(
            '{}/test_scheme_client_ops/test_scheme_client_ops'.format(self.database_name))
        assert_that(response.name, is_('test_scheme_client_ops'))

        session.alter_table(
            '{}/test_scheme_client_ops/test_scheme_client_ops'.format(self.database_name),
            add_columns=(
                ydb.Column(
                    'nvalue',
                    ydb.OptionalType(ydb.PrimitiveType.String),
                ),
            ),
            drop_columns=[
                'value'
            ]
        )

        session.drop_table('{}/test_scheme_client_ops/test_scheme_client_ops'.format(self.database_name))
        self.driver.scheme_client.remove_directory('{}/test_scheme_client_ops'.format(self.database_name))

    def test_scheme_operation_errors_handle(self):
        def callee():
            session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())
            table_description = ydb.TableDescription()
            table_description = table_description.with_columns(
                ydb.Column('key', ydb.OptionalType(ydb.PrimitiveType.Int32)),
                ydb.Column('value', ydb.OptionalType(ydb.PrimitiveType.Utf8))
            )
            session.create_table(
                '{}/test_scheme_client_ops/test_scheme_client_ops'.format(self.database_name),
                table_description
            )

        assert_that(
            callee, raises(
                ydb.Error
            )
        )

    def test_none_values(self):
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())
        prepared_st = session.prepare(
            'declare $value as String?;'
            'select $value as value;'
        )

        result_sets = session.transaction().execute(prepared_st, {'$value': None})

        assert_that(
            result_sets[0].rows[0].value, is_(
                none()
            )
        )

    def test_parse_list_type(self):
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())
        prepared = session.prepare(
            "declare $value as List<Int32?>; "
            "select $value as cnt; "
        )

        result_sets = session.transaction().execute(
            prepared, {'$value': (1, None)}
        )

        assert_that(
            result_sets[0].rows[0].cnt, is_(
                [1, None]
            )
        )
        session.delete()

    def test_parse_tuple(self):
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())

        result_sets = session.transaction().execute(
            'select AsTuple(cast(1 as Int32), cast(Null as Int32)) as cnt;',
            commit_tx=True
        )

        assert_that(
            result_sets[0].rows[0].cnt, is_(
                (1, None)
            )
        )

        session.delete()

    def test_dict_type(self):
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())

        result_sets = session.transaction().execute(
            'select AsDict(AsTuple(Utf8("a"), 1), AsTuple(Utf8("b"), 2)) as `dict`;',
            commit_tx=True,
        )

        assert_that(
            result_sets[0].rows[0].dict, is_(
                {'a': 1, 'b': 2}
            )
        )

    def test_struct_type(self):
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())

        result_sets = session.transaction().execute(
            'select AsStruct(1 as a, 2 as b, "3" as c) as `struct`;',
            commit_tx=True,
        )
        st = result_sets[0].rows[0].struct
        assert_that(
            st.a + st.b, is_(
                3
            )
        )
        assert_that(
            st.c, is_(
                b"3"
            )
        )

    def test_data_types(self):
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())

        for select, expected in (
            ('Int8("5")', 5),
            ('Uint8("5")', 5),
            ('Int16("5")', 5),
            ('Uint16("5")', 5),
            ('Int32("-1")', -1),
            ('Uint32("2")', 2),
            ('Int64("-3")', -3),
            ('Uint64("4")', 4),
            ('Float("-5")', -5),
            ('Double("6")', 6),
        ):

            result_rows = session.transaction().execute(
                'SELECT %s as tv;' % select,
                commit_tx=True
            )
            num_value = abs(result_rows[0].rows[0].tv - expected)
            assert_that(num_value, less_than(0.00001))

        for select, expected in (
                ('Bool("true")', True),
                ('String("foo")', b'foo'),
                ('Utf8("привет")', u'привет'),
                ('Yson("<a=1>[3;%false]")', b'<a=1>[3;%false]'),
                ('Json(@@{"a":1,"b":null}@@)', '{"a":1,"b":null}'),
                ('Date("2017-11-27")', 17497),
                ('Datetime("2017-11-27T13:24:00Z")', 1511789040),
                ('Timestamp("2017-11-27T13:24:00.123456Z")', 1511789040123456),
                ('Interval("P1DT2H3M4.567890S")', 93784567890)
        ):
            result_rows = session.transaction().execute(
                'SELECT %s as tv;' % select, commit_tx=True)
            assert_that(result_rows[0].rows[0].tv, equal_to(expected))

    def test_struct_type_parameter(self):
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())

        prepared = session.prepare(
            "declare $Input AS List<Struct<Key:Uint64, Value:String>>;"
            "select $Input as list_input;",
        )

        class KV(object):
            def __init__(self, Key, Value):
                self.Key = Key
                self.Value = Value

        result_set = session.transaction().execute(
            prepared,
            {
                '$Input': (
                    KV(4, b"four"),
                    KV(5, b"five"),
                )
            }
        )
        assert_that(
            result_set[0].rows[0].list_input[0].Key + result_set[0].rows[0].list_input[1].Key,
            is_(
                9
            )
        )

    def test_bulk_prepared_insert_many_values(self):
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())

        session.execute_scheme(
            'create table `test_bulk_prepared_insert_many_values` '
            '(key Int64, value Utf8, primary key(key)); '
        )

        prepared = session.prepare("""
declare $Input AS List<Struct<key:Int64, value:Utf8>>;

$InputSource = (
    SELECT
        Item.key AS key,
        Item.value AS value
    FROM (SELECT $Input AS lst)
    FLATTEN BY lst AS Item
);

INSERT INTO `test_bulk_prepared_insert_many_values`
SELECT * FROM $InputSource;
""")

        class Kv(object):
            def __init__(self, key, value):
                self.key = key
                self.value = value

        with session.transaction() as tx:
            tx.execute(
                prepared, {
                    '$Input': [
                        Kv(idx, str(idx))
                        for idx in range(50)
                    ]
                },
                commit_tx=True
            )

        result_sets = session.transaction().execute(
            'SELECT COUNT(*) as cnt FROM `test_bulk_prepared_insert_many_values`;', commit_tx=True)

        assert_that(
            result_sets[0].rows[0].cnt, is_(
                50
            )
        )

    def test_bulk_upsert(self):
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())

        session.execute_scheme(
            ' create table `test_bulk_upsert` '
            ' (k Uint64, a Utf8, b Utf8, primary key(k)); '
        )

        class Rec(object):
            __slots__ = ('k', 'a', 'b')

            def __init__(self, k, a, b=None):
                self.k = k
                self.a = a
                self.b = b

        table_client = self.driver.table_client
        table_path = os.path.join(self.database_name, 'test_bulk_upsert')
        rows = [Rec(i, str(i), 'b') for i in range(10)]
        column_types = ydb.BulkUpsertColumns()\
            .add_column('k', ydb.PrimitiveType.Uint64)\
            .add_column('a', ydb.PrimitiveType.Utf8)\
            .add_column('b', ydb.PrimitiveType.Utf8)
        table_client.bulk_upsert(table_path, rows, column_types)

        result_sets = session.transaction().execute(
            'SELECT COUNT(*) as cnt FROM test_bulk_upsert;', commit_tx=True
        )

        assert_that(
            result_sets[0].rows[0].cnt, is_(
                10
            )
        )

        # performing bulk upsert on 2 out of 3 columns
        column_types = ydb.BulkUpsertColumns()\
            .add_column('k', ydb.PrimitiveType.Uint64)\
            .add_column('a', ydb.PrimitiveType.Utf8)
        rows = [Rec(i, str(i)) for i in range(10)]
        table_client.bulk_upsert(table_path, rows, column_types)

        result_sets = session.transaction().execute(
            'SELECT * FROM test_bulk_upsert LIMIT 1;', commit_tx=True
        )

        # check that column b wasn't updated
        assert_that(
            result_sets[0].rows[0].b, is_(
                'b'
            )
        )

    def test_all_enums_are_presented_as_exceptions(self):
        status_ids = ydb_status_codes_pb2.StatusIds
        for name, value in status_ids.StatusCode.items():
            if name == 'STATUS_CODE_UNSPECIFIED' or name == 'SUCCESS':
                continue

            exc = ydb.issues._server_side_error_map.get(value)
            assert_that(
                exc,
                is_(not_(none())),
                "%s status is missing" % name,
            )

            exc = ydb.issues._server_side_error_map.get(value)
            assert_that(
                exc.status,
                equal_to(
                    value
                )
            )

    def test_type_builders_str_methods(self):

        for builder, representation in (
            (
                ydb.OptionalType(ydb.ListType(ydb.PrimitiveType.Int32)),
                "List<Int32>?"
            ),
            (
                ydb.ListType(ydb.DictType(ydb.PrimitiveType.Int32, ydb.PrimitiveType.String)),
                "List<Dict<Int32,String>>"
            ),
            (
                ydb.TupleType()
                .add_element(ydb.PrimitiveType.Int32)
                .add_element(ydb.PrimitiveType.String),
                "Tuple<Int32,String>"
            ),
            (
                ydb.StructType()
                .add_member("Student", ydb.PrimitiveType.String)
                .add_member("Year", ydb.PrimitiveType.Int32),
                "Struct<Student:String,Year:Int32>"
            ),
        ):
            assert_that(
                str(builder),
                equal_to(
                    representation
                )
            )

    def test_create_and_delete_session_then_use_it_again(self):
        initial_session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())
        session_id = initial_session.session_id
        initial_session.delete()

        def invalid_callable():
            initial_session.transaction().execute(
                'select 1 as cnt;',
                commit_tx=True
            )

        assert_that(
            invalid_callable,
            raises(
                ydb.BadSession,
                "Empty session_id"
            )
        )

        # just to test right
        initial_session._state.set_id(session_id)

        assert_that(
            invalid_callable,
            raises(
                ydb.BadSession,
            )
        )

    def test_locks_invalidated_error(self):
        first_session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())
        second_session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())

        first_session.execute_scheme("create table kv_table (key Int32, value Utf8, primary key(key));")
        second_session.transaction().execute('upsert into kv_table (key, value) VALUES (1, "4");', commit_tx=True)

        first_session_tx = first_session.transaction()
        first_session_tx.execute('select value from kv_table where key=1;')
        second_session.transaction().execute('upsert into kv_table (key, value) VALUES (1, "5");', commit_tx=True)

        def invalid_callable():
            first_session_tx.execute('upsert into kv_table (key, value) VALUES (2, "3");', commit_tx=True)

        assert_that(
            invalid_callable,
            raises(
                ydb.Aborted
            )
        )

    def test_tcl(self):

        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())

        with session.transaction().begin() as tx:
            tx.execute('select 1 as cnt;')

            tx.commit()

            assert_that(
                tx._tx_state.tx_id,
                is_(
                    none()
                )
            )

    def test_tcl_2(self):
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())

        with session.transaction().begin() as tx:
            tx.execute('select 1 as cnt;', commit_tx=True)

            assert_that(
                tx._tx_state.tx_id,
                is_(
                    none()
                )
            )

        session.delete()

    def test_tcl_3(self):

        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())

        with session.transaction().begin() as tx:

            tx.execute('select 1 as cnt;')

            tx.rollback()

            assert_that(
                tx._tx_state.tx_id,
                is_(
                    none()
                )
            )

    def test_reuse_session_to_tx_leak(self):
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())
        for _ in range(100):
            with session.transaction() as tx:
                tx.execute('select 1', commit_tx=True)

    def test_direct_leak_tx_but_no_actual_leak_by_best_efforts(self):
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())
        for _ in range(11):
            with session.transaction().begin() as tx:
                tx.execute('select 1')

    def test_presented_in_cache(self):
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())

        session.prepare('select 1 as cnt;')
        assert_that(
            session.has_prepared('select 1 as cnt;'),
            equal_to(
                True
            )
        )

        session.transaction().execute('select 1 as cnt;')

    def test_decimal_values_negative_stories(self):
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())
        select_decimal_query = "declare $value as Decimal(22,9); select $value as value;"
        strings_to_validate = (
            '0.12345678910',
            '0.1111111111',
        )
        session.prepare(select_decimal_query)

        def extract_single_value(result_sets):
            return result_sets[0].rows[0].value

        for value_repr in strings_to_validate:
            def callee():
                with session.transaction() as tx:
                    return extract_single_value(
                        tx.execute(
                            select_decimal_query, commit_tx=True,
                            parameters={
                                '$value': decimal.Decimal(value_repr)
                            }
                        )
                    )

            assert_that(
                callee,
                raises(
                    ydb.GenericError,
                )
            )

    def test_decimal_values(self):
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())
        select_decimal_query = "declare $value as Decimal(22,9); select $value as value;"

        def extract_single_value(result_sets):
            return result_sets[0].rows[0].value

        strings_to_validate = (
            '1', '1.34', '100000000.134', 'Nan', 'Inf', '-Inf', '1000000434.234',
            '42', '0', '-112323.2323',
            '0.1',
            '0.11',
            '0.111',
            '0.1111',
            '0.11111',
            '0.111111',
            '0.1111111',
            '0.11111111',
            '0.111111111',
        )
        session.prepare(select_decimal_query)
        for value_repr in strings_to_validate:
            with session.transaction().begin() as tx:
                r_sets = tx.execute(select_decimal_query, {'$value': decimal.Decimal(value_repr)}, commit_tx=True)
                actual_value = extract_single_value(r_sets)
                expected_value = decimal.Decimal(value_repr)
                if expected_value.is_nan():
                    assert_that(
                        actual_value.is_nan(),
                        equal_to(
                            True
                        )
                    )

                else:
                    assert_that(
                        actual_value,
                        equal_to(
                            expected_value
                        )
                    )

    def test_list_directory_with_children(self):
        working_directory = "{}/test_list_directory_with_children".format(self.database_name)
        self.driver.scheme_client.make_directory(working_directory)
        items_count = 5
        for idx in range(items_count):
            self.driver.scheme_client.make_directory("{}/{}".format(working_directory, str(idx)))

        description = self.driver.scheme_client.list_directory(working_directory)
        assert_that(description.is_directory(), is_(True))
        assert_that(description.is_table(), is_(False))
        assert_that(description.children, has_length(items_count))
        assert_that(description.owner, equal_to("root@builtin"))
        assert_that(description.name, equal_to('test_list_directory_with_children'))
        assert_that(description.is_directory(), is_(True))

    def test_validate_describe_path_result(self):
        self.driver.scheme_client.make_directory("{}/test_validate_describe_path_result".format(self.database_name))
        response = self.driver.scheme_client.describe_path("{}/test_validate_describe_path_result".format(self.database_name))
        assert_that(response.owner, equal_to("root@builtin"))
        assert_that(response.name, equal_to('test_validate_describe_path_result'))
        assert_that(response.is_directory(), is_(True))

    def test_acl_modifications_1(self):
        modify_permissions = ydb.ModifyPermissionsSettings()
        modify_permissions.change_owner("gvit@staff")
        modify_permissions.grant_permissions('gvit@staff', ('full', 'use'))
        expected_req = """actions {
  change_owner: "gvit@staff"
}
actions {
  grant {
    subject: "gvit@staff"
    permission_names: "full"
    permission_names: "use"
  }
}
"""
        message = ydb_scheme_pb2.ModifyPermissionsRequest()
        text_format.Parse(expected_req, message)
        assert_that(
            modify_permissions.to_pb(),
            equal_to(
                message
            )
        )

    def test_acl_modification_2(self):
        modify_permissions = ydb.ModifyPermissionsSettings()
        modify_permissions.change_owner("gvit@staff")
        modify_permissions.revoke_permissions('gvit@staff', ('full', 'use'))
        expected_req = """actions {
  change_owner: "gvit@staff"
}
actions {
  revoke {
    subject: "gvit@staff"
    permission_names: "full"
    permission_names: "use"
  }
}
"""
        message = ydb_scheme_pb2.ModifyPermissionsRequest()
        text_format.Parse(expected_req, message)
        assert_that(
            modify_permissions.to_pb(),
            equal_to(
                message
            )
        )

    def test_can_execute_valid_statement_after_invalid_success(self):
        # Arrange
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())

        def invalid_callee():
            session.execute_scheme('CREATE TABLE `/Root/SomeName` (id integer);')

        assert_that(
            invalid_callee,
            raises(
                ydb.GenericError
            )
        )

        # Act + Assert no error
        session.execute_scheme('create table `/Root/SomeName` (id integer, primary key(id));')
        session.execute_scheme('drop table `/Root/SomeName`;')

        session.delete()

    def test_modify_permissions_3(self):
        modify_permissions = ydb.ModifyPermissionsSettings()
        modify_permissions.change_owner("gvit@staff")
        modify_permissions.set_permissions('gvit@staff', ('full', 'use'))
        expected_req = """actions {
  change_owner: "gvit@staff"
}
actions {
  set {
    subject: "gvit@staff"
    permission_names: "full"
    permission_names: "use"
  }
}
"""
        message = ydb_scheme_pb2.ModifyPermissionsRequest()
        text_format.Parse(expected_req, message)
        assert_that(
            modify_permissions.to_pb(),
            equal_to(
                message
            )
        )

    def test_directory_that_doesnt_exists(self):
        def callee():
            self.driver.scheme_client.describe_path("{}/directory_that_doesnt_exists".format(self.database_name))

        assert_that(
            callee,
            raises(
                ydb.SchemeError,
            )
        )

    def test_crud_acl_actions(self):
        directory_name = "/Root/test_crud_acl_actions"
        self.driver.scheme_client.make_directory(directory_name)
        self.driver.scheme_client.modify_permissions(
            directory_name,
            ydb.ModifyPermissionsSettings()
            .grant_permissions(
                'gvit@staff', (
                    'ydb.generic.read',
                )
            )
        )

        response = self.driver.scheme_client.describe_path(directory_name)
        subjects = set([permission.subject for permission in response.effective_permissions])
        assert "gvit@staff" in subjects

    def test_too_many_pending_transactions(self):
        driver = ydb.Driver(self.connection_params)
        driver.wait()
        session = ydb.retry_operation_sync(lambda: driver.table_client.session().create())

        txs = []
        for _ in range(10):
            txs.append(
                session.transaction().begin()
            )

        def callee():
            session.transaction().begin()

        assert_that(
            callee,
            raises(
                ydb.BadSession,
                'Exceeded maximum allowed number of active transactions'
            )
        )

        try:
            callee()
        except ydb.BadSession as expected_error:
            assert_that(
                isinstance(expected_error.message, str),
                is_(True)
            )

    def test_query_set1(self):
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())
        query_set = [
            # Tests for Int32
            wrap(
                "select cast(1 as Int32) as x;", [{'x': 1}]
            ),
            wrap(
                "select cast(2147483647 as Int32) as x;", [{'x': 2147483647}]
            ),
            wrap(
                "select cast(2147483648 as Int32) as x;", [{'x': None}]
            ),
            wrap(
                "select cast(2147483649 as Int32) as x;", [{'x': None}]
            ),

            # Tests for UInt32
            wrap(
                "select cast(1 as UInt32) as x;", [{'x': 1}]
            ),
            wrap(
                "select cast(2147483647 as UInt32) as x;", [{'x': 2147483647}]
            ),
            wrap(
                "select cast(2147483648 as UInt32) as x;", [{'x': 2147483648}]
            ),
            wrap(
                "select cast(2147483649 as UInt32) as x;", [{'x': 2147483649}]
            ),
            wrap(
                "select cast(4294967295 as UInt32) as x;", [{'x': 4294967295}]
            ),
            wrap(
                "select cast(4294967296 as UInt32) as x;", [{'x': None}]
            ),
            wrap(
                "select cast(-1 as UInt32) as x;", [{'x': None}]
            ),

            # Tests for Uint8
            wrap(
                "select cast(1 as Uint8) as x;", [{'x': 1}]
            ),
            wrap(
                "select cast(-1 as Uint8) as x;", [{'x': None}]
            ),
            wrap(
                "select cast(300 as Uint8) as x;", [{'x': None}]
            ),

            # Tests for Bool
            wrap(
                "select cast(1 as bool) as x;", [{'x': True}]
            ),
            wrap(
                "select cast(0 as bool) as x;", [{'x': False}]
            ),
            wrap(
                "select cast(3 as bool) as x;", [{'x': True}]
            ),
            wrap(
                "select cast(-123423 as bool) as x;", [{'x': True}]
            ),
            wrap(
                "select cast(4294967296 as bool) as x;", [{'x': True}]
            ),
            wrap(
                "select cast('' as bool) as x;", [{'x': None}]
            ),
            wrap(
                "select cast('1' as bool) as x;", [{'x': None}]
            ),
            wrap(
                "select cast('0' as bool) as x;", [{'x': None}]
            ),
            wrap(
                "select cast('qwe' as bool) as x;", [{'x': None}]
            ),

            # Tests for strings/utf8/etc
            wrap(
                "select '' as two", [{'two': b''}]
            ),
            wrap(
                "select 'йцукен' as x;",
                [
                    {'x': codecs.encode(u'йцукен', 'utf8')}
                ]
            ),
            wrap(
                "select cast('йцукен' as utf8) as x;",
                [
                    {'x': u'йцукен'},
                ]
            ),
        ]
        for select, result_set in query_set:
            with session.transaction() as tx:
                actual_result_sets = tx.execute(select, commit_tx=True)

            assert_that(actual_result_sets[0].rows, is_(result_set))
            assert_that(
                actual_result_sets[0].truncated,
                is_(
                    False
                )
            )

    def test_queries_set2(self):
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())
        query_set = [
            wrap("select 3*7 + 19 as s", [{'s': 40}]),
            wrap("select 3*7 + 19 as s union all select 5 as s", [{'s': 40}, {'s': 5}]),
            wrap("select null", [{'column0': None}]),
            wrap("select null as x", [{'x': None}]),
            wrap("select 'dsfgd'", [{'column0': b'dsfgd'}]),
            wrap("select x from (select 1 as x) where x = 1", [{'x': 1}]),
            wrap("select x from (select 1 as x) where x is null", []),
            wrap("select x from (select null as x) where x is null", [{'x': None}]),
            wrap("select x from (select 1 as x) where x == 1", [{'x': 1}]),
            wrap("select count(*) as c from (select 1 as x) where x == 1", [{'c': 1}]),
            wrap("select count(*) as c from (select 1 as x) where x == 2", [{'c': 0}]),
            wrap(
                """select k, max(v) as v
                    from (
                        select 1 as k, 'a' as v
                        union all select 2 as k, 'b' as v
                        union all select 2 as k, 'c' as v
                    ) group by k
                    ORDER BY k
                """,
                [{'k': 1, 'v': b'a'}, {'k': 2, 'v': b'c'}]
            ),
            wrap(
                """select k, count(*) as v
                    from (
                        select 1 as k, 'a' as v
                        union all select 2 as k, 'b' as v
                        union all select 2 as k, 'c' as v
                    )
                    group by k
                    ORDER BY k
                """,
                [{'k': 1, 'v': 1}, {'k': 2, 'v': 2}]
            ),
            wrap(
                """select count(*) as x from (select 1 as x union all select 2 as x) group by x;""",
                [{'x': 1}, {'x': 1}]
            ),
            wrap(
                """select a.k as k, a.v as av, b.v as bv from (
                   select 1 as k, 1 as v union all select 2 as k, 2 as v) as a
                   left join ( select 1 as k, 1 as v union all select 2 as k, 2 as v) as b on a.k = b.k
                    ORDER BY k
                """,
                [
                    {'k': 1, 'av': 1, 'bv': 1},
                    {'k': 2, 'av': 2, 'bv': 2},
                ]
            ),
        ]
        for select, result_set in query_set:
            with session.transaction() as tx:
                actual_result_sets = tx.execute(select, commit_tx=True)

            assert_that(actual_result_sets[0].rows, is_(result_set))
            assert_that(
                actual_result_sets[0].truncated,
                is_(
                    False
                )
            )

    def test_when_result_set_is_large_then_issue_occure(self):
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())
        with session.transaction() as tx:
            assert_that(
                calling(tx.execute).with_args(
                    "select t.x from ( {data} select 201 as x ) as t".format(
                        data='\n'.join('select {x} as x union all'.format(x=x) for x in range(2001))
                    )
                ),
                raises(ydb.issues.TruncatedResponseError),
            )


class TestSessionNotFound(Base):
    def test_session_not_found(self):
        driver = ydb.Driver(self.connection_params)
        driver.wait()
        session = ydb.retry_operation_sync(lambda: driver.table_client.session().create())

        self.cluster.nodes[1].stop()
        self.cluster.nodes[1].start()

        def not_found_callee():
            session.transaction().begin()

        assert_that(
            not_found_callee,
            raises(
                ydb.ConnectionError,
            )
        )

        driver.wait()

        assert_that(
            not_found_callee,
            raises(
                ydb.BadSession,
            )
        )


class TestSessionNotFoundOperations(Base):
    def test_session_pool(self):
        pool = ydb.table.SessionPool(self.driver, size=10)
        with pool.checkout() as session:
            session.transaction().execute(
                'select 1',
                commit_tx=True,
            )

    def test_ok_keep_alive_example(self):
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())
        session = session.keep_alive()
        session = session.keep_alive()
        session.delete()

    def test_can_commit_bad_tx(self):
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())
        with session.transaction() as tx:
            try:
                tx.execute('select [/Unknown/]', commit_tx=True)

            except ydb.Error:
                pass

            assert_that(
                lambda: tx.commit(),
                raises(
                    ydb.PreconditionFailed,
                )
            )

            assert_that(
                lambda: tx.commit(),
                raises(
                    ydb.PreconditionFailed,
                )
            )

    def test_cannot_commit_bad_tx(self):
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())
        with session.transaction() as tx:
            try:
                tx.execute('select [/Unknown/]', commit_tx=True)
            except ydb.Error:
                pass

            assert_that(
                lambda: tx.execute('select 1 as cnt', commit_tx=True),
                raises(
                    RuntimeError,  # "Any operation with finished transaction is denied"
                )
            )

            assert_that(
                lambda: tx.commit(),
                raises(
                    ydb.PreconditionFailed,
                )
            )

    def test_commit_successfully_after_success_commit(self):
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())
        with session.transaction() as tx:
            tx.execute('select 1', commit_tx=True)
            tx.commit()
            tx.commit()

    def test_invalid_keep_alive_example(self):
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session())
        assert_that(
            lambda: session.keep_alive(),
            raises(
                ydb.BadSession,
            )
        )

    def test_describe_table_with_bounds(self):

        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())
        session.create_table(
            '/Root/test_describe_table_with_bounds',
            ydb.TableDescription()
            .with_columns(
                ydb.Column('key', ydb.OptionalType(ydb.PrimitiveType.Uint64)),
                ydb.Column('value', ydb.OptionalType(ydb.PrimitiveType.Json))
            )
            .with_primary_keys(
                'key',
            )
            .with_profile(
                ydb.TableProfile()
                .with_partitioning_policy(
                    ydb.PartitioningPolicy()
                    .with_uniform_partitions(4)
                    .with_auto_partitioning(
                        ydb.AutoPartitioningPolicy.AUTO_SPLIT_MERGE,
                    )
                )
            )
        )

        resp = session.describe_table(
            '/Root/test_describe_table_with_bounds',
            ydb.DescribeTableSettings().with_include_shard_key_bounds(
                True
            )
        )

        assert_that(
            len(resp.shard_key_ranges),
            equal_to(
                4
            )
        )

        last_range = None
        for shard_key_range in resp.shard_key_ranges:
            if last_range is not None:
                assert_that(
                    last_range.to_bound.inclusive,
                    equal_to(
                        True
                    )
                )

                assert_that(
                    shard_key_range.from_bound.inclusive,
                    equal_to(
                        False
                    )
                )

    def test_native_datetime_types(self):
        tc_settings = ydb.TableClientSettings().with_native_datetime_in_result_sets(enabled=True)
        table_client = ydb.TableClient(self.driver, tc_settings)
        data_query = ydb.DataQuery('select Datetime("2019-09-16T00:00:00Z") as dt', {})
        session = ydb.retry_operation_sync(lambda: table_client.session().create())
        result_sets = session.transaction().execute(data_query, commit_tx=True)
        assert_that(
            result_sets[0].rows[0].dt,
            equal_to(
                datetime(2019, 9, 16, 0, 0),
            )
        )

    def test_native_date_types(self):
        tc_settings = ydb.TableClientSettings().with_native_date_in_result_sets(enabled=True)
        table_client = ydb.TableClient(self.driver, tc_settings)
        data_query = ydb.DataQuery('select Date("2021-04-14") as dt', {})
        session = ydb.retry_operation_sync(lambda: table_client.session().create())
        result_sets = session.transaction().execute(data_query, commit_tx=True)
        assert_that(
            result_sets[0].rows[0].dt,
            equal_to(
                date(2021, 4, 14),
            )
        )

    def test_keep_in_cache_disabled(self):
        tc_settings = ydb.TableClientSettings().with_client_query_cache(enabled=False)
        table_client = ydb.TableClient(self.driver, tc_settings)
        data_query = ydb.DataQuery('select 1', {})
        session = ydb.retry_operation_sync(lambda: table_client.session().create())
        session.transaction().execute(data_query, commit_tx=True)
        assert_that(session.has_prepared('select 1'), is_(True))
        assert_that(session._state.lookup('select 1')[1], is_(none()))
        assert_that(session.has_prepared(data_query), is_(True))
        assert_that(session._state.lookup(data_query)[1], is_(none()))

    def test_explicit_partitions_case_1(self):

        description = ydb.TableDescription().with_columns(
            ydb.Column('key', ydb.OptionalType(ydb.PrimitiveType.Uint64)),
            ydb.Column('value', ydb.OptionalType(ydb.PrimitiveType.Utf8))).with_primary_key('key')

        description.with_profile(
            ydb.TableProfile()
            .with_partitioning_policy(
                ydb.PartitioningPolicy()
                .with_explicit_partitions(
                    ydb.ExplicitPartitions(
                        (
                            ydb.KeyBound((100, )),
                            ydb.KeyBound((300, )),
                            ydb.KeyBound((400, )),
                        )
                    )
                )
            )
        )

        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())
        session.create_table('/Root/test_explicit_partitions', description)

        response = session.describe_table(
            '/Root/test_explicit_partitions', ydb.DescribeTableSettings().with_include_shard_key_bounds(
                True
            ).with_include_table_stats(
                True
            )
        )

        assert_that(
            len(response.shard_key_ranges),
            equal_to(
                4
            )
        )
        assert_that(
            response.table_stats.partitions,
            equal_to(
                4
            )
        )

    def test_explict_partitions_case_2(self):

        description = (
            ydb.TableDescription()
            .with_primary_keys('key1', 'key2')
            .with_columns(
                ydb.Column('key1', ydb.OptionalType(ydb.PrimitiveType.Uint64)),
                ydb.Column('key2', ydb.OptionalType(ydb.PrimitiveType.Uint64)),
                ydb.Column('value', ydb.OptionalType(ydb.PrimitiveType.Utf8))
            )
            .with_profile(
                ydb.TableProfile()
                .with_partitioning_policy(
                    ydb.PartitioningPolicy()
                    .with_explicit_partitions(
                        ydb.ExplicitPartitions(
                            (
                                ydb.KeyBound((100, )),
                                ydb.KeyBound((300, 100)),
                                ydb.KeyBound((400, )),
                            )
                        )
                    )
                )
            )
        )

        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())
        session.create_table('/Root/test_explicit_partitions', description)

        response = session.describe_table(
            '/Root/test_explicit_partitions', ydb.DescribeTableSettings().with_include_shard_key_bounds(
                True
            ).with_include_table_stats(
                True
            )
        )

        assert_that(
            len(response.shard_key_ranges),
            equal_to(
                4
            )
        )
        assert_that(
            response.table_stats.partitions,
            equal_to(
                4
            )
        )

    def test_simple_table_profile_settings(self):
        description = ydb.TableDescription().with_column(ydb.Column('key', ydb.PrimitiveType.Utf8))
        description.with_profile(
            ydb.TableProfile()
            .with_preset_name('some_preset')
            .with_storage_policy(
                ydb.StoragePolicy()
                .with_preset_name('my storage preset')
                .with_keep_in_memory(ydb.FeatureFlag.ENABLED)
                .with_log_storage_settings(ydb.StoragePool('ssd'))
                .with_external_storage_settings(ydb.StoragePool('hdd'))
                .with_syslog_storage_settings(ydb.StoragePool('ssd'))
                .with_data_storage_settings(ydb.StoragePool('ssd'))
            )
            .with_compaction_policy(
                ydb.CompactionPolicy()
                .with_preset_name('some_compaction_policy')
            )
            .with_caching_policy(
                ydb.CachingPolicy()
                .with_preset_name('super_caching')
            )
            .with_execution_policy(
                ydb.ExecutionPolicy()
                .with_preset_name('execution!')
            )
            .with_replication_policy(
                ydb.ReplicationPolicy()
                .with_preset_name('replication!')
                .with_allow_promotion(ydb.FeatureFlag.ENABLED)
                .with_create_per_availability_zone(ydb.FeatureFlag.ENABLED)
            )
            .with_partitioning_policy(
                ydb.PartitioningPolicy()
                .with_preset_name('best_partitioning')
                .with_uniform_partitions(32)
                .with_auto_partitioning(ydb.AutoPartitioningPolicy.AUTO_SPLIT)
            )
        )
        expected_table_profile = ydb_table_pb2.TableProfile()
        text_format.Parse(SIMPLE_TABLE_PROFILE_SETTINGS, expected_table_profile)
        assert_that(
            description.profile.to_pb(description),
            equal_to(
                expected_table_profile,
            )
        )


class TestBadSession(Base):
    def test_simple(self):
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())

        self.cluster.nodes[1].stop()

        assert_that(
            lambda: self.driver.table_client.session().create(),
            raises(
                ydb.ConnectionError,
            )
        )

        self.cluster.nodes[1].start()
        assert_that(
            lambda: ydb.retry_operation_sync(lambda: session.keep_alive()),
            raises(
                ydb.BadSession,
            )
        )


class TestDriverCanRecover(Base):
    def test_driver_recovery(self):
        self.cluster.nodes[1].stop()

        assert_that(
            lambda: self.driver.table_client.session().create(),
            raises(
                ydb.ConnectionError,
            )
        )

        self.cluster.nodes[1].start()
        ydb.retry_operation_sync(
            lambda: self.driver.table_client.session().create()
        )


class TestSelectAfterDropWithRepetitions(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = kikimr_cluster_factory(
            configurator=KikimrConfigGenerator(
                additional_log_configs={
                    'TX_PROXY': LogLevels.DEBUG,
                    'TX_PROXY_SCHEME_CACHE': LogLevels.DEBUG,
                    'FLAT_TX_SCHEMESHARD': LogLevels.DEBUG,
                }
            )
        )
        cls.cluster.start()
        cls.driver = ydb.Driver(ydb.DriverConfig(
            database="/Root",
            endpoint="%s:%s" % (cls.cluster.nodes[1].host, cls.cluster.nodes[1].port)))
        cls.driver.wait()

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'cluster'):
            cls.cluster.stop()

    @pytest.mark.parametrize(['repetitions'], [(10,)])
    def test_select_on_dropped_table_unsuccessful(self, repetitions):
        for idx in range(repetitions):
            table_name = "/Root/test_select_on_dropped_table_unsuccessful_%d" % idx
            session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())
            session.create_table(
                table_name,
                ydb.TableDescription()
                .with_columns(
                    ydb.Column('id', ydb.OptionalType(ydb.PrimitiveType.Uint64)),
                    ydb.Column('name', ydb.OptionalType(ydb.PrimitiveType.String)),
                )
                .with_primary_key(
                    'id',
                )
            )

            with session.transaction() as tx:
                for key, value in [(2, "Vitalii"), (3, "Alex"), (4, "Pavel"), (5, "Phil"), (6, "Artem")]:
                    tx.execute(
                        'upsert into `%s` (id, name) values (%d, "%s")' % (
                            table_name, key, value
                        )
                    )

                tx.commit()

            session.execute_scheme(
                'drop table `%s`' % table_name
            )

            def callee():
                with session.transaction() as fx:
                    fx.execute(
                        "select id from `{}` ORDER BY id LIMIT 1".format(table_name),
                        commit_tx=True,
                    )

            assert_that(
                callee,
                raises(
                    ydb.SchemeError,
                )
            )


class TestMetaDataInvalidation(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = kikimr_cluster_factory()
        cls.cluster.start()
        cls.driver = ydb.Driver(ydb.DriverConfig(
            database="/Root",
            endpoint="%s:%d" % (cls.cluster.nodes[1].host, cls.cluster.nodes[1].port)))
        cls.driver.wait()

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'cluster'):
            cls.cluster.stop()

    def test_invalidation_success(self):
        # Act: creating table, inserting data, dropping table
        table_name = "/Root/test_invalidation_success"
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())
        session.execute_scheme(
            "create table `%s` (entryId int, name string, data utf8, primary key(entryId));" % table_name)
        with session.transaction() as tx:
            for entryId, name, data in [(1, "one", 'what is going on'), (2, 'two', 'help')]:
                tx.execute(
                    'upsert into `%s` (entryId, name, data) values (%d, "%s", "%s")' % (
                        table_name, entryId, name, data
                    )
                )
            tx.commit()

        session.execute_scheme("drop table `%s`" % table_name)

        # Act: recreating table with new schema, with retries
        retries = 10
        while True:
            if retries == 0:
                raise RuntimeError("Unable to create table in time")
            retries -= 1
            try:
                session.execute_scheme("create table `%s` (rowId int, name utf8, primary key(rowId));" % table_name)
                break
            except ydb.Error:
                time.sleep(5)

        # Act: inserting data
        with session.transaction() as tx:
            for rowId, name in [(3, "three")]:
                tx.execute(
                    'upsert into `%s` (rowId, name) values (%d, "%s")' % (
                        table_name, rowId, name
                    )
                )
            tx.commit()

        # Assert: simple select
        result_sets = session.transaction().execute('select * from `%s`' % table_name, commit_tx=True)
        assert_that(result_sets[0].rows, equal_to([{'name': 'three', 'rowId': 3}]))

        # Assert failure: Member not found!
        def invalid_callee1():
            session.transaction().execute(
                'select entryId from `%s`' % table_name, commit_tx=True)

        assert_that(
            invalid_callee1,
            raises(
                ydb.GenericError,
            )
        )

        # Assert failure: insert data with wrong column
        def invalid_callee2():
            session.transaction().execute(
                'upsert into `%s` (rowId, name, data) values (%d, "%s", "%s")' % (
                    table_name, 4, "four", "some wrong col"
                )
            )

        assert_that(
            invalid_callee2,
            raises(
                ydb.GenericError,
            )
        )


class TestJsonExample(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = kikimr_cluster_factory()
        cls.cluster.start()
        cls.driver = ydb.Driver(ydb.DriverConfig(
            database="/Root",
            endpoint="%s:%d" % (cls.cluster.nodes[1].host, cls.cluster.nodes[1].port)))
        cls.driver.wait()

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'cluster'):
            cls.cluster.stop()

        if hasattr(cls, 'driver'):
            cls.driver.stop()

    def test_json_unexpected_failure(self):
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())
        session.create_table(
            '/Root/test_json_unexpected_failure',
            ydb.TableDescription()
            .with_columns(
                ydb.Column('key', ydb.OptionalType(ydb.PrimitiveType.Utf8)),
                ydb.Column('value', ydb.OptionalType(ydb.PrimitiveType.Json))
            )
            .with_primary_keys(
                'key'
            )
        )

        prepared = session.prepare(
            'DECLARE $key as Utf8; \n'
            'DECLARE $value as Json; \n'
            'UPSERT INTO `/Root/test_json_unexpected_failure` (key, value) VALUES ($key, $value)')

        def callee():
            session.transaction().execute(
                prepared,
                commit_tx=True,
                parameters={
                    '$key': 'one',
                    '$value': str(
                        {'one': 1},
                    )
                }
            )

        assert_that(
            callee,
            raises(
                ydb.BadRequest
            )
        )

    def test_json_success(self):
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())
        session.create_table(
            '/Root/test_json_success',
            ydb.TableDescription()
            .with_columns(
                ydb.Column('key', ydb.OptionalType(ydb.PrimitiveType.Utf8)),
                ydb.Column('value', ydb.OptionalType(ydb.PrimitiveType.Json))
            )
            .with_primary_keys(
                'key'
            )
        )

        prepared = session.prepare(
            'DECLARE $key as Utf8; \n'
            'DECLARE $value as Json; \n'
            'UPSERT INTO `/Root/test_json_success` (key, value) VALUES ($key, $value)')
        session.transaction().execute(
            prepared,
            commit_tx=True,
            parameters={
                '$key': 'one',
                '$value': json.dumps(
                    {'one': 1},
                )
            }
        )


class TestForPotentialDeadlock(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = kikimr_cluster_factory()
        cls.cluster.start()

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'cluster'):
            cls.cluster.stop()

    def test_deadlocked_threads_on_cleanup(self):
        driver = ydb.Driver(
            ydb.DriverConfig(
                database="/Root",
                endpoint="%s:%d" % (self.cluster.nodes[1].host, self.cluster.nodes[1].port)
            )
        )
        driver.wait()
        connection = driver._store.get()
        driver.stop()
        connection.close()


class TestRecursiveCreation(Base):
    def test_mkdir(self):
        path = '{}/test_recursive_mkdir/a/b/dir'.format(self.database_name)

        self.driver.scheme_client.make_directory(path)
        response = self.driver.scheme_client.describe_path(path)
        assert_that(response.name, is_(os.path.basename(path)))

    def test_create_table(self):
        path = '{}/test_recursive_create_table/a/b/table'.format(self.database_name)

        self.driver.table_client.session().create().create_table(
            path,
            ydb.TableDescription()
            .with_columns(
                ydb.Column('key', ydb.OptionalType(ydb.PrimitiveType.Int32)),
                ydb.Column('value', ydb.OptionalType(ydb.PrimitiveType.Utf8))
            )
            .with_primary_key('key')
        )
        response = self.driver.scheme_client.describe_path(path)
        assert_that(response.name, is_(os.path.basename(path)))


class TestAttributes(WithTenant):
    ATTRIBUTES = {
        'one': '1',
        'two': 'two',
    }

    def test_create_table(self):
        path = '{}/test_attributes/create_table/table'.format(self.database_name)
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())
        session.create_table(
            path,
            ydb.TableDescription()
            .with_columns(
                ydb.Column('key', ydb.OptionalType(ydb.PrimitiveType.Int32)),
                ydb.Column('value', ydb.OptionalType(ydb.PrimitiveType.Utf8))
            )
            .with_primary_key('key')
            .with_attributes(self.ATTRIBUTES)
        )

        response = session.describe_table(path)
        assert_that(response.attributes, equal_to(self.ATTRIBUTES))

    def test_copy_table(self):
        src_path = '{}/test_attributes/copy_table/src_table'.format(self.database_name)
        dst_path = '{}/test_attributes/copy_table/dst_table'.format(self.database_name)

        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())
        session.create_table(
            src_path,
            ydb.TableDescription()
            .with_columns(
                ydb.Column('key', ydb.OptionalType(ydb.PrimitiveType.Int32)),
                ydb.Column('value', ydb.OptionalType(ydb.PrimitiveType.Utf8))
            )
            .with_primary_key('key')
            .with_attributes(self.ATTRIBUTES)
        )

        session.copy_table(src_path, dst_path)

        response = session.describe_table(dst_path)
        assert_that(response.attributes, equal_to(self.ATTRIBUTES))

    def test_create_indexed_table(self):
        path = '{}/test_attributes/create_indexed_table/table'.format(self.database_name)

        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())
        session.create_table(
            path,
            ydb.TableDescription()
            .with_columns(
                ydb.Column('key', ydb.OptionalType(ydb.PrimitiveType.Int32)),
                ydb.Column('value', ydb.OptionalType(ydb.PrimitiveType.Utf8))
            )
            .with_primary_key('key')
            .with_index(
                ydb.TableIndex('by_value').with_index_columns('value')
            )
            .with_attributes(self.ATTRIBUTES)
        )

        response = session.describe_table(path)
        assert_that(response.attributes, equal_to(self.ATTRIBUTES))

    def test_alter_table(self):
        path = '{}/test_attributes/alter_table/table'.format(self.database_name)

        # create
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())
        session.create_table(
            path,
            ydb.TableDescription()
            .with_columns(
                ydb.Column('key', ydb.OptionalType(ydb.PrimitiveType.Int32)),
                ydb.Column('value', ydb.OptionalType(ydb.PrimitiveType.Utf8))
            )
            .with_primary_key('key')
        )

        response = session.describe_table(path)
        assert_that(response.attributes, equal_to({}))

        # alter (add)
        session.alter_table(
            path, add_columns=[], drop_columns=[], alter_attributes=self.ATTRIBUTES
        )

        response = session.describe_table(path)
        assert_that(response.attributes, equal_to(self.ATTRIBUTES))

        # alter (drop)
        session.alter_table(
            path, add_columns=[], drop_columns=[], alter_attributes={
                k: '' for k in self.ATTRIBUTES.keys()
            }
        )

        response = session.describe_table(path)
        assert_that(response.attributes, equal_to({}))

        # mixed alter
        def mixed_alter():
            session.alter_table(
                path, add_columns=[], drop_columns=['value'], alter_attributes=self.ATTRIBUTES
            )

        assert_that(mixed_alter, raises(ydb.Unsupported))

    @pytest.mark.parametrize('attributes', [
        {'': 'v'},  # empty key
        {'k': ''},  # empty value
        {'k' * (100 + 1): 'v'},  # too long key
        {'k': 'v' * (4096 + 1)},  # too long value
        {'k1': 'v1' * 2048, 'k2': 'v2' * 2048, 'k3': 'v3' * 2048},  # bad total size
    ])
    def test_limits(self, attributes):
        path = '{}/test_attributes/limits/table'.format(self.database_name)
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())

        def callee():
            session.create_table(
                path,
                ydb.TableDescription()
                .with_columns(
                    ydb.Column('key', ydb.OptionalType(ydb.PrimitiveType.Int32)),
                    ydb.Column('value', ydb.OptionalType(ydb.PrimitiveType.Utf8))
                )
                .with_primary_key('key')
                .with_attributes(attributes)
            )

        assert_that(callee, any_of(raises(ydb.BadRequest), raises(ydb.GenericError)))


class TestDocApiTables(WithTenant):
    def _create_table(self, path):
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())
        session.create_table(
            path,
            ydb.TableDescription()
            .with_columns(
                ydb.Column('key', ydb.OptionalType(ydb.PrimitiveType.Int32)),
                ydb.Column('value', ydb.OptionalType(ydb.PrimitiveType.Utf8))
            )
            .with_primary_key('key')
            .with_attributes({'__document_api_version': '1'})
        )

    def test_create_table(self):
        path = '{}/test_doc_api_tables/create_table/table'.format(self.database_name)
        self._create_table(path)

    @pytest.mark.parametrize('settings,ex', [
        (None, ydb.BadRequest),
        (ydb.BaseRequestSettings().with_request_type('_document_api_request'), None),
    ])
    def test_alter_table(self, settings, ex):
        path = '{}/test_test_doc_api_tables/alter_table/table'.format(self.database_name)
        self._create_table(path)
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())

        def callee():
            session.alter_table(
                path, add_columns=[], drop_columns=['value'], settings=settings
            )

        if ex:
            assert_that(callee, raises(ex, "Document API table cannot be modified"))
        else:
            callee()

    @pytest.mark.parametrize('settings,ex', [
        (None, None),
        (ydb.BaseRequestSettings().with_request_type('_document_api_request'), None),
    ])
    def test_drop_table(self, settings, ex):
        path = '{}/test_test_doc_api_tables/drop_table/table'.format(self.database_name)
        self._create_table(path)
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())

        def callee():
            session.drop_table(
                path, settings=settings
            )

        if ex:
            assert_that(callee, raises(ex, "Document API table cannot be modified"))
        else:
            callee()
