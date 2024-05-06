# -*- coding: utf-8 -*-
from hamcrest import assert_that, equal_to, raises, contains_string

from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.oss.ydb_sdk_import import ydb


class TestTransactionIsolation(object):
    """
    Tests are based on the following documentation:
    https://github.com/ept/hermitage/blob/master/postgres.md
    """
    @classmethod
    def setup_class(cls):
        cls.cluster = kikimr_cluster_factory()
        cls.cluster.start()
        cls.driver = ydb.Driver(
            ydb.DriverConfig(
                database='/Root',
                endpoint="%s:%s" % (
                    cls.cluster.nodes[1].host, cls.cluster.nodes[1].port
                )
            )
        )
        cls.driver.wait()

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'cluster'):
            cls.cluster.stop()

        if hasattr(cls, 'driver'):
            cls.driver.stop()

    def _prepare(self, table):
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())
        session.execute_scheme(
            'create table %s (id Int32, value Int32, primary key(id)); ' % table)
        session.transaction().execute(
            'upsert into %s (id, value) values (1, 10), (2, 20); ' % table, commit_tx=True)
        return table, session

    def test_prevents_write_cycles_g0(self):
        """Write Cycles (G0), locking updated rows"""
        table_name, session = self._prepare("test_prevents_write_cycles_g0")

        t1 = session.transaction()
        t2 = session.transaction()

        prefix = ''

        t1.execute('{} upsert into {} (id, value) values (1, 11)'.format(prefix, table_name))
        t2.execute('{} select * from {} where id=1 or id=2;'.format(prefix, table_name))
        t2.execute('{} upsert into {} (id, value) values (1, 12)'.format(prefix, table_name))
        t1.execute('{} upsert into {} (id, value) values (2, 21)'.format(prefix, table_name))
        t1.commit()

        t3 = session.transaction()
        result_rows = t3.execute("{} select id, value from {} order by id;".format(prefix, table_name), commit_tx=True)
        assert_that(
            result_rows[0].rows,
            equal_to(
                [
                    {'id': 1, 'value': 11},
                    {'id': 2, 'value': 21},
                ]
            )
        )

        def callee():
            t2.execute("{} upsert into {} (id, value) values (2, 22);".format(prefix, table_name))
            t2.commit()

        assert_that(
            callee,
            raises(
                ydb.Aborted,
                "Transaction locks invalidated"
            )
        )

        t4 = session.transaction()
        result_rows = t4.execute("{} select id, value from {} order by id;".format(prefix, table_name))
        assert_that(
            result_rows[0].rows,
            equal_to(
                [
                    {'id': 1, 'value': 11},
                    {'id': 2, 'value': 21},
                ]
            )
        )

    def test_prevents_aborted_reads_g1a(self):
        table_name, session = self._prepare("test_prevents_aborted_reads_g1a")

        t1 = session.transaction()
        t2 = session.transaction()

        prefix = ''

        t1.execute('{} update {} set value = 101 where id = 1;'.format(prefix, table_name))
        result_rows = t2.execute('{} select id, value from {} order by id;'.format(prefix, table_name))
        assert_that(
            result_rows[0].rows,
            equal_to(
                [
                    {'id': 1, 'value': 10},
                    {'id': 2, 'value': 20},
                ]
            )
        )

        # abort;  -- T1 ----> replaced to rollback
        t1.rollback()
        result_rows = t2.execute('{} select id, value from {} order by id;'.format(prefix, table_name))
        assert_that(
            result_rows[0].rows,
            equal_to(
                [
                    {'id': 1, 'value': 10},
                    {'id': 2, 'value': 20},
                ]
            )
        )
        t2.commit()

    def test_prevents_intermediate_reads_g1b(self):
        table_name, session = self._prepare("test_prevents_intermediate_reads_g1b")

        t1 = session.transaction()
        t2 = session.transaction()

        prefix = ''

        t1.execute('{} select * from {} where id=1'.format(prefix, table_name))
        t1.execute('{} upsert into {} (id, value) values (1, 101);'.format(prefix, table_name))
        result_rows = t2.execute('{} select id, value from {} order by id;'.format(prefix, table_name), commit_tx=True)
        assert_that(
            result_rows[0].rows,
            equal_to(
                [
                    {'id': 1, 'value': 10},
                    {'id': 2, 'value': 20},
                ]
            )
        )

        t1.execute('{} upsert into {} (id, value) values (1, 11);'.format(prefix, table_name))
        t1.commit()

        t3 = session.transaction()
        result_rows = t3.execute('{} select id, value from {} order by id;'.format(prefix, table_name), commit_tx=True)
        assert_that(
            result_rows[0].rows,
            equal_to(
                [
                    {'id': 1, 'value': 11},
                    {'id': 2, 'value': 20},
                ]
            )
        )

    def test_prevents_circular_information_flow_g1c(self):
        table_name, session = self._prepare("test_prevents_circular_information_flow_g1c")

        t1 = session.transaction()
        t2 = session.transaction()

        t1.execute('update {} set value = 11 where id = 1;'.format(table_name))
        t2.execute('update {} set value = 22 where id = 2;'.format(table_name))

        result_rows = t1.execute('select * from {} where id = 2;'.format(table_name))
        assert_that(
            result_rows[0].rows,
            equal_to(
                [
                    {'id': 2, 'value': 20},
                ]
            )
        )

        result_rows = t2.execute('select * from {} where id = 1;'.format(table_name))
        assert_that(
            result_rows[0].rows,
            equal_to(
                [
                    {'id': 1, 'value': 10},
                ]
            )
        )

        t1.commit()
        try:
            t2.commit()
        except ydb.Aborted as e:
            assert_that(str(e), contains_string("Transaction locks invalidated"))

    def test_isolation_mailing_list_example(self):
        table_name, session = self._prepare("test_isolation_mailing_list_example")

        t1 = session.transaction()
        t2 = session.transaction()

        prefix = ''

        t1.execute(session.prepare('{} upsert into {} (id, value) values (1, 3);'.format(prefix, table_name)), commit_tx=True)

        t3 = session.transaction()
        t3.execute(session.prepare('{} select id, value FROM {} WHERE id = 1'.format(prefix, table_name)))
        t2.execute(session.prepare('{} select id, value FROM {} WHERE id = 1'.format(prefix, table_name)))
        t3.execute(session.prepare('{} upsert into {} (id, value) values (1, 4);'.format(prefix, table_name)))
        t3.commit()

        def callee():
            t2.execute(session.prepare('{} upsert into {} (id, value) values (1, 5);'.format(prefix, table_name)))
            t2.commit()

        assert_that(
            callee,
            raises(
                ydb.Aborted,
                "Transaction locks invalidated"
            )
        )

        t4 = session.transaction()
        result_rows = t4.execute('{} select id, value from {} where id = 1;'.format(prefix, table_name), commit_tx=True)
        assert_that(
            result_rows[0].rows[0].value,
            equal_to(
                4
            )
        )

    def test_prevents_observed_transaction_vanishes_otv(self):
        table_name, session = self._prepare("test_prevents_observed_transaction_vanishes_otv")

        t1 = session.transaction()
        t2 = session.transaction()
        t3 = session.transaction()

        prefix = ''

        t1.execute('{} select * from {} where id=1 or id=2;'.format(prefix, table_name))
        t1.execute('{} upsert into {} (id, value) values (1, 11);'.format(prefix, table_name))
        t1.execute('{} upsert into {} (id, value) values (2, 19);'.format(prefix, table_name))
        t2.execute('{} select * from {} where id=1'.format(prefix, table_name))
        t2.execute('{} upsert into {} (id, value) values (1, 12);'.format(prefix, table_name))
        t1.commit()
        result_rows = t3.execute('{} select value from {} where id = 1;'.format(prefix, table_name))
        assert_that(
            result_rows[0].rows,
            equal_to(
                [
                    {'value': 11}
                ]
            )
        )

        def callee():
            t2.execute('{} upsert into {} (id, value) values (2, 18);'.format(prefix, table_name))
            t2.commit()

        assert_that(
            callee,
            raises(
                ydb.Aborted,
                "Transaction locks invalidated"
            )
        )

        t3.execute('{} select value from {} where id = 2;'.format(prefix, table_name))
        # commit; -- T2 we already got transaction locks invalidated error

        result_rows = t3.execute('{} select value from {} where id = 2;'.format(prefix, table_name))
        assert_that(
            result_rows[0].rows,
            equal_to(
                [
                    {'value': 19}
                ]
            )
        )
        result_rows = t3.execute('{} select value from {} where id = 1;'.format(prefix, table_name))
        assert_that(
            result_rows[0].rows,
            equal_to(
                [
                    {'value': 11}
                ]
            )
        )
        t3.commit()

    def test_does_not_prevent_predicate_many_preceders_pmp(self):
        table_name, session = self._prepare("test_does_not_prevent_predicate_many_preceders_pmp")

        t1 = session.transaction()
        t2 = session.transaction()

        prefix = ''

        result_rows = t1.execute('{} select id from {} where value = 30;'.format(prefix, table_name))
        assert_that(result_rows[0].rows, equal_to([]))

        t2.execute('{} upsert into {} (id, value) values(3, 30);'.format(prefix, table_name))
        t2.commit()

        try:
            result_rows = t1.execute('{} select id from {} where value % 3 = 0;'.format(prefix, table_name))
            t1.commit()
        except ydb.Aborted as e:
            assert_that(str(e), contains_string("Transaction locks invalidated"))
        else:
            assert_that(result_rows[0].rows, equal_to([]))

    def test_does_not_prevent_predicate_many_preceders_pmp_for_write_predicates(self):
        table_name, session = self._prepare("test_does_not_prevent_predicate_many_preceders_pmp_for_write_predicates")

        t1 = session.transaction()
        t2 = session.transaction()

        t1.execute('update {} set value = value + 10;'.format(table_name))
        t2.execute('delete from {} where value = 20;'.format(table_name))

        t1.commit()

        try:
            t2.commit()
        except ydb.Aborted as e:
            assert_that(str(e), contains_string("Transaction locks invalidated"))

    def test_lost_update_p4(self):
        table_name, session = self._prepare("test_lost_update_p4")

        t1 = session.transaction()
        t2 = session.transaction()

        prefix = ''

        result_rows = t1.execute('{} select id from {} where id = 1;'.format(prefix, table_name))
        assert_that(
            result_rows[0].rows, equal_to(
                [{'id': 1}]
            )
        )

        result_rows = t2.execute('{} select id from {} where id = 1;'.format(prefix, table_name))
        assert_that(
            result_rows[0].rows, equal_to(
                [{'id': 1}]
            )
        )

        t1.execute('{} update {} set value = 11 where id = 1;'.format(prefix, table_name))
        t2.execute('{} update {} set value = 11 where id = 1;'.format(prefix, table_name))

        t1.commit()

        def callee():
            t2.commit()

        assert_that(
            callee,
            raises(
                ydb.Aborted,
                "Transaction locks invalidated"
            )
        )

    def test_lost_update_on_value_p4(self):
        table_name, session = self._prepare("test_lost_update_on_value_p4")

        t1 = session.transaction()
        t2 = session.transaction()

        prefix = ''

        result_rows = t1.execute('{} select id, value from {} where id = 1;'.format(prefix, table_name))
        assert_that(
            result_rows[0].rows,
            equal_to(
                [
                    {'id': 1, 'value': 10}
                ]
            )
        )

        result_rows = t2.execute('{} select id, value from {} where id = 1;'.format(prefix, table_name))
        assert_that(
            result_rows[0].rows,
            equal_to(
                [
                    {'id': 1, 'value': 10}
                ]
            )
        )

        t1.execute('{} update {} set value = 11 where id = 1;'.format(prefix, table_name))
        t1.commit()

        def callee():
            t2.execute('{} update {} set value = 12 where id = 1;'.format(prefix, table_name))
            t2.commit()

        assert_that(
            callee,
            raises(
                ydb.Aborted,
                "Transaction locks invalidated"
            )
        )

    def test_lost_update_on_value_with_upsert_p4(self):
        table_name, session = self._prepare("test_lost_update_on_value_with_upsert_p4")

        t1 = session.transaction()
        t2 = session.transaction()

        prefix = ''

        result_rows = t1.execute('{} select id, value from {} where id = 1;'.format(prefix, table_name))
        assert_that(
            result_rows[0].rows, equal_to(
                [
                    {'id': 1, 'value': 10}
                ]
            )
        )

        result_rows = t2.execute('{} select id, value from {} where id = 1;'.format(prefix, table_name))
        assert_that(
            result_rows[0].rows, equal_to(
                [
                    {'id': 1, 'value': 10}
                ]
            )
        )

        t1.execute('{} upsert into {} (id, value) VALUES (1, 11);'.format(prefix, table_name))
        t1.commit()
        t2.execute('{} upsert into {} (id, value) VALUES (1, 12);'.format(prefix, table_name))

        def callee():
            t2.commit()

        assert_that(
            callee,
            raises(
                ydb.Aborted,
                "Transaction locks invalidated"
            )
        )

    def test_read_skew_g_single(self):
        table_name, session = self._prepare("test_read_skew_g_single")

        t1 = session.transaction()
        t2 = session.transaction()

        prefix = ''

        result_rows = t1.execute('{} select value from {} where id = 1;'.format(prefix, table_name))
        assert_that(
            result_rows[0].rows, equal_to(
                [
                    {'value': 10}
                ]
            )
        )

        result_rows = t2.execute('{} select value from {} where id = 1;'.format(prefix, table_name))
        assert_that(
            result_rows[0].rows, equal_to(
                [{'value': 10}]
            )
        )

        result_rows = t2.execute('{} select value from {} where id = 2;'.format(prefix, table_name))
        assert_that(
            result_rows[0].rows, equal_to(
                [
                    {'value': 20}
                ]
            )
        )

        t2.execute('{} upsert into {} (id, value) values (1, 12);'.format(prefix, table_name))
        t2.execute('{} upsert into {} (id, value) values (2, 18);'.format(prefix, table_name))
        t2.commit()

        try:
            result_rows = t1.execute('{} select value from {} where id = 2;'.format(prefix, table_name))
            t1.commit()
        except ydb.Aborted as e:
            assert_that(str(e), contains_string("Transaction locks invalidated"))
        else:
            assert_that(
                result_rows[0].rows, equal_to(
                    [
                        {'value': 20}
                    ]
                )
            )

    def test_read_skew_g_single_predicate_deps(self):
        table_name, session = self._prepare("test_read_skew_g_single_predicate_deps")

        t1 = session.transaction()
        t2 = session.transaction()

        prefix = ''

        t1.execute('{} select value from {} where value % 5 = 0;'.format(prefix, table_name))
        t2.execute('{} update {} set value = 12 where value = 10;'.format(prefix, table_name))

        t2.commit()

        try:
            result_rows = t1.execute('{} select value from {} where value % 3 = 0;'.format(prefix, table_name))
            t1.commit()
        except ydb.Aborted as e:
            assert_that(str(e), contains_string("Transaction locks invalidated"))
        else:
            assert_that(result_rows[0].rows, equal_to([]))

    def test_read_skew_g_single_write_predicate(self):
        table_name, session = self._prepare("test_read_skew_g_single_write_predicate")

        t1 = session.transaction()
        t2 = session.transaction()

        prefix = ''

        result_rows = t1.execute('{} select value from {} where id = 1;'.format(prefix, table_name))
        assert_that(
            result_rows[0].rows, equal_to(
                [
                    {'value': 10}
                ]
            )
        )

        t2.execute('{} select * from {};'.format(prefix, table_name))
        t2.execute('{} upsert into {} (id, value) values (1, 12)'.format(prefix, table_name))
        t2.execute('{} upsert into {} (id, value) values (2, 18);'.format(prefix, table_name))

        t2.commit()

        def callee():
            t1.execute('{} delete from {} where value = 20;'.format(prefix, table_name), commit_tx=True)

        assert_that(
            callee,
            raises(
                ydb.Aborted,
                "Transaction locks invalidated"
            )
        )

    def test_write_skew_g2_item(self):
        table_name, session = self._prepare("test_write_skew_g2_item")

        t1 = session.transaction()
        t2 = session.transaction()

        prefix = ''

        t1.execute('{} select value from {} where id in (1,2);'.format(prefix, table_name))
        t2.execute('{} select value from {} where id in (1,2);'.format(prefix, table_name))
        t1.execute('{} update {} set value = 11 where id = 1;'.format(prefix, table_name))
        t2.execute('{} update {} set value = 21 where id = 2;'.format(prefix, table_name))
        t1.commit()

        def callee():
            t2.commit()

        assert_that(
            callee,
            raises(
                ydb.Aborted,
                "Transaction locks invalidated"
            )
        )

    def test_anti_dependency_cycles_g2(self):
        table_name, session = self._prepare("test_anti_dependency_cycles_g2")

        t1 = session.transaction()
        t2 = session.transaction()

        prefix = ''

        t1.execute('{} select value from {} where value % 3 = 0;'.format(prefix, table_name))
        t2.execute('{} select value from {} where value % 3 = 0;'.format(prefix, table_name))

        t1.execute('{} upsert into {} (id, value) values(3, 30);'.format(prefix, table_name))
        t2.execute('{} upsert into {} (id, value) values(4, 42);'.format(prefix, table_name))

        t1.commit()

        def callee():
            t2.commit()

        assert_that(
            callee,
            raises(
                ydb.Aborted,
                "Transaction locks invalidated"
            )
        )

    def test_anti_dependency_cycles_g2_two_edges(self):
        table_name, session = self._prepare("test_anti_dependency_cycles_g2_two_edges")

        t1 = session.transaction()
        t2 = session.transaction()
        t3 = session.transaction()

        prefix = ''

        result_rows = t1.execute('{} select id, value from {};'.format(prefix, table_name))
        assert_that(
            result_rows[0].rows,
            equal_to(
                [
                    {'id': 1, 'value': 10},
                    {'id': 2, 'value': 20},
                ]
            )
        )

        t2.execute('{} update {} set value = value + 5 where id = 2;'.format(prefix, table_name))

        t2.commit()

        result_rows = t3.execute('{} select id, value from {};'.format(prefix, table_name))
        assert_that(
            result_rows[0].rows,
            equal_to(
                [
                    {'id': 1, 'value': 10},
                    {'id': 2, 'value': 25},
                ]
            )
        )

        t3.commit()

        def callee():
            t1.execute('{} update {} set value = 0 where id = 1;'.format(prefix, table_name))

        assert_that(
            callee,
            raises(
                ydb.Aborted,
                "Transaction locks invalidated"
            )
        )
