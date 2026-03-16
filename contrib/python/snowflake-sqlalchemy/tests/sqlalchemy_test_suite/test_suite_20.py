#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import pytest
from sqlalchemy import Integer, testing
from sqlalchemy.schema import Column, Sequence, Table
from sqlalchemy.testing import config
from sqlalchemy.testing.assertions import eq_
from sqlalchemy.testing.suite import (
    BizarroCharacterFKResolutionTest as _BizarroCharacterFKResolutionTest,
)
from sqlalchemy.testing.suite import (
    CompositeKeyReflectionTest as _CompositeKeyReflectionTest,
)
from sqlalchemy.testing.suite import DateTimeHistoricTest as _DateTimeHistoricTest
from sqlalchemy.testing.suite import FetchLimitOffsetTest as _FetchLimitOffsetTest
from sqlalchemy.testing.suite import HasSequenceTest as _HasSequenceTest
from sqlalchemy.testing.suite import InsertBehaviorTest as _InsertBehaviorTest
from sqlalchemy.testing.suite import LikeFunctionsTest as _LikeFunctionsTest
from sqlalchemy.testing.suite import LongNameBlowoutTest as _LongNameBlowoutTest
from sqlalchemy.testing.suite import SimpleUpdateDeleteTest as _SimpleUpdateDeleteTest
from sqlalchemy.testing.suite import TimeMicrosecondsTest as _TimeMicrosecondsTest
from sqlalchemy.testing.suite import TrueDivTest as _TrueDivTest
from sqlalchemy.testing.suite import *  # noqa

# 1. Unsupported by snowflake db

del ComponentReflectionTest  # require indexes not supported by snowflake
del HasIndexTest  # require indexes not supported by snowflake
del QuotedNameArgumentTest  # require indexes not supported by snowflake


class LongNameBlowoutTest(_LongNameBlowoutTest):
    # The combination ("ix",) is removed due to Snowflake not supporting indexes
    def ix(self, metadata, connection):
        pytest.skip("ix required index feature not supported by Snowflake")


class FetchLimitOffsetTest(_FetchLimitOffsetTest):
    @pytest.mark.skip(
        "Snowflake only takes non-negative integer constants for offset/limit"
    )
    def test_bound_offset(self, connection):
        pass

    @pytest.mark.skip(
        "Snowflake only takes non-negative integer constants for offset/limit"
    )
    def test_simple_limit_expr_offset(self, connection):
        pass

    @pytest.mark.skip(
        "Snowflake only takes non-negative integer constants for offset/limit"
    )
    def test_simple_offset(self, connection):
        pass

    @pytest.mark.skip(
        "Snowflake only takes non-negative integer constants for offset/limit"
    )
    def test_simple_offset_zero(self, connection):
        pass


class InsertBehaviorTest(_InsertBehaviorTest):
    @pytest.mark.skip(
        "Snowflake does not support inserting empty values, the value may be a literal or an expression."
    )
    def test_empty_insert(self, connection):
        pass

    @pytest.mark.skip(
        "Snowflake does not support inserting empty values, The value may be a literal or an expression."
    )
    def test_empty_insert_multiple(self, connection):
        pass

    @pytest.mark.skip("Snowflake does not support returning in insert.")
    def test_no_results_for_non_returning_insert(self, connection, style, executemany):
        pass


# road to 2.0
class TrueDivTest(_TrueDivTest):
    @pytest.mark.skip("`//` not supported")
    def test_floordiv_integer_bound(self, connection):
        """Snowflake does not provide `//` arithmetic operator.

        https://docs.snowflake.com/en/sql-reference/operators-arithmetic.
        """
        pass

    @pytest.mark.skip("`//` not supported")
    def test_floordiv_integer(self, connection, left, right, expected):
        """Snowflake does not provide `//` arithmetic operator.

        https://docs.snowflake.com/en/sql-reference/operators-arithmetic.
        """
        pass


class TimeMicrosecondsTest(_TimeMicrosecondsTest):
    def __init__(self):
        super().__init__()


class DateTimeHistoricTest(_DateTimeHistoricTest):
    def __init__(self):
        super().__init__()


# 2. Patched Tests


class HasSequenceTest(_HasSequenceTest):
    # Override the define_tables method as snowflake does not support 'nomaxvalue'/'nominvalue'
    @classmethod
    def define_tables(cls, metadata):
        Sequence("user_id_seq", metadata=metadata)
        # Replace Sequence("other_seq") creation as in the original test suite,
        # the Sequence created with 'nomaxvalue' and 'nominvalue'
        # which snowflake does not support:
        #     Sequence("other_seq", metadata=metadata, nomaxvalue=True, nominvalue=True)
        Sequence("other_seq", metadata=metadata)
        if testing.requires.schemas.enabled:
            Sequence("user_id_seq", schema=config.test_schema, metadata=metadata)
            Sequence("schema_seq", schema=config.test_schema, metadata=metadata)
        Table(
            "user_id_table",
            metadata,
            Column("id", Integer, primary_key=True),
        )


class LikeFunctionsTest(_LikeFunctionsTest):
    @testing.requires.regexp_match
    @testing.combinations(
        ("a.cde.*", {1, 5, 6, 9}),
        ("abc.*", {1, 5, 6, 9, 10}),
        ("^abc.*", {1, 5, 6, 9, 10}),
        (".*9cde.*", {8}),
        ("^a.*", set(range(1, 11))),
        (".*(b|c).*", set(range(1, 11))),
        ("^(b|c).*", set()),
    )
    def test_regexp_match(self, text, expected):
        super().test_regexp_match(text, expected)

    def test_not_regexp_match(self):
        col = self.tables.some_table.c.data
        self._test(~col.regexp_match("a.cde.*"), {2, 3, 4, 7, 8, 10})


class SimpleUpdateDeleteTest(_SimpleUpdateDeleteTest):
    def test_update(self, connection):
        t = self.tables.plain_pk
        r = connection.execute(t.update().where(t.c.id == 2), dict(data="d2_new"))
        assert not r.is_insert
        # snowflake returns a row with numbers of rows updated and number of multi-joined rows updated
        assert r.returns_rows
        assert r.rowcount == 1

        eq_(
            connection.execute(t.select().order_by(t.c.id)).fetchall(),
            [(1, "d1"), (2, "d2_new"), (3, "d3")],
        )

    def test_delete(self, connection):
        t = self.tables.plain_pk
        r = connection.execute(t.delete().where(t.c.id == 2))
        assert not r.is_insert
        # snowflake returns a row with number of rows deleted
        assert r.returns_rows
        assert r.rowcount == 1
        eq_(
            connection.execute(t.select().order_by(t.c.id)).fetchall(),
            [(1, "d1"), (3, "d3")],
        )


class CompositeKeyReflectionTest(_CompositeKeyReflectionTest):
    @pytest.mark.xfail(reason="Fixing this would require behavior breaking change.")
    def test_fk_column_order(self):
        # Check https://snowflakecomputing.atlassian.net/browse/SNOW-640134 for details on breaking changes discussion.
        super().test_fk_column_order()

    @pytest.mark.xfail(reason="Fixing this would require behavior breaking change.")
    def test_pk_column_order(self):
        # Check https://snowflakecomputing.atlassian.net/browse/SNOW-640134 for details on breaking changes discussion.
        super().test_pk_column_order()


class BizarroCharacterFKResolutionTest(_BizarroCharacterFKResolutionTest):
    @testing.combinations(
        ("id",), ("(3)",), ("col%p",), ("[brack]",), argnames="columnname"
    )
    @testing.variation("use_composite", [True, False])
    @testing.combinations(
        ("plain",),
        ("(2)",),
        ("[brackets]",),
        argnames="tablename",
    )
    def test_fk_ref(self, connection, metadata, use_composite, tablename, columnname):
        super().test_fk_ref(connection, metadata, use_composite, tablename, columnname)
