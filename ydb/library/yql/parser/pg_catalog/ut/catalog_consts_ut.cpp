#include <ydb/library/yql/parser/pg_catalog/catalog.h>

// see pg_attribute_printf macro in c.h
// should expand to nothing
#undef __GNUC__
#undef __IBMC__

extern "C" {
#include "postgres.h"
#include "catalog/pg_collation_d.h"
#include "access/stratnum.h"

#undef Min
#undef Max
}

#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(TConstantsTests) {
    Y_UNIT_TEST(TestCollationConsts) {
        UNIT_ASSERT(NYql::NPg::DefaultCollationOid == DEFAULT_COLLATION_OID);
        UNIT_ASSERT(NYql::NPg::C_CollationOid == C_COLLATION_OID);
        UNIT_ASSERT(NYql::NPg::PosixCollationOid == POSIX_COLLATION_OID);
    }

    Y_UNIT_TEST(BTreeAmStrategyConsts) {
        UNIT_ASSERT(static_cast<ui32>(NYql::NPg::EBtreeAmStrategy::Less) == BTLessStrategyNumber);
        UNIT_ASSERT(static_cast<ui32>(NYql::NPg::EBtreeAmStrategy::LessOrEqual) == BTLessEqualStrategyNumber);
        UNIT_ASSERT(static_cast<ui32>(NYql::NPg::EBtreeAmStrategy::Equal) == BTEqualStrategyNumber);
        UNIT_ASSERT(static_cast<ui32>(NYql::NPg::EBtreeAmStrategy::GreaterOrEqual) == BTGreaterEqualStrategyNumber);
        UNIT_ASSERT(static_cast<ui32>(NYql::NPg::EBtreeAmStrategy::Greater) == BTGreaterStrategyNumber);
    }
}
