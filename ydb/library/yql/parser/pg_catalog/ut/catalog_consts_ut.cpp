#include <ydb/library/yql/parser/pg_wrapper/pg_compat.h>
#include <ydb/library/yql/parser/pg_catalog/catalog.h>

extern "C" {
#include "catalog/pg_collation_d.h"
#include "catalog/pg_type_d.h"
#include "catalog/pg_database_d.h"
#include "catalog/pg_tablespace_d.h"
#include "catalog/pg_shdescription_d.h"
#include "catalog/pg_trigger_d.h"
#include "catalog/pg_inherits_d.h"
#include "catalog/pg_description_d.h"
#include "catalog/pg_am_d.h"
#include "catalog/pg_namespace_d.h"
#include "catalog/pg_auth_members_d.h"
#include "catalog/pg_class_d.h"
#include "access/stratnum.h"
}

#include <library/cpp/testing/unittest/registar.h>


using namespace NYql::NPg;

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

    Y_UNIT_TEST(TTypeOidConsts) {
        TTypeDesc typeDesc;
        typeDesc = LookupType("unknown");
        UNIT_ASSERT_VALUES_EQUAL(typeDesc.TypeId, UnknownOid);
        typeDesc = LookupType("any");
        UNIT_ASSERT_VALUES_EQUAL(typeDesc.TypeId, AnyOid);
        typeDesc = LookupType("anyarray");
        UNIT_ASSERT_VALUES_EQUAL(typeDesc.TypeId, AnyArrayOid);
        typeDesc = LookupType("record");
        UNIT_ASSERT_VALUES_EQUAL(typeDesc.TypeId, RecordOid);
        typeDesc = LookupType("varchar");
        UNIT_ASSERT_VALUES_EQUAL(typeDesc.TypeId, VarcharOid);
        typeDesc = LookupType("text");
        UNIT_ASSERT_VALUES_EQUAL(typeDesc.TypeId, TextOid);
        typeDesc = LookupType("anynonarray");
        UNIT_ASSERT_VALUES_EQUAL(typeDesc.TypeId, AnyNonArrayOid);
    }

    Y_UNIT_TEST(TRelationOidConsts) {
        UNIT_ASSERT_VALUES_EQUAL(TypeRelationOid, TypeRelationId);
        UNIT_ASSERT_VALUES_EQUAL(DatabaseRelationOid, DatabaseRelationId);
        UNIT_ASSERT_VALUES_EQUAL(TableSpaceRelationOid, TableSpaceRelationId);
        UNIT_ASSERT_VALUES_EQUAL(SharedDescriptionRelationOid, SharedDescriptionRelationId);
        UNIT_ASSERT_VALUES_EQUAL(TriggerRelationOid, TriggerRelationId);
        UNIT_ASSERT_VALUES_EQUAL(InheritsRelationOid, InheritsRelationId);
        UNIT_ASSERT_VALUES_EQUAL(DescriptionRelationOid, DescriptionRelationId);
        UNIT_ASSERT_VALUES_EQUAL(AccessMethodRelationOid, AccessMethodRelationId);
        UNIT_ASSERT_VALUES_EQUAL(NamespaceRelationOid, NamespaceRelationId);
        UNIT_ASSERT_VALUES_EQUAL(AuthMemRelationOid, AuthMemRelationId);
        UNIT_ASSERT_VALUES_EQUAL(RelationRelationOid, RelationRelationId);
    }
}
