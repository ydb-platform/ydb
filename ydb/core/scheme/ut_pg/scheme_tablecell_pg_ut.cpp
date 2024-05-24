#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme/scheme_type_registry.h>

extern "C" {
#include "postgres.h"
#include "catalog/pg_type_d.h"
}

namespace NKikimr {
namespace NTable {

Y_UNIT_TEST_SUITE(PgTest) {

    Y_UNIT_TEST(DumpCells) {
        NScheme::TTypeRegistry typeRegistry;

        char strVal[] = "This is the value";
        TCell strCell((const char*)&strVal, sizeof(strVal) - 1);

        NScheme::TTypeInfo typeInfo(NScheme::TString::TypeId);
        TString strRes = DbgPrintCell(strCell, typeInfo, typeRegistry);
        UNIT_ASSERT_STRINGS_EQUAL(strRes, TStringBuilder() << "String : " << strVal);

        NScheme::TTypeInfo pgTypeInfo(NScheme::NTypeIds::Pg, NPg::TypeDescFromPgTypeId(TEXTOID));
        TString pgStrRes = DbgPrintCell(strCell, pgTypeInfo, typeRegistry);
        UNIT_ASSERT_STRINGS_EQUAL(pgStrRes, TStringBuilder() << "pgtext : " << strVal);
    }
}

}
}
