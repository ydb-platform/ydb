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

    Y_UNIT_TEST(DumpIntCells) {
        NScheme::TTypeRegistry typeRegistry;

        i64 intVal = 55555;

        {
            TCell cell((const char *)&intVal, sizeof(i64));
            NScheme::TTypeInfo typeInfo(NScheme::TInt64::TypeId);

            TString printRes = DbgPrintCell(cell, typeInfo, typeRegistry);
            UNIT_ASSERT_STRINGS_EQUAL(printRes, "Int64 : 55555");
        }

        {
            NScheme::TTypeInfo pgTypeInfo(NPg::TypeDescFromPgTypeId(INT8OID));
            auto desc = pgTypeInfo.GetPgTypeDesc();
            auto res = NPg::PgNativeBinaryFromNativeText(Sprintf("%u", intVal), desc);
            UNIT_ASSERT_C(!res.Error, *res.Error);
            TString binary = res.Str;
            TCell pgCell((const char*)binary.data(), binary.size());

            TString printRes = DbgPrintCell(pgCell, pgTypeInfo, typeRegistry);
            UNIT_ASSERT_STRINGS_EQUAL(printRes, "pgint8 : 55555");
        }
    }

    Y_UNIT_TEST(DumpStringCells) {
        NScheme::TTypeRegistry typeRegistry;

        char strVal[] = "This is the value";

        {
            TCell cell((const char*)&strVal, sizeof(strVal) - 1);
            NScheme::TTypeInfo typeInfo(NScheme::TString::TypeId);

            TString printRes = DbgPrintCell(cell, typeInfo, typeRegistry);
            UNIT_ASSERT_STRINGS_EQUAL(printRes, TStringBuilder() << "String : " << strVal);
        }

        {
            NScheme::TTypeInfo pgTypeInfo(NPg::TypeDescFromPgTypeId(TEXTOID));
            auto desc = pgTypeInfo.GetPgTypeDesc();
            auto res = NPg::PgNativeBinaryFromNativeText(strVal, desc);
            UNIT_ASSERT_C(!res.Error, *res.Error);
            TString binary = res.Str;
            TCell pgCell((const char*)binary.data(), binary.size());

            TString printRes = DbgPrintCell(pgCell, pgTypeInfo, typeRegistry);
            UNIT_ASSERT_STRINGS_EQUAL(printRes, TStringBuilder() << "pgtext : " << strVal);
        }
    }
}

}
}
