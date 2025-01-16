#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/tablet_flat/test/libs/table/test_dbase.h>

extern "C" {
#include "postgres.h"
#include "catalog/pg_type_d.h"
}

namespace NKikimr {
namespace NTable {

Y_UNIT_TEST_SUITE(TFlatDatabasePgTest) {

    Y_UNIT_TEST(BasicTypes) {
        constexpr ui32 tableId = 1;

        enum EColumnIds : ui32 {
            IdBool = 1,
            IdChar = 2,
            IdInt2 = 3,
            IdInt4 = 4,
            IdInt8 = 5,
            IdFloat4 = 6,
            IdFloat8 = 7,
            IdText = 8,
            IdBytea = 9,
            IdBpchar = 10,
        };

        NTest::TDbExec db;

        auto makePgTypeInfo = [] (ui32 pgTypeId) {
            NKikimrProto::TTypeInfo typeInfo;
            typeInfo.SetPgTypeId(pgTypeId);
            typeInfo.SetPgTypeMod("");
            return typeInfo;
        };

        db.Begin();
        db->Alter()
            .AddTable("TestTable", tableId)
            .AddColumnWithTypeInfo(tableId, "boolean", IdBool, NScheme::NTypeIds::Pg, makePgTypeInfo(BOOLOID), false)
            .AddColumnWithTypeInfo(tableId, "char", IdChar, NScheme::NTypeIds::Pg, makePgTypeInfo(CHAROID), false)
            .AddColumnWithTypeInfo(tableId, "int2", IdInt2, NScheme::NTypeIds::Pg, makePgTypeInfo(INT2OID), false)
            .AddColumnWithTypeInfo(tableId, "int4", IdInt4, NScheme::NTypeIds::Pg, makePgTypeInfo(INT4OID), false)
            .AddColumnWithTypeInfo(tableId, "int8", IdInt8, NScheme::NTypeIds::Pg, makePgTypeInfo(INT8OID), false)
            .AddColumnWithTypeInfo(tableId, "float4", IdFloat4, NScheme::NTypeIds::Pg, makePgTypeInfo(FLOAT4OID), false)
            .AddColumnWithTypeInfo(tableId, "float8", IdFloat8, NScheme::NTypeIds::Pg, makePgTypeInfo(FLOAT8OID), false)
            .AddColumnWithTypeInfo(tableId, "text", IdText, NScheme::NTypeIds::Pg, makePgTypeInfo(TEXTOID), false)
            .AddColumnWithTypeInfo(tableId, "bytea", IdBytea, NScheme::NTypeIds::Pg, makePgTypeInfo(BYTEAOID), false)
            .AddColumnWithTypeInfo(tableId, "bpchar", IdBpchar, NScheme::NTypeIds::Pg, makePgTypeInfo(BPCHAROID), false)
            .AddColumnToKey(tableId, IdText)
            .AddColumnToKey(tableId, IdBytea)
            .AddColumnToKey(tableId, IdInt2)
            .AddColumnToKey(tableId, IdFloat4);
        db.Commit();

        bool boolVal = true;
        char charVal = 'a';
        i16 i16Val = 2;
        i32 i32Val = 3;
        i64 i64Val = 4;
        float floatVal = 5.f;
        double doubleVal = 6.;
        auto strText = TString("abc");
        auto strBytea = TString("bytea");
        auto strBpchar = TString("bpchar");

        for (int i = 1; i <= 10; ++i) {
            db.Begin();

            TVector<TRawTypeValue> key;
            key.emplace_back(strText.data(), strText.size(), NScheme::NTypeIds::Pg);
            key.emplace_back(strBytea.data(), strBytea.size(), NScheme::NTypeIds::Pg);
            key.emplace_back(&i16Val, sizeof(i16), NScheme::NTypeIds::Pg);
            float f = (float)i;
            key.emplace_back(&f, sizeof(float), NScheme::NTypeIds::Pg);

            TVector<NTable::TUpdateOp> ops;
            ops.emplace_back(IdBool, NTable::ECellOp::Set,
                TRawTypeValue(&boolVal, sizeof(bool), NScheme::NTypeIds::Pg));
            ops.emplace_back(IdChar, NTable::ECellOp::Set,
                TRawTypeValue(&charVal, sizeof(char), NScheme::NTypeIds::Pg));
            ops.emplace_back(IdInt4, NTable::ECellOp::Set,
                TRawTypeValue(&i32Val, sizeof(i32), NScheme::NTypeIds::Pg));
            ops.emplace_back(IdInt8, NTable::ECellOp::Set,
                TRawTypeValue(&i64Val, sizeof(i64), NScheme::NTypeIds::Pg));
            ops.emplace_back(IdFloat8, NTable::ECellOp::Set,
                TRawTypeValue(&doubleVal, sizeof(double), NScheme::NTypeIds::Pg));
            ops.emplace_back(IdBpchar, NTable::ECellOp::Set,
                TRawTypeValue(strBpchar.data(), strBpchar.size(), NScheme::NTypeIds::Pg));

            db->Update(tableId, NTable::ERowOp::Upsert, key, ops);

            db.Commit();
        }

        TVector<NTable::TTag> tags;
        for (NTable::TTag t = 1; t <= 10; ++t) {
            tags.push_back(t);
        }

        auto readDatabase = [&] () {
            db.Begin();

            TVector<TRawTypeValue> key;
            key.emplace_back(strText.data(), strText.size(), NScheme::NTypeIds::Pg);
            key.emplace_back(strBytea.data(), strBytea.size(), NScheme::NTypeIds::Pg);
            key.emplace_back(&i16Val, sizeof(i16), NScheme::NTypeIds::Pg);
            key.emplace_back(&floatVal, sizeof(float), NScheme::NTypeIds::Pg);

            auto it = db->Iterate(tableId, key, tags, ELookup::GreaterThan);
            size_t count = 0;
            while (it->Next(NTable::ENext::All) == NTable::EReady::Data) {
                auto key = it->GetKey();
                auto value = it->GetValues();
                UNIT_ASSERT_VALUES_EQUAL(key.ColumnCount, 4);
                UNIT_ASSERT_VALUES_EQUAL(value.ColumnCount, 10);

                UNIT_ASSERT_VALUES_EQUAL(value.Columns[0].AsValue<bool>(), boolVal);
                UNIT_ASSERT_VALUES_EQUAL(value.Columns[1].AsValue<char>(), charVal);
                UNIT_ASSERT_VALUES_EQUAL(value.Columns[2].AsValue<i16>(), i16Val);
                UNIT_ASSERT_VALUES_EQUAL(value.Columns[3].AsValue<i32>(), i32Val);
                UNIT_ASSERT_VALUES_EQUAL(value.Columns[4].AsValue<i64>(), i64Val);
                UNIT_ASSERT(value.Columns[5].AsValue<float>() > floatVal);
                UNIT_ASSERT_VALUES_EQUAL(value.Columns[6].AsValue<double>(), doubleVal);
                UNIT_ASSERT(std::memcmp(value.Columns[7].Data(), strText.data(), value.Columns[7].Size()) == 0);
                UNIT_ASSERT(std::memcmp(value.Columns[8].Data(), strBytea.data(), value.Columns[8].Size()) == 0);
                UNIT_ASSERT(std::memcmp(value.Columns[9].Data(), strBpchar.data(), value.Columns[9].Size()) == 0);
                ++count;
            }

            db.Commit();
        };

        readDatabase();

        db.Snap(tableId).Compact(tableId, false);

        readDatabase();

        db.Replay(NTest::EPlay::Boot);
        db.Replay(NTest::EPlay::Redo);        
    }
}

}
}
