#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/tablet_flat/test/libs/table/test_dbase.h>

namespace NKikimr {
namespace NTable {

Y_UNIT_TEST_SUITE(TFlatDatabaseDecimal) {

    Y_UNIT_TEST(UpdateRead) {
        constexpr ui32 tableId = 1;

        enum EColumnIds : ui32 {
            IdKeyDecimal229 = 1,
            IdKeyDecimal356 = 2,
            IdKeyInt = 3,
            IdValueDecimal229 = 4,
            IdValueDecimal356 = 5,
            IdValueInt = 6,
        };

        NTest::TDbExec db;

        auto makeDecimalTypeInfoProto = [] (ui32 precision, ui32 scale) {
            NKikimrProto::TTypeInfo typeInfo;
            typeInfo.SetDecimalPrecision(precision);
            typeInfo.SetDecimalScale(scale);
            return typeInfo;
        };

        db.Begin();
        db->Alter()
            .AddTable("TestTable", tableId)
            .AddColumnWithTypeInfo(tableId, "KeyDecimal229", IdKeyDecimal229, NScheme::NTypeIds::Decimal, makeDecimalTypeInfoProto(22,9), false)
            .AddColumnWithTypeInfo(tableId, "KeyDecimal356", IdKeyDecimal356, NScheme::NTypeIds::Decimal, makeDecimalTypeInfoProto(35,6), false)
            .AddColumn(tableId, "KeyInt", IdKeyInt, NScheme::NTypeIds::Int32, false)
            .AddColumnWithTypeInfo(tableId, "ValueDecimal229", IdValueDecimal229, NScheme::NTypeIds::Decimal, makeDecimalTypeInfoProto(22,9), false)
            .AddColumnWithTypeInfo(tableId, "ValueDecimal356", IdValueDecimal356, NScheme::NTypeIds::Decimal, makeDecimalTypeInfoProto(35,6), false)
            .AddColumn(tableId, "ValueInt", IdValueInt, NScheme::NTypeIds::Int32, false)
            .AddColumnToKey(tableId, IdKeyDecimal229)
            .AddColumnToKey(tableId, IdKeyDecimal356)
            .AddColumnToKey(tableId, IdKeyInt);
        db.Commit();

        typedef std::pair<ui64, ui64> TDecimalPair;
        const TDecimalPair decimalKey229(2, 2);
        const TDecimalPair decimalKey356(3, 3);
        const TDecimalPair decimalVal229(5, 5);
        const TDecimalPair decimalVal356(6, 6);

        for (int i = 1; i <= 10; ++i) {
            i32 intKey = i;
            i32 intVal = i * 10;

            db.Begin();

            TVector<TRawTypeValue> key;
            key.emplace_back(&decimalKey229, sizeof(TDecimalPair), NScheme::NTypeIds::Decimal);
            key.emplace_back(&decimalKey356, sizeof(TDecimalPair), NScheme::NTypeIds::Decimal);
            key.emplace_back(&intKey, sizeof(i32), NScheme::NTypeIds::Int32);

            TVector<NTable::TUpdateOp> ops;
            ops.emplace_back(IdValueDecimal229, NTable::ECellOp::Set,
                TRawTypeValue(&decimalVal229, sizeof(TDecimalPair), NScheme::NTypeIds::Decimal));
            ops.emplace_back(IdValueDecimal356, NTable::ECellOp::Set,
                TRawTypeValue(&decimalVal356, sizeof(TDecimalPair), NScheme::NTypeIds::Decimal));
            ops.emplace_back(IdValueInt, NTable::ECellOp::Set,
                TRawTypeValue(&intVal, sizeof(i32), NScheme::NTypeIds::Int32));

            db->Update(tableId, NTable::ERowOp::Upsert, key, ops);

            db.Commit();
        }

        TVector<NTable::TTag> tags(6);
        std::iota(tags.begin(), tags.end(), 1);

        auto readDatabase = [&] () {
            i32 intKey = 1;
            i32 intVal = 10;

            db.Begin();

            TVector<TRawTypeValue> key;
            key.emplace_back(&decimalKey229, sizeof(TDecimalPair), NScheme::NTypeIds::Decimal);
            key.emplace_back(&decimalKey356, sizeof(TDecimalPair), NScheme::NTypeIds::Decimal);
            key.emplace_back(&intKey, sizeof(i32), NScheme::NTypeIds::Int32);

            auto it = db->Iterate(tableId, key, tags, ELookup::GreaterOrEqualThan);
            size_t count = 0;
            while (it->Next(NTable::ENext::All) == NTable::EReady::Data) {
                auto key = it->GetKey();
                auto value = it->GetValues();
                UNIT_ASSERT_VALUES_EQUAL(key.ColumnCount, 3);
                UNIT_ASSERT_VALUES_EQUAL(value.ColumnCount, 6);

                UNIT_ASSERT_VALUES_EQUAL(value.Columns[3].AsValue<TDecimalPair>(), decimalVal229);
                UNIT_ASSERT_VALUES_EQUAL(value.Columns[4].AsValue<TDecimalPair>(), decimalVal356);
                UNIT_ASSERT(value.Columns[5].AsValue<i32>() >= intVal);
                ++count;
            }

            UNIT_ASSERT_VALUES_EQUAL(count, 10);

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
