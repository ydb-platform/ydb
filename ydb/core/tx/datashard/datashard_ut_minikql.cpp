#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>

#include <ydb/public/lib/deprecated/kicli/kicli.h>


namespace NKikimr {

using NClient::TValue;
using IEngineFlat = NMiniKQL::IEngineFlat;


Y_UNIT_TEST_SUITE(TTxDataShardMiniKQL) {

Y_UNIT_TEST(ReadConstant) {
    TTester t(TTester::ESchema_KV);
    TFakeMiniKQLProxy proxy(t);

    auto programText = R"___((
        (return (AsList (SetResult 'res1 (Int32 '2016))))
    ))___";

    UNIT_ASSERT_EQUAL(proxy.Execute(programText), IEngineFlat::EStatus::Complete);
}

//

Y_UNIT_TEST(Write) {
    TTester t(TTester::ESchema_KV);
    TFakeMiniKQLProxy proxy(t);

    auto programText = R"___((
        (let row '('('key (Uint32 '42))))
        (let myUpd '(
            '('value (Utf8 'Robert))))
        (let pgmReturn (AsList
        (UpdateRow 'table1 row myUpd)
        ))
        (return pgmReturn)
    ))___";

    UNIT_ASSERT_EQUAL(proxy.Execute(programText), IEngineFlat::EStatus::Complete);
}

//

Y_UNIT_TEST(ReadAfterWrite) {
    TTester t(TTester::ESchema_KV);
    TFakeMiniKQLProxy proxy(t);

    {
        auto programText = R"___((
            (let row '('('key (Uint32 '42))))
            (let myUpd '(
                '('value (Utf8 'Robert))))
            (let pgmReturn (AsList
            (UpdateRow 'table1 row myUpd)
            ))
            (return pgmReturn)
        ))___";

        UNIT_ASSERT_EQUAL(proxy.Execute(programText), IEngineFlat::EStatus::Complete);
    }

    {
        auto programText = R"___((
            (let row '('('key (Uint32 '42))))
            (let select '('value))
            (let pgmReturn (AsList
            (SetResult 'myRes (SelectRow 'table1 row select))
            ))
            (return pgmReturn)
        ))___";

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(proxy.Execute(programText, res), IEngineFlat::EStatus::Complete);

        TValue value = TValue::Create(res.GetValue(), res.GetType());
        TValue row = value["myRes"];
        TString data = row["value"];
        UNIT_ASSERT_EQUAL(data, "Robert");
    }
}

Y_UNIT_TEST(ReadSpecialColumns) {
    TTester t(TTester::ESchema_SpecialKV);
    TFakeMiniKQLProxy proxy(t);

    {
        TString tx1 = R"((
            (let row1 '('('key (Uint32 '1))))
            (let row2 '('('key (Uint32 '2))))
            (let upsert1 '('('value (Utf8 'one))))
            (let upsert2 '('('value (Utf8 'two))))
            (return (AsList
                (UpdateRow 'table1 row1 upsert1)
                (UpdateRow 'table1 row2 upsert2)
            ))
        ))";

        UNIT_ASSERT_EQUAL(proxy.Execute(tx1), IEngineFlat::EStatus::Complete);

        TString tx2 = R"((
            (let row3 '('('key (Uint32 '3))))
            (let upsert3 '('('value (Utf8 'three))))
            (return (AsList
                (UpdateRow 'table1 row3 upsert3)
            ))
        ))";

        UNIT_ASSERT_EQUAL(proxy.Execute(tx2), IEngineFlat::EStatus::Complete);
    }

    {
        auto programText = R"((
            (let row1 '('('key (Uint32 '1))))
            (let row2 '('('key (Uint32 '2))))
            (let row3 '('('key (Uint32 '3))))
            (let select '('value '__tablet '__updateEpoch '__updateNo))
            (return (AsList
                (SetResult 'one (SelectRow 'table1 row1 select))
                (SetResult 'two (SelectRow 'table1 row2 select))
                (SetResult 'three (SelectRow 'table1 row3 select))
            ))
        ))";

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(proxy.Execute(programText, res), IEngineFlat::EStatus::Complete);

        TValue value = TValue::Create(res.GetValue(), res.GetType());
        TValue row1 = value["one"];
        TValue row2 = value["two"];
        TValue row3 = value["three"];
        UNIT_ASSERT_EQUAL(TString(row1["value"]), "one");
        UNIT_ASSERT_EQUAL(TString(row2["value"]), "two");
        UNIT_ASSERT_EQUAL(TString(row3["value"]), "three");

        UNIT_ASSERT(row1["__tablet"].HaveValue() && row1["__updateNo"].HaveValue() && row1["__updateEpoch"].HaveValue());
        UNIT_ASSERT(row2["__tablet"].HaveValue() && row2["__updateNo"].HaveValue() && row2["__updateEpoch"].HaveValue());
        UNIT_ASSERT(row3["__tablet"].HaveValue() && row3["__updateNo"].HaveValue() && row3["__updateEpoch"].HaveValue());

        Cout << "row1 tablet: " << (ui64)row1["__tablet"]
            << " update: " << (ui64)row1["__updateEpoch"] << '.' << (ui64)row1["__updateNo"] << Endl;
        Cout << "row2 tablet: " << (ui64)row2["__tablet"]
            << " update: " << (ui64)row1["__updateEpoch"] << '.' << (ui64)row2["__updateNo"] << Endl;
        Cout << "row3 tablet: " << (ui64)row3["__tablet"]
            << " update: " << (ui64)row1["__updateEpoch"] << '.' << (ui64)row3["__updateNo"] << Endl;

        UNIT_ASSERT_VALUES_EQUAL((ui64)row1["__tablet"], (ui64)row2["__tablet"]);
        UNIT_ASSERT_VALUES_EQUAL((ui64)row1["__tablet"], (ui64)row3["__tablet"]);
        UNIT_ASSERT_VALUES_EQUAL((ui64)row1["__updateEpoch"], (ui64)row2["__updateEpoch"]);
        UNIT_ASSERT_VALUES_EQUAL((ui64)row1["__updateNo"], (ui64)row2["__updateNo"]);
        UNIT_ASSERT((ui64)row1["__updateNo"] < (ui64)row3["__updateNo"]);
        UNIT_ASSERT((ui64)row1["__updateEpoch"] <= (ui64)row3["__updateEpoch"]);
        UNIT_ASSERT((ui64)row1["__tablet"] != Max<ui64>() && (ui64)row1["__tablet"] != 0);
        UNIT_ASSERT((ui64)row1["__updateEpoch"] != Max<ui64>());
    }
}

//

Y_UNIT_TEST(ReadNonExisting) {
    TTester t(TTester::ESchema_KV);
    TFakeMiniKQLProxy proxy(t);

    {
        auto programText = R"___((
            (let row '('('key (Uint32 '42))))
            (let select '('value))
            (let pgmReturn (AsList
            (SetResult 'myRes (SelectRow 'table1 row select))
            ))
            (return pgmReturn)
        ))___";

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(proxy.Execute(programText, res), IEngineFlat::EStatus::Complete);

        TValue value = TValue::Create(res.GetValue(), res.GetType());
        TValue row = value["myRes"];
        UNIT_ASSERT(!row.HaveValue());
    }
}

Y_UNIT_TEST(WriteEraseRead) {
    TTester t(TTester::ESchema_KV);
    TFakeMiniKQLProxy proxy(t);

    {
        auto programText = R"___((
            (let row '('('key (Uint32 '42))))
            (let myUpd '(
                '('value (Utf8 'Robert))))
            (let pgmReturn (AsList
            (UpdateRow 'table1 row myUpd)
            ))
            (return pgmReturn)
        ))___";

        UNIT_ASSERT_EQUAL(proxy.Execute(programText), IEngineFlat::EStatus::Complete);
    }

    {
        auto programText = R"___((
            (let row '('('key (Uint32 '42))))
            (let pgmReturn (AsList
            (EraseRow 'table1 row)
            ))
            (return pgmReturn)
        ))___";

        UNIT_ASSERT_EQUAL(proxy.Execute(programText), IEngineFlat::EStatus::Complete);
    }

    {
        auto programText = R"___((
            (let row '('('key (Uint32 '42))))
            (let select '('value))
            (let pgmReturn (AsList
            (SetResult 'myRes (SelectRow 'table1 row select))
            ))
            (return pgmReturn)
        ))___";

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(proxy.Execute(programText, res), IEngineFlat::EStatus::Complete);

        TValue value = TValue::Create(res.GetValue(), res.GetType());
        TValue row = value["myRes"];
        UNIT_ASSERT(!row.HaveValue());
    }
}

//

Y_UNIT_TEST(SelectRange) {
    TTester t(TTester::ESchema_KV);
    TFakeMiniKQLProxy proxy(t);

    {
        auto programText = R"___((
            (let row1 '('('key (Uint32 '345))))
            (let row2 '('('key (Uint32 '346))))
            (let row3 '('('key (Uint32 '347))))
            (let myUpd1 '(
                '('value (Utf8 'Robert))))
            (let myUpd2 '(
                '('value (Utf8 'Tables))))
            (let myUpd3 '(
                '('value (Utf8 'Paulson))))
            (let pgmReturn (AsList
            (UpdateRow 'table1 row1 myUpd1)
            (UpdateRow 'table1 row2 myUpd2)
            (UpdateRow 'table1 row3 myUpd3)
            ))
            (return pgmReturn)
        ))___";

        UNIT_ASSERT_EQUAL(proxy.Execute(programText), IEngineFlat::EStatus::Complete);
    }

    { // Erase one row
        auto programText = R"___((
            (let row '('('key (Uint32 '346))))
            (let pgmReturn (AsList
            (EraseRow 'table1 row)
            ))
            (return pgmReturn)
        ))___";

        UNIT_ASSERT_EQUAL(proxy.Execute(programText), IEngineFlat::EStatus::Complete);
    }

    {
        auto programText = R"___((
            (let range '('ExcFrom '('key (Uint32 '2) (Void))))
            (let select '('key 'value))
            (let options '())
            (let pgmReturn (AsList
            (SetResult 'myRes (SelectRange 'table1 range select options))
            ))
            (return pgmReturn)
        ))___";

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(proxy.Execute(programText, res), IEngineFlat::EStatus::Complete);

        TValue value = TValue::Create(res.GetValue(), res.GetType());
        TValue rs0 = value["myRes"];
        UNIT_ASSERT_EQUAL(bool(rs0["Truncated"]), false);
        TValue rsl = rs0["List"];
        UNIT_ASSERT_EQUAL(rsl.Size(), 2);
        TValue row1 = rsl[0];
        TValue row2 = rsl[1];
        UNIT_ASSERT_EQUAL(ui32(row1["key"]), 345);
        UNIT_ASSERT_EQUAL(ui32(row2["key"]), 347);
        UNIT_ASSERT_EQUAL(TString(row1["value"]), "Robert");
        UNIT_ASSERT_EQUAL(TString(row2["value"]), "Paulson");
    }

    {
        auto programText = R"___((
            (let range '('ExcFrom '('key (Uint32 '345) (Void))))
            (let select '('key 'value))
            (let options '())
            (let pgmReturn (AsList
            (SetResult 'myRes (SelectRange 'table1 range select options))
            ))
            (return pgmReturn)
        ))___";

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(proxy.Execute(programText, res), IEngineFlat::EStatus::Complete);

        TValue value = TValue::Create(res.GetValue(), res.GetType());
        TValue rs0 = value["myRes"];
        UNIT_ASSERT_EQUAL(bool(rs0["Truncated"]), false);
        TValue rsl = rs0["List"];
        UNIT_ASSERT_EQUAL(rsl.Size(), 1);
        TValue row2 = rsl[0];
        UNIT_ASSERT_EQUAL(ui32(row2["key"]), 347);
        UNIT_ASSERT_EQUAL(TString(row2["value"]), "Paulson");
    }

    {
        auto programText = R"___((
            (let range '('IncFrom '('key (Uint32 '345) (Void))))
            (let select '('key 'value))
            (let options '())
            (let pgmReturn (AsList
            (SetResult 'myRes (SelectRange 'table1 range select options))
            ))
            (return pgmReturn)
        ))___";

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(proxy.Execute(programText, res), IEngineFlat::EStatus::Complete);

        TValue value = TValue::Create(res.GetValue(), res.GetType());
        TValue rs0 = value["myRes"];
        UNIT_ASSERT_EQUAL(bool(rs0["Truncated"]), false);
        TValue rsl = rs0["List"];
        UNIT_ASSERT_EQUAL(rsl.Size(), 2);
        TValue row1 = rsl[0];
        TValue row2 = rsl[1];
        UNIT_ASSERT_EQUAL(ui32(row1["key"]), 345);
        UNIT_ASSERT_EQUAL(ui32(row2["key"]), 347);
        UNIT_ASSERT_EQUAL(TString(row1["value"]), "Robert");
        UNIT_ASSERT_EQUAL(TString(row2["value"]), "Paulson");
    }

    {
        auto programText = R"___((
            (let range '('IncFrom 'IncTo '('key (Uint32 '345) (Uint32 '347))))
            (let select '('key 'value))
            (let options '())
            (let pgmReturn (AsList
            (SetResult 'myRes (SelectRange 'table1 range select options))
            ))
            (return pgmReturn)
        ))___";

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(proxy.Execute(programText, res), IEngineFlat::EStatus::Complete);

        TValue value = TValue::Create(res.GetValue(), res.GetType());
        TValue rs0 = value["myRes"];
        UNIT_ASSERT_EQUAL(bool(rs0["Truncated"]), false);
        TValue rsl = rs0["List"];
        UNIT_ASSERT_EQUAL(rsl.Size(), 2);
        TValue row1 = rsl[0];
        TValue row2 = rsl[1];
        UNIT_ASSERT_EQUAL(ui32(row1["key"]), 345);
        UNIT_ASSERT_EQUAL(ui32(row2["key"]), 347);
        UNIT_ASSERT_EQUAL(TString(row1["value"]), "Robert");
        UNIT_ASSERT_EQUAL(TString(row2["value"]), "Paulson");
    }

    {
        auto programText = R"___((
            (let range '('ExcFrom 'IncTo '('key (Uint32 '345) (Uint32 '347))))
            (let select '('key 'value))
            (let options '())
            (let pgmReturn (AsList
            (SetResult 'myRes (SelectRange 'table1 range select options))
            ))
            (return pgmReturn)
        ))___";

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(proxy.Execute(programText, res), IEngineFlat::EStatus::Complete);

        TValue value = TValue::Create(res.GetValue(), res.GetType());
        TValue rs0 = value["myRes"];
        UNIT_ASSERT_EQUAL(bool(rs0["Truncated"]), false);
        TValue rsl = rs0["List"];
        UNIT_ASSERT_EQUAL(rsl.Size(), 1);
        TValue row2 = rsl[0];
        UNIT_ASSERT_EQUAL(ui32(row2["key"]), 347);
        UNIT_ASSERT_EQUAL(TString(row2["value"]), "Paulson");
    }

    {
        auto programText = R"___((
            (let range '('IncFrom 'ExcTo '('key (Uint32 '345) (Uint32 '347))))
            (let select '('key 'value))
            (let options '())
            (let pgmReturn (AsList
            (SetResult 'myRes (SelectRange 'table1 range select options))
            ))
            (return pgmReturn)
        ))___";

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(proxy.Execute(programText, res), IEngineFlat::EStatus::Complete);

        TValue value = TValue::Create(res.GetValue(), res.GetType());
        auto rs0 = value["myRes"];
        UNIT_ASSERT_EQUAL(bool(rs0["Truncated"]), false);
        auto rsl = rs0["List"];
        UNIT_ASSERT_EQUAL(rsl.Size(), 1);
        auto row1 = rsl[0];
        UNIT_ASSERT_EQUAL(ui32(row1["key"]), 345);
        UNIT_ASSERT_EQUAL(TString(row1["value"]), "Robert");
    }

    {
        auto programText = R"___((
            (let range '('ExcFrom 'ExcTo '('key (Uint32 '345) (Uint32 '347))))
            (let select '('key 'value))
            (let options '())
            (let pgmReturn (AsList
            (SetResult 'myRes (SelectRange 'table1 range select options))
            ))
            (return pgmReturn)
        ))___";

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(proxy.Execute(programText, res), IEngineFlat::EStatus::Complete);

        TValue value = TValue::Create(res.GetValue(), res.GetType());
        TValue rs0 = value["myRes"];
        UNIT_ASSERT_EQUAL(bool(rs0["Truncated"]), false);
        TValue rsl = rs0["List"];
        UNIT_ASSERT_EQUAL(rsl.Size(), 0);
    }
}

//

Y_UNIT_TEST(SelectRangeWithNotFullKey) {
    TTester t(TTester::ESchema_DoubleKV);
    TFakeMiniKQLProxy proxy(t);

    {
        auto programText = R"___((
            (let row1 '('('key1 (Uint32 '345)) '('key2 (Utf8 'id123))))
            (let row2 '('('key1 (Uint32 '346)) '('key2 (Utf8 'id345))))
            (let row3 '('('key1 (Uint32 '347)) '('key2 (Utf8 'idXYZ))))
            (let row4 '('('key1 (Uint32 '348)) '('key2 (Utf8 'idABC))))
            (let myUpd1 '(
                '('value (Utf8 'Robert))))
            (let myUpd2 '(
                '('value (Utf8 'Tables))))
            (let myUpd3 '(
                '('value (Utf8 'Paulson))))
            (let myUpd4 '(
                '('value (Utf8 'Drop))))
            (let pgmReturn (AsList
            (UpdateRow 'table2 row1 myUpd1)
            (UpdateRow 'table2 row2 myUpd2)
            (UpdateRow 'table2 row3 myUpd3)
            (UpdateRow 'table2 row4 myUpd4)
            ))
            (return pgmReturn)
        ))___";

        UNIT_ASSERT_EQUAL(proxy.Execute(programText), IEngineFlat::EStatus::Complete);
    }

    { // range end < range begin
        auto programText = R"___((
            (let $4 (Uint32 '"346"))
            (let $5 '('"key1" $4 $4))
            (let $6 (Utf8 '"id345"))
            (let $7 (Utf8 '"id344"))
            (let $8 '('"key2" $6 $7))
            (let $9 '('"ExcFrom" '"IncTo" $5 $8))
            (let $10 '('"key1" '"key2" '"value"))
            (let $11 '())
            (let $12 (SelectRange '"table2" $9 $10 $11))
            (let $17 (SetResult '"Result" $12))
            (let $18 (AsList $17))
            (return $18)
        ))___";

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(proxy.Execute(programText, res), IEngineFlat::EStatus::Complete);

        TValue value = TValue::Create(res.GetValue(), res.GetType());
        TValue rs0 = value["Result"];
        UNIT_ASSERT_EQUAL(bool(rs0["Truncated"]), false);
        TValue rsl = rs0["List"];
        UNIT_ASSERT_EQUAL(rsl.Size(), 0);
    }

    {
        auto programText = R"___((
            (let r1 '('key1 (Uint32 '345) (Uint32 '347)))
            (let r2 '('key2 (Void) (Void)))
            (let range '('ExcFrom 'IncTo r1 r2))
            (let select '('key1 'key2 'value))
            (let options '())
            (let pgmReturn (AsList
            (SetResult 'myRes (SelectRange 'table2 range select options))
            ))
            (return pgmReturn)
        ))___";

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(proxy.Execute(programText, res), IEngineFlat::EStatus::Complete);

        TValue value = TValue::Create(res.GetValue(), res.GetType());
        TValue rs0 = value["myRes"];
        UNIT_ASSERT_EQUAL(bool(rs0["Truncated"]), false);
        TValue rsl = rs0["List"];
        UNIT_ASSERT_EQUAL(rsl.Size(), 2);
        TValue row1 = rsl[0];
        TValue row2 = rsl[1];
        UNIT_ASSERT_EQUAL(ui32(row1["key1"]), 346);
        UNIT_ASSERT_EQUAL(ui32(row2["key1"]), 347);
        UNIT_ASSERT_EQUAL(TString(row1["key2"]), "id345");
        UNIT_ASSERT_EQUAL(TString(row2["key2"]), "idXYZ");
        UNIT_ASSERT_EQUAL(TString(row1["value"]), "Tables");
        UNIT_ASSERT_EQUAL(TString(row2["value"]), "Paulson");
    }

    {
        auto programText = R"___((
            (let r1 '('key1 (Uint32 '345) (Uint32 '347)))
            (let r2 '('key2 (Nothing (OptionalType (DataType 'Utf8))) (Nothing (OptionalType (DataType 'Utf8)))))
            (let range '('IncFrom 'ExcTo r1 r2))
            (let select '('key1 'key2 'value))
            (let options '())
            (let pgmReturn (AsList
            (SetResult 'myRes (SelectRange 'table2 range select options))
            ))
            (return pgmReturn)
        ))___";

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(proxy.Execute(programText, res), IEngineFlat::EStatus::Complete);

        TValue value = TValue::Create(res.GetValue(), res.GetType());
        TValue rs0 = value["myRes"];
        UNIT_ASSERT_EQUAL(bool(rs0["Truncated"]), false);
        TValue rsl = rs0["List"];
        UNIT_ASSERT_EQUAL(rsl.Size(), 2);
        TValue row1 = rsl[0];
        TValue row2 = rsl[1];
        UNIT_ASSERT_EQUAL(ui32(row1["key1"]), 345);
        UNIT_ASSERT_EQUAL(ui32(row2["key1"]), 346);
        UNIT_ASSERT_EQUAL(TString(row1["key2"]), "id123");
        UNIT_ASSERT_EQUAL(TString(row2["key2"]), "id345");
        UNIT_ASSERT_EQUAL(TString(row1["value"]), "Robert");
        UNIT_ASSERT_EQUAL(TString(row2["value"]), "Tables");
    }

    {
        auto programText = R"___((
            (let r1 '('key1 (Uint32 '345) (Uint32 '347)))
            (let r2 '('key2 (Void) (Nothing (OptionalType (DataType 'Utf8)))))
            (let range '('ExcFrom 'ExcTo r1 r2))
            (let select '('key1 'key2 'value))
            (let options '())
            (let pgmReturn (AsList
            (SetResult 'myRes (SelectRange 'table2 range select options))
            ))
            (return pgmReturn)
        ))___";

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(proxy.Execute(programText, res), IEngineFlat::EStatus::Complete);

        TValue value = TValue::Create(res.GetValue(), res.GetType());
        TValue rs0 = value["myRes"];
        UNIT_ASSERT_EQUAL(bool(rs0["Truncated"]), false);
        TValue rsl = rs0["List"];
        UNIT_ASSERT_EQUAL(rsl.Size(), 1);
        TValue row1 = rsl[0];
        UNIT_ASSERT_EQUAL(ui32(row1["key1"]), 346);
        UNIT_ASSERT_EQUAL(TString(row1["key2"]), "id345");
        UNIT_ASSERT_EQUAL(TString(row1["value"]), "Tables");
    }

    {
        auto programText = R"___((
            (let r1 '('key1 (Uint32 '345) (Uint32 '347)))
            (let r2 '('key2 (Nothing (OptionalType (DataType 'Utf8))) (Void)))
            (let range '('IncFrom 'IncTo r1 r2))
            (let select '('key1 'key2 'value))
            (let options '())
            (let pgmReturn (AsList
            (SetResult 'myRes (SelectRange 'table2 range select options))
            ))
            (return pgmReturn)
        ))___";

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(proxy.Execute(programText, res), IEngineFlat::EStatus::Complete);

        TValue value = TValue::Create(res.GetValue(), res.GetType());
        auto rs0 = value["myRes"];
        UNIT_ASSERT_EQUAL(bool(rs0["Truncated"]), false);
        auto rsl = rs0["List"];
        UNIT_ASSERT_EQUAL(rsl.Size(), 3);
        auto row1 = rsl[0];
        auto row2 = rsl[1];
        auto row3 = rsl[2];
        UNIT_ASSERT_EQUAL(ui32(row1["key1"]), 345);
        UNIT_ASSERT_EQUAL(ui32(row2["key1"]), 346);
        UNIT_ASSERT_EQUAL(ui32(row3["key1"]), 347);
        UNIT_ASSERT_EQUAL(TString(row1["key2"]), "id123");
        UNIT_ASSERT_EQUAL(TString(row2["key2"]), "id345");
        UNIT_ASSERT_EQUAL(TString(row3["key2"]), "idXYZ");
        UNIT_ASSERT_EQUAL(TString(row1["value"]), "Robert");
        UNIT_ASSERT_EQUAL(TString(row2["value"]), "Tables");
        UNIT_ASSERT_EQUAL(TString(row3["value"]), "Paulson");
    }

    // (345,inf).. (347,inf)
    {
        auto programText = R"___((
            (let r1 '('key1 (Uint32 '345) (Uint32 '347)))
            (let range '('ExcFrom 'IncTo r1))
            (let select '('key1 'key2 'value))
            (let options '())
            (let pgmReturn (AsList
            (SetResult 'myRes (SelectRange 'table2 range select options))
            ))
            (return pgmReturn)
        ))___";

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(proxy.Execute(programText, res), IEngineFlat::EStatus::Complete);

        TValue value = TValue::Create(res.GetValue(), res.GetType());
        auto rs0 = value["myRes"];
        UNIT_ASSERT_EQUAL(bool(rs0["Truncated"]), false);
        auto rsl = rs0["List"];
        UNIT_ASSERT_EQUAL(rsl.Size(), 2);
        auto row1 = rsl[0];
        auto row2 = rsl[1];
        UNIT_ASSERT_EQUAL(ui32(row1["key1"]), 346);
        UNIT_ASSERT_EQUAL(ui32(row2["key1"]), 347);
        UNIT_ASSERT_EQUAL(TString(row1["key2"]), "id345");
        UNIT_ASSERT_EQUAL(TString(row2["key2"]), "idXYZ");
        UNIT_ASSERT_EQUAL(TString(row1["value"]), "Tables");
        UNIT_ASSERT_EQUAL(TString(row2["value"]), "Paulson");
    }
}

//

Y_UNIT_TEST(WriteAndReadMultipleShards) {
    TTester t(TTester::ESchema_MultiShardKV);
    TFakeMiniKQLProxy proxy(t);

    {
        auto programText = R"___((
            (let row1 '('('key (Uint32 '100))))
            (let row2 '('('key (Uint32 '1100))))
            (let row3 '('('key (Uint32 '2100))))
            (let myUpd1 '(
                '('value (Utf8 'ImInShard1))))
            (let myUpd2 '(
                '('value (Utf8 'ImInShard2))))
            (let myUpd3 '(
                '('value (Utf8 'ImInShard3))))
            (let pgmReturn (AsList
            (UpdateRow 'table1 row1 myUpd1)
            (UpdateRow 'table1 row2 myUpd2)
            (UpdateRow 'table1 row3 myUpd3)
            ))
            (return pgmReturn)
        ))___";

        UNIT_ASSERT_EQUAL(proxy.Execute(programText), IEngineFlat::EStatus::Complete);
    }

    {
        auto programText = R"___((
            (let row1 '('('key (Uint32 '100))))
            (let row2 '('('key (Uint32 '1100))))
            (let row3 '('('key (Uint32 '2100))))
            (let select '('value))
            (let pgmReturn (AsList
            (SetResult 'myRes (AsList
                (SelectRow 'table1 row1 select)
                (SelectRow 'table1 row2 select)
                (SelectRow 'table1 row3 select)
            ))
            ))
            (return pgmReturn)
        ))___";

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(proxy.Execute(programText, res), IEngineFlat::EStatus::Complete);

        TValue value = TValue::Create(res.GetValue(), res.GetType());
        auto rl = value["myRes"];
        auto row1 = rl[0];
        auto row2 = rl[1];
        auto row3 = rl[2];
        UNIT_ASSERT_EQUAL(TString(row1["value"]), "ImInShard1");
        UNIT_ASSERT_EQUAL(TString(row2["value"]), "ImInShard2");
        UNIT_ASSERT_EQUAL(TString(row3["value"]), "ImInShard3");
    }
}

//

void GetTableStats(TTestActorRuntime &runtime, ui64 tabletId, ui64 tableId, NKikimrTableStats::TTableStats& stats) {
    TAutoPtr<TEvDataShard::TEvGetTableStats> ev(new TEvDataShard::TEvGetTableStats(tableId));

    TActorId edge = runtime.AllocateEdgeActor();
    runtime.SendToPipe(tabletId, edge, ev.Release());
    TAutoPtr<IEventHandle> handle;
    TEvDataShard::TEvGetTableStatsResult* response = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvGetTableStatsResult>(handle);
    stats.Swap(response->Record.MutableTableStats());
}

Y_UNIT_TEST(TableStats) {
    TTester t(TTester::ESchema_MultiShardKV);
    TFakeMiniKQLProxy proxy(t);

    ui64 tableId = 13;
    ui64 ds1 = TTestTxConfig::TxTablet0;

    {
        auto programText = R"___((
            (let row1 '('('key (Uint32 '100))))
            (let row2 '('('key (Uint32 '1100))))
            (let row3 '('('key (Uint32 '2100))))
            (let myUpd1 '(
                '('value (Utf8 'ImInShard1))))
            (let myUpd2 '(
                '('value (Utf8 'ImInShard2))))
            (let myUpd3 '(
                '('value (Utf8 'ImInShard3))))
            (let pgmReturn (AsList
                (UpdateRow 'table1 row1 myUpd1)
                (UpdateRow 'table1 row2 myUpd2)
                (UpdateRow 'table1 row3 myUpd3)
            ))
            (return pgmReturn)
        ))___";

        UNIT_ASSERT_EQUAL(proxy.Execute(programText), IEngineFlat::EStatus::Complete);
    }

    NKikimrTableStats::TTableStats statsAfterUpdate;
    GetTableStats(t.Runtime, ds1, tableId, statsAfterUpdate);
    Cerr << statsAfterUpdate << Endl;
    UNIT_ASSERT_C(statsAfterUpdate.GetIndexSize() > 0 || statsAfterUpdate.GetInMemSize() > 0, "Table shoud not be empty");
    UNIT_ASSERT_C(statsAfterUpdate.GetLastAccessTime() > 0, "LastAccessTime wasn't set");
    UNIT_ASSERT_C(statsAfterUpdate.GetLastUpdateTime() > 0, "LastUpdateTime wasn't set");
    UNIT_ASSERT_VALUES_EQUAL_C(statsAfterUpdate.GetLastAccessTime(), statsAfterUpdate.GetLastUpdateTime(),
                               "After update LastAccessTime should be equal to LastUpdateTime ");

    // Non-existing table
    NKikimrTableStats::TTableStats stats;
    GetTableStats(t.Runtime, ds1, tableId+42, stats);
    Cerr << stats << Endl;
    UNIT_ASSERT_C(!stats.HasIndexSize() && !stats.HasInMemSize(), "Unknown table shouldn't have stats");

    {
        auto programText = R"___((
            (let row1 '('('key (Uint32 '100))))
            (let row2 '('('key (Uint32 '1100))))
            (let row3 '('('key (Uint32 '2100))))
            (let select '('value))
            (let pgmReturn (AsList
                (SetResult 'myRes (AsList
                    (SelectRow 'table1 row1 select)
                    (SelectRow 'table1 row2 select)
                    (SelectRow 'table1 row3 select)
                ))
            ))
            (return pgmReturn)
        ))___";

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(proxy.Execute(programText, res), IEngineFlat::EStatus::Complete);

        TValue value = TValue::Create(res.GetValue(), res.GetType());
        TValue rl = value["myRes"];
        TValue row1 = rl[0];
        TValue row2 = rl[1];
        TValue row3 = rl[2];
        UNIT_ASSERT_EQUAL(TString(row1["value"]), "ImInShard1");
        UNIT_ASSERT_EQUAL(TString(row2["value"]), "ImInShard2");
        UNIT_ASSERT_EQUAL(TString(row3["value"]), "ImInShard3");
    }

    NKikimrTableStats::TTableStats statsAfterRead;
    GetTableStats(t.Runtime, ds1, tableId, statsAfterRead);
    Cerr << statsAfterRead << Endl;
    UNIT_ASSERT_VALUES_EQUAL_C(statsAfterRead.GetLastUpdateTime(), statsAfterUpdate.GetLastUpdateTime(),
                               "LastUpdateTime shoud not change after read");
    UNIT_ASSERT_C(statsAfterRead.GetLastAccessTime() > statsAfterUpdate.GetLastAccessTime(),
                  "LastAccessTime should change after read");
}

Y_UNIT_TEST(TableStatsHistograms) {
    TTester t(TTester::ESchema_MultiShardKV);
    TFakeMiniKQLProxy proxy(t);

    ui64 tableId = 13;
    ui64 ds1 = TTestTxConfig::TxTablet0;

    for (int i = 0; i < 1000; ++i) {
        auto programText = R"___((
            (let row1 '('('key (Uint32 '%d))))
            (let row2 '('('key (Uint32 '%d))))
            (let row3 '('('key (Uint32 '%d))))
            (let myUpd1 '(
                '('value (Utf8 'ImInShard111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111))))
            (let myUpd2 '(
                '('value (Utf8 'ImInShard222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222))))
            (let myUpd3 '(
                '('value (Utf8 'ImInShard333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333))))
            (let pgmReturn (AsList
                (UpdateRow 'table1 row1 myUpd1)
#                  (UpdateRow 'table1 row2 myUpd2)
#                  (UpdateRow 'table1 row3 myUpd3)
            ))
            (return pgmReturn)
        ))___";

        TString query = Sprintf(programText, i, 1+1000, i+2000);

        UNIT_ASSERT_EQUAL(proxy.Execute(query), IEngineFlat::EStatus::Complete);
        Cerr << ".";
    }

    NKikimrTableStats::TTableStats statsAfterUpdate;
    GetTableStats(t.Runtime, ds1, tableId, statsAfterUpdate);
    Cerr << statsAfterUpdate << Endl;
}

//

void InitCrossShard(TFakeMiniKQLProxy& proxy) {
    auto programText = R"___((
        (let row1 '('('key (Uint32 '100))))
        (let row2 '('('key (Uint32 '1100))))
        (let row3 '('('key (Uint32 '2100))))
        (let myUpd1 '(
            '('value (Utf8 'ImInShard1))))
        (let myUpd2 '(
            '('value (Utf8 'ImInShard2))))
        (let myUpd3 '(
            '('value (Utf8 'ImInShard3))))
        (let pgmReturn (AsList
        (UpdateRow 'table1 row1 myUpd1)
        (UpdateRow 'table1 row2 myUpd2)
        (UpdateRow 'table1 row3 myUpd3)
        ))
        (return pgmReturn)
    ))___";

    UNIT_ASSERT_EQUAL(proxy.Execute(programText), IEngineFlat::EStatus::Complete);
}

void CrossShard_1_Cycle_Impl(const TString& dispatchName, std::function<void (TTestActorRuntime&)> setup, bool& activeZone) {
    TTester t(TTester::ESchema_MultiShardKV, dispatchName, setup, activeZone);
    TFakeMiniKQLProxy proxy(t);

    InitCrossShard(proxy);

    {
        TTester::TActiveZone az(t);
        auto programText = R"___((
            (let row1 '('('key (Uint32 '100))))
            (let row2 '('('key (Uint32 '1100))))
            (let row3 '('('key (Uint32 '2100))))
            (let select '('value))
            (let val1 (FlatMap (SelectRow 'table1 row1 select) (lambda '(x) (Member x 'value))))
            (let val2 (FlatMap (SelectRow 'table1 row2 select) (lambda '(x) (Member x 'value))))
            (let val3 (FlatMap (SelectRow 'table1 row3 select) (lambda '(x) (Member x 'value))))
            (let upd1 '('('value val1)))
            (let upd2 '('('value val2)))
            (let upd3 '('('value val3)))
            (let pgmReturn (AsList
            (UpdateRow 'table1 row1 upd3)
            (UpdateRow 'table1 row2 upd1)
            (UpdateRow 'table1 row3 upd2)
            ))
            (return pgmReturn)
        ))___";

        proxy.Execute(programText, false);
    }

    {
        auto programText = R"___((
            (let row1 '('('key (Uint32 '100))))
            (let row2 '('('key (Uint32 '1100))))
            (let row3 '('('key (Uint32 '2100))))
            (let select '('value))
            (let pgmReturn (AsList
            (SetResult 'myRes (AsList
                (SelectRow 'table1 row1 select)
                (SelectRow 'table1 row2 select)
                (SelectRow 'table1 row3 select)
            ))
            ))
            (return pgmReturn)
        ))___";

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(proxy.Execute(programText, res), IEngineFlat::EStatus::Complete);

        TValue value = TValue::Create(res.GetValue(), res.GetType());
        TValue rl = value["myRes"];
        TValue row1 = rl[0];
        TValue row2 = rl[1];
        TValue row3 = rl[2];
        UNIT_ASSERT_EQUAL(TString(row1["value"]), "ImInShard3");
        UNIT_ASSERT_EQUAL(TString(row2["value"]), "ImInShard1");
        UNIT_ASSERT_EQUAL(TString(row3["value"]), "ImInShard2");
    }
}

Y_UNIT_TEST(CrossShard_1_Cycle) {
    TVector<ui64> tabletIds;
    tabletIds.push_back((ui64)TTestTxConfig::TxTablet0);
    tabletIds.push_back((ui64)TTestTxConfig::TxTablet1);
    tabletIds.push_back((ui64)TTestTxConfig::TxTablet2);
    TDatashardInitialEventsFilter initialEventsFilter(tabletIds);
    RunTestWithReboots(tabletIds, [&]() {
        return initialEventsFilter.Prepare();
    }, &CrossShard_1_Cycle_Impl);
}

//

void CrossShard_2_SwapAndCopy_Impl(const TString& dispatchName, std::function<void (TTestActorRuntime&)> setup, bool& activeZone) {
    TTester t(TTester::ESchema_MultiShardKV, dispatchName, setup, activeZone);
    TFakeMiniKQLProxy proxy(t);

    InitCrossShard(proxy);

    {
        TTester::TActiveZone az(t);
        auto programText = R"___((
            (let row1 '('('key (Uint32 '100))))
            (let row2 '('('key (Uint32 '1100))))
            (let row3 '('('key (Uint32 '2100))))
            (let row3new '('('key (Uint32 '2200))))
            (let select '('value))
            (let val1 (FlatMap (SelectRow 'table1 row1 select) (lambda '(x) (Member x 'value))))
            (let val2 (FlatMap (SelectRow 'table1 row2 select) (lambda '(x) (Member x 'value))))
            (let val3 (FlatMap (SelectRow 'table1 row3 select) (lambda '(x) (Member x 'value))))
            (let upd1 '('('value val1)))
            (let upd2 '('('value val2)))
            (let upd3 '('('value val3)))
            (let pgmReturn (AsList
            (UpdateRow 'table1 row1 upd2)
            (UpdateRow 'table1 row2 upd1)
            (UpdateRow 'table1 row3new upd3)
            ))
            (return pgmReturn)
        ))___";

        proxy.Execute(programText, false);
    }

    {
        auto programText = R"___((
            (let row1 '('('key (Uint32 '100))))
            (let row2 '('('key (Uint32 '1100))))
            (let row3 '('('key (Uint32 '2100))))
            (let row3new '('('key (Uint32 '2200))))
            (let select '('value))
            (let pgmReturn (AsList
            (SetResult 'myRes (AsList
                (SelectRow 'table1 row1 select)
                (SelectRow 'table1 row2 select)
                (SelectRow 'table1 row3 select)
                (SelectRow 'table1 row3new select)
            ))
            ))
            (return pgmReturn)
        ))___";

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(proxy.Execute(programText, res), IEngineFlat::EStatus::Complete);
        TValue value = TValue::Create(res.GetValue(), res.GetType());
        TValue rl = value["myRes"];
        TValue row1 = rl[0];
        TValue row2 = rl[1];
        TValue row3 = rl[2];
        TValue row3new = rl[3];
        UNIT_ASSERT_EQUAL(TString(row1["value"]), "ImInShard2");
        UNIT_ASSERT_EQUAL(TString(row2["value"]), "ImInShard1");
        UNIT_ASSERT_EQUAL(TString(row3["value"]), "ImInShard3");
        UNIT_ASSERT_EQUAL(TString(row3new["value"]), "ImInShard3");
    }
}

Y_UNIT_TEST(CrossShard_2_SwapAndCopy) {
    TVector<ui64> tabletIds;
    tabletIds.push_back((ui64)TTestTxConfig::TxTablet0);
    tabletIds.push_back((ui64)TTestTxConfig::TxTablet1);
    tabletIds.push_back((ui64)TTestTxConfig::TxTablet2);
    TDatashardInitialEventsFilter initialEventsFilter(tabletIds);
    RunTestWithReboots(tabletIds, [&]() {
        return initialEventsFilter.Prepare();
    }, &CrossShard_2_SwapAndCopy_Impl);
}

//

void CrossShard_3_AllToOne_Impl(const TString& dispatchName, std::function<void (TTestActorRuntime&)> setup, bool& activeZone) {
    TTester t(TTester::ESchema_MultiShardKV, dispatchName, setup, activeZone);
    TFakeMiniKQLProxy proxy(t);

    InitCrossShard(proxy);

    {
        TTester::TActiveZone az(t);
        auto programText = R"___((
            (let row1 '('('key (Uint32 '100))))
            (let row2 '('('key (Uint32 '1100))))
            (let row3 '('('key (Uint32 '2100))))
            (let row31 '('('key (Uint32 '2201))))
            (let row32 '('('key (Uint32 '2202))))
            (let row33 '('('key (Uint32 '2203))))
            (let select '('value))
            (let val1 (FlatMap (SelectRow 'table1 row1 select) (lambda '(x) (Member x 'value))))
            (let val2 (FlatMap (SelectRow 'table1 row2 select) (lambda '(x) (Member x 'value))))
            (let val3 (FlatMap (SelectRow 'table1 row3 select) (lambda '(x) (Member x 'value))))
            (let upd1 '('('value val1)))
            (let upd2 '('('value val2)))
            (let upd3 '('('value val3)))
            (let pgmReturn (AsList
            (UpdateRow 'table1 row31 upd1)
            (UpdateRow 'table1 row32 upd2)
            (UpdateRow 'table1 row33 upd3)
            ))
            (return pgmReturn)
        ))___";

        proxy.Execute(programText, false);
    }

    {
        auto programText = R"___((
            (let row1 '('('key (Uint32 '100))))
            (let row31 '('('key (Uint32 '2201))))
            (let row32 '('('key (Uint32 '2202))))
            (let row33 '('('key (Uint32 '2203))))
            (let select '('value))
            (let pgmReturn (AsList
            (SetResult 'myRes (AsList
                (SelectRow 'table1 row1 select)
                (SelectRow 'table1 row31 select)
                (SelectRow 'table1 row32 select)
                (SelectRow 'table1 row33 select)
            ))
            ))
            (return pgmReturn)
        ))___";

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(proxy.Execute(programText, res), IEngineFlat::EStatus::Complete);

        TValue value = TValue::Create(res.GetValue(), res.GetType());
        TValue rl = value["myRes"];
        TValue row31 = rl[1];
        TValue row32 = rl[2];
        TValue row33 = rl[3];
        UNIT_ASSERT_EQUAL(TString(row31["value"]), "ImInShard1");
        UNIT_ASSERT_EQUAL(TString(row32["value"]), "ImInShard2");
        UNIT_ASSERT_EQUAL(TString(row33["value"]), "ImInShard3");
    }
}

Y_UNIT_TEST(CrossShard_3_AllToOne) {
    TVector<ui64> tabletIds;
    tabletIds.push_back((ui64)TTestTxConfig::TxTablet0);
    tabletIds.push_back((ui64)TTestTxConfig::TxTablet1);
    tabletIds.push_back((ui64)TTestTxConfig::TxTablet2);
    TDatashardInitialEventsFilter initialEventsFilter(tabletIds);
    RunTestWithReboots(tabletIds, [&]() {
        return initialEventsFilter.Prepare();
    }, &CrossShard_3_AllToOne_Impl);
}

//

void CrossShard_4_OneToAll_Impl(const TString& dispatchName, std::function<void (TTestActorRuntime&)> setup, bool& activeZone) {
    TTester t(TTester::ESchema_MultiShardKV, dispatchName, setup, activeZone);
    TFakeMiniKQLProxy proxy(t);

    InitCrossShard(proxy);

    {
        TTester::TActiveZone az(t);
        auto programText = R"___((
            (let row1 '('('key (Uint32 '100))))
            (let row2 '('('key (Uint32 '1100))))
            (let row3 '('('key (Uint32 '2100))))
            (let row13 '('('key (Uint32 '203))))
            (let row23 '('('key (Uint32 '1203))))
            (let row33 '('('key (Uint32 '2203))))
            (let select '('value))
            (let val1 (FlatMap (SelectRow 'table1 row1 select) (lambda '(x) (Member x 'value))))
            (let val2 (FlatMap (SelectRow 'table1 row2 select) (lambda '(x) (Member x 'value))))
            (let val3 (FlatMap (SelectRow 'table1 row3 select) (lambda '(x) (Member x 'value))))
            (let upd1 '('('value val1)))
            (let upd2 '('('value val2)))
            (let upd3 '('('value val3)))
            (let pgmReturn (AsList
            (UpdateRow 'table1 row13 upd3)
            (UpdateRow 'table1 row23 upd3)
            (UpdateRow 'table1 row33 upd3)
            ))
            (return pgmReturn)
        ))___";

        proxy.Execute(programText, false);
    }

    {
        auto programText = R"___((
            (let row13 '('('key (Uint32 '203))))
            (let row23 '('('key (Uint32 '1203))))
            (let row33 '('('key (Uint32 '2203))))
            (let select '('value))
            (let pgmReturn (AsList
            (SetResult 'myRes (AsList
                (SelectRow 'table1 row13 select)
                (SelectRow 'table1 row23 select)
                (SelectRow 'table1 row33 select)
            ))
            ))
            (return pgmReturn)
        ))___";

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(proxy.Execute(programText, res), IEngineFlat::EStatus::Complete);

        TValue value = TValue::Create(res.GetValue(), res.GetType());
        TValue rl = value["myRes"];
        TValue row13 = rl[0];
        TValue row23 = rl[1];
        TValue row33 = rl[2];
        UNIT_ASSERT_EQUAL(TString(row13["value"]), "ImInShard3");
        UNIT_ASSERT_EQUAL(TString(row23["value"]), "ImInShard3");
        UNIT_ASSERT_EQUAL(TString(row33["value"]), "ImInShard3");
    }
}

Y_UNIT_TEST(CrossShard_4_OneToAll) {
    TVector<ui64> tabletIds;
    tabletIds.push_back((ui64)TTestTxConfig::TxTablet0);
    tabletIds.push_back((ui64)TTestTxConfig::TxTablet1);
    tabletIds.push_back((ui64)TTestTxConfig::TxTablet2);
    TDatashardInitialEventsFilter initialEventsFilter(tabletIds);
    RunTestWithReboots(tabletIds, [&]() {
        return initialEventsFilter.Prepare();
    }, &CrossShard_4_OneToAll_Impl);
}

//

void CrossShard_5_AllToAll_Impl(const TString& dispatchName, std::function<void (TTestActorRuntime&)> setup, bool& activeZone) {
    TTester t(TTester::ESchema_MultiShardKV, dispatchName, setup, activeZone);
    TFakeMiniKQLProxy proxy(t);

    InitCrossShard(proxy);

    {
        TTester::TActiveZone az(t);
        auto programText = R"___((
            (let row1 '('('key (Uint32 '100))))
            (let row2 '('('key (Uint32 '1100))))
            (let row3 '('('key (Uint32 '2100))))
            (let row11 '('('key (Uint32 '201))))
            (let row12 '('('key (Uint32 '202))))
            (let row13 '('('key (Uint32 '203))))
            (let row21 '('('key (Uint32 '1201))))
            (let row22 '('('key (Uint32 '1202))))
            (let row23 '('('key (Uint32 '1203))))
            (let row31 '('('key (Uint32 '2201))))
            (let row32 '('('key (Uint32 '2202))))
            (let row33 '('('key (Uint32 '2203))))
            (let select '('value))
            (let val1 (FlatMap (SelectRow 'table1 row1 select) (lambda '(x) (Member x 'value))))
            (let val2 (FlatMap (SelectRow 'table1 row2 select) (lambda '(x) (Member x 'value))))
            (let val3 (FlatMap (SelectRow 'table1 row3 select) (lambda '(x) (Member x 'value))))
            (let upd1 '('('value val1)))
            (let upd2 '('('value val2)))
            (let upd3 '('('value val3)))
            (let pgmReturn (AsList
            (UpdateRow 'table1 row11 upd1)
            (UpdateRow 'table1 row12 upd2)
            (UpdateRow 'table1 row13 upd3)
            (UpdateRow 'table1 row21 upd1)
            (UpdateRow 'table1 row22 upd2)
            (UpdateRow 'table1 row23 upd3)
            (UpdateRow 'table1 row31 upd1)
            (UpdateRow 'table1 row32 upd2)
            (UpdateRow 'table1 row33 upd3)
            ))
            (return pgmReturn)
        ))___";

        proxy.Execute(programText, false);
    }

    {
        auto programText = R"___((
            (let row11 '('('key (Uint32 '201))))
            (let row12 '('('key (Uint32 '202))))
            (let row13 '('('key (Uint32 '203))))
            (let row21 '('('key (Uint32 '1201))))
            (let row22 '('('key (Uint32 '1202))))
            (let row23 '('('key (Uint32 '1203))))
            (let row31 '('('key (Uint32 '2201))))
            (let row32 '('('key (Uint32 '2202))))
            (let row33 '('('key (Uint32 '2203))))
            (let select '('value))
            (let pgmReturn (AsList
            (SetResult 'myRes (AsList
                (SelectRow 'table1 row11 select)
                (SelectRow 'table1 row12 select)
                (SelectRow 'table1 row13 select)
                (SelectRow 'table1 row21 select)
                (SelectRow 'table1 row22 select)
                (SelectRow 'table1 row23 select)
                (SelectRow 'table1 row31 select)
                (SelectRow 'table1 row32 select)
                (SelectRow 'table1 row33 select)
            ))
            ))
            (return pgmReturn)
        ))___";

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(proxy.Execute(programText, res), IEngineFlat::EStatus::Complete);

        TValue value = TValue::Create(res.GetValue(), res.GetType());
        TValue rl = value["myRes"];
        TValue row11 = rl[0];
        TValue row12 = rl[1];
        TValue row13 = rl[2];
        TValue row21 = rl[3];
        TValue row22 = rl[4];
        TValue row23 = rl[5];
        TValue row31 = rl[6];
        TValue row32 = rl[7];
        TValue row33 = rl[8];
        UNIT_ASSERT_EQUAL(TString(row11["value"]), "ImInShard1");
        UNIT_ASSERT_EQUAL(TString(row12["value"]), "ImInShard2");
        UNIT_ASSERT_EQUAL(TString(row13["value"]), "ImInShard3");
        UNIT_ASSERT_EQUAL(TString(row21["value"]), "ImInShard1");
        UNIT_ASSERT_EQUAL(TString(row22["value"]), "ImInShard2");
        UNIT_ASSERT_EQUAL(TString(row23["value"]), "ImInShard3");
        UNIT_ASSERT_EQUAL(TString(row31["value"]), "ImInShard1");
        UNIT_ASSERT_EQUAL(TString(row32["value"]), "ImInShard2");
        UNIT_ASSERT_EQUAL(TString(row33["value"]), "ImInShard3");
    }
}

Y_UNIT_TEST(CrossShard_5_AllToAll) {
    TVector<ui64> tabletIds;
    tabletIds.push_back((ui64)TTestTxConfig::TxTablet0);
    tabletIds.push_back((ui64)TTestTxConfig::TxTablet1);
    tabletIds.push_back((ui64)TTestTxConfig::TxTablet2);
    TDatashardInitialEventsFilter initialEventsFilter(tabletIds);
    RunTestWithReboots(tabletIds, [&]() {
        return initialEventsFilter.Prepare();
    }, &CrossShard_5_AllToAll_Impl);
}

//

void CrossShard_6_Local_Impl(const TString& dispatchName, std::function<void (TTestActorRuntime&)> setup, bool& activeZone) {
    TTester t(TTester::ESchema_MultiShardKV, dispatchName, setup, activeZone);
    TFakeMiniKQLProxy proxy(t);

    InitCrossShard(proxy);

    {
        TTester::TActiveZone az(t);
        auto programText = R"___((
            (let row1 '('('key (Uint32 '100))))
            (let row2 '('('key (Uint32 '1100))))
            (let row3 '('('key (Uint32 '2100))))
            (let row11 '('('key (Uint32 '201))))
            (let row22 '('('key (Uint32 '1202))))
            (let row33 '('('key (Uint32 '2203))))
            (let select '('value))
            (let val1 (FlatMap (SelectRow 'table1 row1 select) (lambda '(x) (Member x 'value))))
            (let val2 (FlatMap (SelectRow 'table1 row2 select) (lambda '(x) (Member x 'value))))
            (let val3 (FlatMap (SelectRow 'table1 row3 select) (lambda '(x) (Member x 'value))))
            (let upd1 '('('value val1)))
            (let upd2 '('('value val2)))
            (let upd3 '('('value val3)))
            (let pgmReturn (AsList
            (UpdateRow 'table1 row11 upd1)
            (UpdateRow 'table1 row22 upd2)
            (UpdateRow 'table1 row33 upd3)
            ))
            (return pgmReturn)
        ))___";

        proxy.Execute(programText, false);
    }

    {
        auto programText = R"___((
            (let row11 '('('key (Uint32 '201))))
            (let row22 '('('key (Uint32 '1202))))
            (let row33 '('('key (Uint32 '2203))))
            (let select '('value))
            (let pgmReturn (AsList
            (SetResult 'myRes (AsList
                (SelectRow 'table1 row11 select)
                (SelectRow 'table1 row22 select)
                (SelectRow 'table1 row33 select)
            ))
            ))
            (return pgmReturn)
        ))___";

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(proxy.Execute(programText, res), IEngineFlat::EStatus::Complete);

        TValue value = TValue::Create(res.GetValue(), res.GetType());
        auto rl = value["myRes"];
        auto row11 = rl[0];
        auto row22 = rl[1];
        auto row33 = rl[2];
        UNIT_ASSERT_EQUAL(TString(row11["value"]), "ImInShard1");
        UNIT_ASSERT_EQUAL(TString(row22["value"]), "ImInShard2");
        UNIT_ASSERT_EQUAL(TString(row33["value"]), "ImInShard3");
    }
}

Y_UNIT_TEST(CrossShard_6_Local) {
    TVector<ui64> tabletIds;
    tabletIds.push_back((ui64)TTestTxConfig::TxTablet0);
    tabletIds.push_back((ui64)TTestTxConfig::TxTablet1);
    tabletIds.push_back((ui64)TTestTxConfig::TxTablet2);
    TDatashardInitialEventsFilter initialEventsFilter(tabletIds);
    RunTestWithReboots(tabletIds, [&]() {
        return initialEventsFilter.Prepare();
    }, &CrossShard_6_Local_Impl);
}

Y_UNIT_TEST(WriteAndReadMany) {
    TTester t(TTester::ESchema_DoubleKV);
    TFakeMiniKQLProxy proxy(t);

    ui32 linesCount = 200 * 1000;
    ui32 pageSize = 50 * 1000;

    const char * progUpsert = R"___((
        (return (AsList
            %s
        ))
    ))___";

    TString key = "Key";
    TString value = "SomeBigDataValue";
    for (ui32 i = 0; i < 4; ++i) {
        key += key;
        value += value;
    }

    for (ui32 i = 0; i < linesCount/pageSize; ++i) {
        TString body;
        for (ui32 j=0; j < pageSize; ++j) {
            body += Sprintf(
                "(UpdateRow 'table2 '('('key1 (Uint32 '%u)) '('key2 (Utf8 '%s))) '('('value (Utf8 '%s))))\n",
                i*pageSize + j, key.data(), value.data());
        }

        Cout << "inserting " << i * pageSize << Endl;
        proxy.CheckedExecute(Sprintf(progUpsert, body.data()));
    }

    const char * progSelect = R"___((
        (let $1 '('key1 (Uint32 '0) (Uint32 '%u)))
        (let $2 '('key2 (Null) (Void)))
        (let $3 '('IncFrom 'IncTo $1 $2))
        (let $4 '('key1 'key2 'value))
        (let $5 '('('BytesLimit (Uint64 '40000000))))
        (return (AsList
            (SetResult 'Result (SelectRange 'table2 $3 $4 $5))
        ))
    ))___";

    for (ui32 maxCount = 150; maxCount <= 200; maxCount+=10) {
        Cout << "reading " << maxCount << Endl;
        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(proxy.Execute(Sprintf(progSelect, maxCount*1000), res), IEngineFlat::EStatus::Complete);

        auto result = NClient::TValue::Create(res.GetValue(), res.GetType());
        UNIT_ASSERT_EQUAL(bool(result["Result"]["Truncated"]), true);
    }
}

Y_UNIT_TEST(WriteKeyTooLarge) {
    TTester t(TTester::ESchema_DoubleKV);
    TFakeMiniKQLProxy proxy(t);

    const char * progUpsert = R"___((
        (return (AsList
            (UpdateRow 'table2 '('('key1 (Uint32 '%u)) '('key2 (Utf8 '%s))) '('('value (Utf8 '%s))))
        ))
    ))___";

    TString key2;
    key2.resize(2 * 1024 * 1024, 'x');

    UNIT_ASSERT_EQUAL(proxy.Execute(Sprintf(progUpsert, 123u, key2.c_str(), "foo"), /* wait */ false), IEngineFlat::EStatus::Error);
}

Y_UNIT_TEST(WriteValueTooLarge) {
    TTester t(TTester::ESchema_DoubleKV);
    TFakeMiniKQLProxy proxy(t);

    const char * progUpsert = R"___((
        (return (AsList
            (UpdateRow 'table2 '('('key1 (Uint32 '%u)) '('key2 (Utf8 '%s))) '('('value (Utf8 '%s))))
        ))
    ))___";

    TString value;
    value.resize(17 * 1024 * 1024, 'x');

    UNIT_ASSERT_EQUAL(proxy.Execute(Sprintf(progUpsert, 123u, "foo", value.c_str()), /* wait */ false), IEngineFlat::EStatus::Error);
}

Y_UNIT_TEST(WriteLargeExternalBlob) {
    TTester t(TTester::ESchema_DoubleKVExternal);
    TFakeMiniKQLProxy proxy(t);

    const char * progUpsert = R"___((
        (return (AsList
            (UpdateRow 'table2 '('('key1 (Uint32 '%u)) '('key2 (Utf8 '%s))) '('('value (Utf8 '%s))))
        ))
    ))___";

    TString value;

    value.resize(7 * 1024 * 1024, 'x');

    // Would write an external blob
    NKikimrMiniKQL::TResult res1;
    UNIT_ASSERT_EQUAL(proxy.Execute(Sprintf(progUpsert, 123u, "foo", value.c_str()), res1), IEngineFlat::EStatus::Complete);

    value.resize(16 * 1024 * 1024, 'x');

    // Would write an inline value (too large for an external blob)
    NKikimrMiniKQL::TResult res2;
    UNIT_ASSERT_EQUAL(proxy.Execute(Sprintf(progUpsert, 234u, "bar", value.c_str()), res2), IEngineFlat::EStatus::Complete);
}

void SetupProfiles(TTestActorRuntime &runtime)
{
    TResourceProfiles::TResourceProfile profile;
    profile.SetTabletType(NKikimrTabletBase::TTabletTypes::Unknown);
    profile.SetName("default");
    profile.SetStaticTabletTxMemoryLimit(100 << 20);
    profile.SetStaticTxMemoryLimit(128);
    profile.SetTxMemoryLimit(300 << 20);
    profile.SetInitialTxMemory(128);
    profile.SetSmallTxMemoryLimit(1 << 20);
    profile.SetMediumTxMemoryLimit(50 << 20);
    profile.SetSmallTxTaskType("transaction");
    profile.SetMediumTxTaskType("transaction");
    profile.SetLargeTxTaskType("transaction");

    for (ui32 i = 0; i < runtime.GetNodeCount(); ++i) {
        auto &appData = runtime.GetAppData();
        appData.ResourceProfiles = new TResourceProfiles;
        appData.ResourceProfiles->AddProfile(profile);
    }
}

std::tuple<ui64, ui64, ui64> ReadTxMemoryCounters(TTester &t, ui64 tabletId)
{
    auto sender = t.Runtime.AllocateEdgeActor();
    t.Runtime.SendToPipe(tabletId, sender, new TEvTablet::TEvGetCounters);
    TAutoPtr<IEventHandle> handle;
    auto event = t.Runtime.GrabEdgeEvent<TEvTablet::TEvGetCountersResponse>(handle);

    std::tuple<ui64, ui64, ui64> res;
    const auto &resp = event->Record;
    for (auto &counter : resp.GetTabletCounters().GetExecutorCounters().GetCumulativeCounters()) {
        if (counter.GetName() == "TxMemoryRequests")
            std::get<0>(res) = counter.GetValue();
        else if (counter.GetName() == "TxMemoryCaptures")
            std::get<1>(res) = counter.GetValue();
        else if (counter.GetName() == "TxMemoryAttaches")
            std::get<2>(res) = counter.GetValue();
    }
    return res;
}

struct TCounterRange {
    const ui64 MinValue;
    const ui64 MaxValue;

    friend inline bool operator==(ui64 value, const TCounterRange& range) {
        return range.MinValue <= value && value <= range.MaxValue;
    }

    friend inline IOutputStream& operator<<(IOutputStream& out, const TCounterRange& range) {
        return out << '[' << range.MinValue << ',' << range.MaxValue << ']';
    }
};

template<class TCounter1, class TCounter2, class TCounter3>
void CheckCounters(const std::tuple<ui64, ui64, ui64> &newCnt, const std::tuple<ui64, ui64, ui64> &oldCnt,
                   const TCounter1& d1, const TCounter2& d2, const TCounter3& d3)
{
    UNIT_ASSERT_VALUES_EQUAL(std::get<0>(newCnt) - std::get<0>(oldCnt), d1);
    UNIT_ASSERT_VALUES_EQUAL(std::get<1>(newCnt) - std::get<1>(oldCnt), d2);
    UNIT_ASSERT_VALUES_EQUAL(std::get<2>(newCnt) - std::get<2>(oldCnt), d3);
}

Y_UNIT_TEST(MemoryUsageImmediateSmallTx) {
    bool activeZone = false;
    TTester t(TTester::ESchema_KV, "dispatch_name", [&](TTestActorRuntime &runtime) {
            SetupProfiles(runtime);
        }, activeZone);
    TFakeMiniKQLProxy proxy(t);

    auto counters1 = ReadTxMemoryCounters(t, TTestTxConfig::TxTablet0);

    auto programText = R"___((
        (let row '('('key (Uint32 '42))))
        (let myUpd '(
            '('value (Utf8 '%s))))
        (let pgmReturn (AsList
        (UpdateRow 'table1 row myUpd)
        ))
        (return pgmReturn)
    ))___";

    auto text = Sprintf(programText, TString(1024, 'v').data());
    UNIT_ASSERT_EQUAL(proxy.Execute(text), IEngineFlat::EStatus::Complete);

    auto counters2 = ReadTxMemoryCounters(t, TTestTxConfig::TxTablet0);
    // Expect one allocation on prepare.
    CheckCounters(counters2, counters1, 1, 0, 0);

    {
        auto programText = R"___((
            (let row1 '('('key (Uint32 '42))))
            (let select '('value))
            (let pgmReturn (AsList
            (SetResult 'myRes (AsList
                (SelectRow 'table1 row1 select)
            ))
            ))
            (return pgmReturn)
        ))___";

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(proxy.Execute(programText, res), IEngineFlat::EStatus::Complete);
    }

    auto counters3 = ReadTxMemoryCounters(t, TTestTxConfig::TxTablet0);
    // Expect one allocation on prepare for default pool
    // on execute_data_tx_unit.cpp:127 we set limit and on temporary object allocation (on reading) we add new page with exhausted limit.
    CheckCounters(counters3, counters2, 1, 0, 0);
}

Y_UNIT_TEST(MemoryUsageImmediateMediumTx) {
    bool activeZone = false;
    TTester t(TTester::ESchema_KV, "dispatch_name", [&](TTestActorRuntime &runtime) {
            SetupProfiles(runtime);
        }, activeZone);
    TFakeMiniKQLProxy proxy(t);

    auto counters1 = ReadTxMemoryCounters(t, TTestTxConfig::TxTablet0);

    {
        auto programText = R"___((
            (let row1 '('('key (Uint32 '42))))
            (let sum (lambda '(x y) (+ x y)))
            (let x1 (Collect (ListFromRange (Uint32 '0) (Uint32 '100000))))
            (let x2 (Fold x1 (Uint32 '0) sum))
            (let myUpd '('('uint x2)))
            (let pgmReturn (AsList
                (UpdateRow 'table1 row1 myUpd)
            ))
            (return pgmReturn)
        ))___";

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(proxy.Execute(programText, res), IEngineFlat::EStatus::Complete);
    }

    auto counters2 = ReadTxMemoryCounters(t, TTestTxConfig::TxTablet0);
    // Expect one allocation on prepare and one or two
    // allocations during execution.
    CheckCounters(counters2, counters1, TCounterRange{2, 3}, 0, 0);
}

Y_UNIT_TEST(MemoryUsageImmediateHugeTx) {
    bool activeZone = false;
    TTester t(TTester::ESchema_KV, "dispatch_name", [&](TTestActorRuntime &runtime) {
            SetupProfiles(runtime);
        }, activeZone);
    TFakeMiniKQLProxy proxy(t);

    auto counters1 = ReadTxMemoryCounters(t, TTestTxConfig::TxTablet0);

    {
        auto programText = R"___((
            (let row1 '('('key (Uint32 '42))))
            (let sum (lambda '(x y) (+ x y)))
            (let x1 (Collect (ListFromRange (Uint32 '0) (Uint32 '1000000))))
            (let x2 (Fold x1 (Uint32 '0) sum))
            (let myUpd '('('uint x2)))
            (let pgmReturn (AsList
                (UpdateRow 'table1 row1 myUpd)
            ))
            (return pgmReturn)
        ))___";

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(proxy.Execute(programText, res), IEngineFlat::EStatus::Complete);
    }

    auto counters2 = ReadTxMemoryCounters(t, TTestTxConfig::TxTablet0);
    // Expect one allocation on prepare and three
    // allocations during execution.
    CheckCounters(counters2, counters1, 4, 0, 0);
}

Y_UNIT_TEST(MemoryUsageMultiShard) {
    bool activeZone = false;
    TTester t(TTester::ESchema_MultiShardKV, "dispatch_name", [&](TTestActorRuntime &runtime) {
            SetupProfiles(runtime);
        }, activeZone);
    TFakeMiniKQLProxy proxy(t);

    t.Runtime.SetLogPriority(NKikimrServices::RESOURCE_BROKER, NActors::NLog::PRI_DEBUG);
    t.Runtime.SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_DEBUG);
    t.Runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);

    //InitCrossShard(proxy);
    auto programText = R"___((
        (let row1 '('('key (Uint32 '100))))
        (let row2 '('('key (Uint32 '1100))))
        (let row3 '('('key (Uint32 '2100))))
        (let myUpd1 '(
            '('value (Utf8 'ImInShard1))))
        (let myUpd2 '(
            '('value (Utf8 'ImInShard2))))
        (let myUpd3 '(
            '('value (Utf8 'ImInShard3))))
        (let pgmReturn (AsList
        (UpdateRow 'table1 row1 myUpd1)
        (UpdateRow 'table1 row2 myUpd2)
        (UpdateRow 'table1 row3 myUpd3)
        ))
        (return pgmReturn)
    ))___";

    UNIT_ASSERT_EQUAL(proxy.Execute(programText), IEngineFlat::EStatus::Complete);

    {
        TTester::TActiveZone az(t);
        auto programText = R"___((
            (let row1 '('('key (Uint32 '100))))
            (let row2 '('('key (Uint32 '1100))))
            (let row3 '('('key (Uint32 '2100))))
            (let row13 '('('key (Uint32 '203))))
            (let row23 '('('key (Uint32 '1203))))
            (let row33 '('('key (Uint32 '2203))))
            (let select '('value))
            (let val3 (FlatMap (SelectRow 'table1 row3 select) (lambda '(x) (Member x 'value))))
            (let sum (lambda '(x y) (+ x y)))
            (let x1 (Collect (ListFromRange (Uint32 '0) (Uint32 '1000000))))
            (let x2 (Fold x1 (Uint32 '0) sum))
            (let upd3 '('('value val3)))
            (let upd4 '('('uint x2)))
            (let pgmReturn (AsList
            (UpdateRow 'table1 row13 upd3)
            (UpdateRow 'table1 row13 upd4)
            (UpdateRow 'table1 row23 upd3)
            (UpdateRow 'table1 row23 upd4)
            (UpdateRow 'table1 row33 upd3)
            (UpdateRow 'table1 row33 upd4)
            ))
            (return pgmReturn)
        ))___";
        proxy.CheckedExecute(programText);
    }
}

} // Y_UNIT_TEST_SUITE(TTxDataShardMiniKQL)

} // namespace NKikimr
