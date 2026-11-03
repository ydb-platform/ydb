#include "json_change_record.h"

#include <ydb/core/protos/tx_datashard.pb.h>

#include <library/cpp/json/yson/json2yson.h>
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/aclib/user_context.h>

namespace NKikimr::NReplication::NService {

Y_UNIT_TEST_SUITE(JsonChangeRecord) {
    Y_UNIT_TEST(DataChange) {
        auto record = TChangeRecordBuilder()
            .WithBody(R"({"key":[1], "update":{"value":100500}})")
            .Build();
        UNIT_ASSERT_VALUES_EQUAL(record->GetKind(), TChangeRecord::EKind::CdcDataChange);
        UNIT_ASSERT_VALUES_EQUAL(record->GetStep(), 0);
        UNIT_ASSERT_VALUES_EQUAL(record->GetTxId(), 0);
    }

    Y_UNIT_TEST(DataChangeVersion) {
        auto record = TChangeRecordBuilder()
            .WithBody(R"({"key":[1], "update":{"value":100500}, "ts":[10,20]})")
            .Build();
        UNIT_ASSERT_VALUES_EQUAL(record->GetStep(), 10);
        UNIT_ASSERT_VALUES_EQUAL(record->GetTxId(), 20);
    }

    Y_UNIT_TEST(DataChangeDoubleKeyPrecision) {
        auto schema = MakeIntrusive<TLightweightSchema>();
        schema->KeyColumns.emplace_back(NScheme::NTypeIds::Double);

        const std::pair<TStringBuf, double> cases[] = {
            {"93.829127966695111", 93.829127966695111},
            {"-93.829127966695111", -93.829127966695111},
            {"9.3829127966695111e1", 93.829127966695111},
            {"2.2250738585072014e-308", 2.2250738585072014e-308},
            {"4.9406564584124654e-324", 4.9406564584124654e-324},
            {"1.7976931348623157e308", 1.7976931348623157e308},
        };
        for (const auto& [text, expected] : cases) {
            auto record = TChangeRecordBuilder()
                .WithBody(TStringBuilder() << R"({"key":[)" << text << R"(], "erase":{}})")
                .WithSchema(schema)
                .Build();

            TMemoryPool pool(256);
            const auto key = record->GetKey(pool);
            UNIT_ASSERT_VALUES_EQUAL(key.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL_C(key[0].AsValue<double>(), expected, text);

            NKikimrTxDataShard::TEvApplyReplicationChanges::TChange change;
            record->Serialize(change, pool);
            const TSerializedCellVec serializedKey(change.GetKey());
            UNIT_ASSERT_VALUES_EQUAL(serializedKey.GetCells().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL_C(serializedKey.GetCells()[0].AsValue<double>(), expected, text);
        }
    }

    Y_UNIT_TEST(DataChangeDoubleValuePrecision) {
        auto schema = MakeIntrusive<TLightweightSchema>();
        schema->KeyColumns.emplace_back(NScheme::NTypeIds::Uint64);
        schema->ValueColumns["value"] = {2, NScheme::TTypeInfo(NScheme::NTypeIds::Double)};

        for (const auto* operation : {"update", "reset"}) {
            auto record = TChangeRecordBuilder()
                .WithBody(TStringBuilder() << R"({"key":[1], ")" << operation
                    << R"(":{"value":93.829127966695111}})")
                .WithSchema(schema)
                .Build();

            TMemoryPool pool(256);
            NKikimrTxDataShard::TEvApplyReplicationChanges::TChange change;
            record->Serialize(change, pool);
            const auto& updates = change.HasUpsert() ? change.GetUpsert() : change.GetReset();
            UNIT_ASSERT_VALUES_EQUAL(updates.TagsSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(updates.GetTags(0), 2);
            const TSerializedCellVec values(updates.GetData());
            UNIT_ASSERT_VALUES_EQUAL(values.GetCells().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL_C(values.GetCells()[0].AsValue<double>(), 93.829127966695111, operation);
        }
    }

    Y_UNIT_TEST(DataChangeIntegerTypes) {
        auto schema = MakeIntrusive<TLightweightSchema>();
        schema->KeyColumns.emplace_back(NScheme::NTypeIds::Uint64);
        schema->ValueColumns["value"] = {2, NScheme::TTypeInfo(NScheme::NTypeIds::Yson)};

        auto record = TChangeRecordBuilder()
            .WithBody(R"({"key":[1], "update":{"value":[
                1, 4294967295, 4294967296, 9223372036854775807,
                9223372036854775808, 18446744073709551615,
                -2147483648, -2147483649, -9223372036854775808,
                18446744073709551616, -9223372036854775809
            ]}})")
            .WithSchema(schema)
            .Build();

        NJson::TJsonValue expected(NJson::JSON_ARRAY);
        for (const i64 value : {i64(1), i64(4294967295), i64(4294967296), Max<i64>()}) {
            expected.AppendValue(value);
        }
        expected.AppendValue(ui64(9223372036854775808ull));
        expected.AppendValue(Max<ui64>());
        for (const i64 value : {i64(-2147483648), i64(-2147483649), Min<i64>()}) {
            expected.AppendValue(value);
        }
        expected.AppendValue(18446744073709551616.0);
        expected.AppendValue(-9223372036854775809.0);

        TMemoryPool pool(256);
        NKikimrTxDataShard::TEvApplyReplicationChanges::TChange change;
        record->Serialize(change, pool);
        const TSerializedCellVec values(change.GetUpsert().GetData());
        UNIT_ASSERT_VALUES_EQUAL(values.GetCells().size(), 1);
        // YSON distinguishes signed integers, unsigned integers and doubles.
        UNIT_ASSERT_VALUES_EQUAL(values.GetCells()[0].AsBuf(), NJson2Yson::SerializeJsonValueAsYson(expected));
    }

    Y_UNIT_TEST(InvalidJson) {
        for (const auto* body : {
            "", "{", R"({"key":[1],})", R"({'key':[1]})", R"({key:[1]})",
            R"({"key":[01]})", R"({"key":[NaN]})", R"({"key":[1e999]})",
            R"({"key":[1]} {"key":[2]})", R"({/*comment*/"key":[1]})",
            R"({"key":["\uD800"]})", "{\"key\":[\"\xC0\xAF\"]}",
        }) {
            NJson::TJsonValue json;
            TString error;
            UNIT_ASSERT_C(!TChangeRecordBuilder::ParseJsonBody(body, json, error), body);
            UNIT_ASSERT_VALUES_EQUAL(error, "cannot parse JSON");
        }
    }

    Y_UNIT_TEST(Heartbeat) {
        auto record = TChangeRecordBuilder()
            .WithBody(R"({"resolved":[10,20]})")
            .Build();
        UNIT_ASSERT_VALUES_EQUAL(record->GetKind(), TChangeRecord::EKind::CdcHeartbeat);
        UNIT_ASSERT_VALUES_EQUAL(record->GetStep(), 10);
        UNIT_ASSERT_VALUES_EQUAL(record->GetTxId(), 20);
    }
}

}
