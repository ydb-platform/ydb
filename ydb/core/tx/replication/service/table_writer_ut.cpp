#include "service.h"
#include "table_writer.h"
#include "worker.h"

#include <ydb/core/tx/replication/ut_helpers/test_env.h>
#include <ydb/core/tx/replication/ut_helpers/test_table.h>

#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/string/printf.h>

namespace NKikimr::NReplication::NService {

Y_UNIT_TEST_SUITE(LocalTableWriter) {
    using namespace NTestHelpers;
    using TRecord = TEvWorker::TEvData::TRecord;

    Y_UNIT_TEST(WriteTable) {
        TEnv env;
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_SERVICE, NLog::PRI_DEBUG);

        env.CreateTable("/Root", *MakeTableDescription(TTestTableDescription{
            .Name = "Table",
            .KeyColumns = {"key"},
            .Columns = {
                {.Name = "key", .Type = "Uint32"},
                {.Name = "value", .Type = "Utf8"},
            },
        }));

        auto writer = env.GetRuntime().Register(CreateLocalTableWriter(env.GetPathId("/Root/Table")));
        env.Send<TEvWorker::TEvHandshake>(writer, new TEvWorker::TEvHandshake());

        env.Send<TEvWorker::TEvPoll>(writer, new TEvWorker::TEvData("TestSource", {
            TRecord(1, R"({"key":[1], "update":{"value":"10"}})"),
            TRecord(2, R"({"key":[2], "update":{"value":"20"}})"),
            TRecord(3, R"({"key":[3], "update":{"value":"30"}})"),
        }));
    }

    Y_UNIT_TEST(SupportedTypes) {
        TEnv env(TFeatureFlags()
            .SetEnableTableDatetime64(true)
            .SetEnableTablePgTypes(true)
            .SetEnableParameterizedDecimal(true)
            .SetEnablePgSyntax(true));
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_SERVICE, NLog::PRI_DEBUG);

        env.CreateTable("/Root", *MakeTableDescription(TTestTableDescription{
            .Name = "Table",
            .KeyColumns = {"key"},
            .Columns = {
                {.Name = "key", .Type = "Uint32"},
                {.Name = "int32_value", .Type = "Int32"},
                {.Name = "uint32_value", .Type = "Uint32"},
                {.Name = "int64_value", .Type = "Int64"},
                {.Name = "uint64_value", .Type = "Uint64"},
                {.Name = "uint8_value", .Type = "Uint8"},
                {.Name = "bool_value", .Type = "Bool"},
                {.Name = "double_value", .Type = "Double"},
                {.Name = "float_value", .Type = "Float"},
                {.Name = "date_value", .Type = "Date"},
                {.Name = "datetime_value", .Type = "Datetime"},
                {.Name = "timestamp_value", .Type = "Timestamp"},
                {.Name = "interval_value", .Type = "Interval"},
                {.Name = "decimal_value", .Type = "Decimal"},
                {.Name = "dynumber_value", .Type = "DyNumber"},
                {.Name = "string_value", .Type = "String"},
                {.Name = "utf8_value", .Type = "Utf8"},
                {.Name = "json_value", .Type = "Json"},
                {.Name = "jsondoc_value", .Type = "JsonDocument"},
                {.Name = "uuid_value", .Type = "Uuid"},
                {.Name = "date32_value", .Type = "Date32"},
                {.Name = "datetime64_value", .Type = "Datetime64"},
                {.Name = "timestamp64_value", .Type = "Timestamp64"},
                {.Name = "interval64_value", .Type = "Interval64"},
                {.Name = "pgint2_value", .Type = "pgint2"},
                {.Name = "pgint4_value", .Type = "pgint4"},
                {.Name = "pgint8_value", .Type = "pgint8"},
                {.Name = "pgfloat4_value", .Type = "pgfloat4"},
                {.Name = "pgfloat8_value", .Type = "pgfloat8"},
                {.Name = "pgbytea_value", .Type = "pgbytea"},
                {.Name = "pgtext_value", .Type = "pgtext"},
                {.Name = "decimal35_value", .Type = "Decimal(35,10)"},
            },
        }));

        auto writer = env.GetRuntime().Register(CreateLocalTableWriter(env.GetPathId("/Root/Table")));
        env.Send<TEvWorker::TEvHandshake>(writer, new TEvWorker::TEvHandshake());

        env.Send<TEvWorker::TEvPoll>(writer, new TEvWorker::TEvData("TestSource", {
            TRecord(1, R"({"key":[1], "update":{"int32_value":-100500}})"),
            TRecord(2, R"({"key":[2], "update":{"uint32_value":100500}})"),
            TRecord(3, R"({"key":[3], "update":{"int64_value":-200500}})"),
            TRecord(4, R"({"key":[4], "update":{"uint64_value":200500}})"),
            TRecord(5, R"({"key":[5], "update":{"uint8_value":255}})"),
            TRecord(6, R"({"key":[6], "update":{"bool_value":true}})"),
            TRecord(7, R"({"key":[7], "update":{"double_value":1.1234}})"),
            TRecord(8, R"({"key":[8], "update":{"float_value":-1.123}})"),
            TRecord(9, R"({"key":[9], "update":{"date_value":"2020-08-12T00:00:00.000000Z"}})"),
            TRecord(10, R"({"key":[10], "update":{"datetime_value":"2020-08-12T12:34:56.000000Z"}})"),
            TRecord(11, R"({"key":[11], "update":{"timestamp_value":"2020-08-12T12:34:56.123456Z"}})"),
            TRecord(12, R"({"key":[12], "update":{"interval_value":-300500}})"),
            TRecord(13, R"({"key":[13], "update":{"decimal_value":"3.321"}})"),
            TRecord(14, R"({"key":[14], "update":{"dynumber_value":".3321e1"}})"),
            TRecord(15, Sprintf(R"({"key":[15], "update":{"string_value":"%s"}})", Base64Encode("lorem ipsum").c_str())),
            TRecord(16, R"({"key":[16], "update":{"utf8_value":"lorem ipsum"}})"),
            TRecord(17, R"({"key":[17], "update":{"json_value":{"key": "value"}}})"),
            TRecord(18, R"({"key":[18], "update":{"jsondoc_value":{"key": "value"}}})"),
            TRecord(19, R"({"key":[19], "update":{"uuid_value":"65df1ec1-a97d-47b2-ae56-3c023da6ee8c"}})"),
            TRecord(20, R"({"key":[20], "update":{"date32_value":18486}})"),
            TRecord(21, R"({"key":[21], "update":{"datetime64_value":1597235696}})"),
            TRecord(22, R"({"key":[22], "update":{"timestamp64_value":1597235696123456}})"),
            TRecord(23, R"({"key":[23], "update":{"interval64_value":-300500}})"),
            TRecord(24, R"({"key":[24], "update":{"pgint2_value":"-42"}})"),
            TRecord(25, R"({"key":[25], "update":{"pgint4_value":"-420"}})"),
            TRecord(26, R"({"key":[26], "update":{"pgint8_value":"-4200"}})"),
            TRecord(27, R"({"key":[27], "update":{"pgfloat4_value":"3.1415"}})"),
            TRecord(28, R"({"key":[28], "update":{"pgfloat8_value":"2.718"}})"),
            TRecord(29, R"({"key":[29], "update":{"pgbytea_value":"\\x6c6f72656d2022697073756d22"}})"),
            TRecord(30, R"({"key":[30], "update":{"pgtext_value":"lorem \"ipsum\""}})"),
            TRecord(31, R"({"key":[31], "update":{"decimal35_value":"355555555555555.321"}})"),
        }));
    }

    THolder<TEvService::TEvTxIdResult> MakeTxIdResult(const TMap<TRowVersion, ui64>& result) {
        auto ev = MakeHolder<TEvService::TEvTxIdResult>();

        for (const auto& [version, txId] : result) {
            auto& item = *ev->Record.AddVersionTxIds();
            version.ToProto(item.MutableVersion());
            item.SetTxId(txId);
        }

        return ev;
    }

    Y_UNIT_TEST(ConsistentWrite) {
        TEnv env;
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_SERVICE, NLog::PRI_DEBUG);

        env.CreateTable("/Root", *MakeTableDescription(TTestTableDescription{
            .Name = "Table",
            .KeyColumns = {"key"},
            .Columns = {
                {.Name = "key", .Type = "Uint32"},
                {.Name = "value", .Type = "Utf8"},
            },
            .ReplicationConfig = TTestTableDescription::TReplicationConfig{
                .Mode = TTestTableDescription::TReplicationConfig::MODE_READ_ONLY,
                .ConsistencyLevel = TTestTableDescription::TReplicationConfig::CONSISTENCY_LEVEL_GLOBAL,
            },
        }));

        auto writer = env.GetRuntime().Register(CreateLocalTableWriter(env.GetPathId("/Root/Table"), EWriteMode::Consistent));
        env.Send<TEvWorker::TEvHandshake>(writer, new TEvWorker::TEvHandshake());
        ui64 order = 1;

        {
            auto ev = env.Send<TEvService::TEvGetTxId>(writer, new TEvWorker::TEvData("TestSource", {
                TRecord(order++, R"({"key":[1], "update":{"value":"10"}, "ts":[1,0]})"),
                TRecord(order++, R"({"key":[2], "update":{"value":"20"}, "ts":[2,0]})"),
                TRecord(order++, R"({"key":[3], "update":{"value":"30"}, "ts":[3,0]})"),
            }));

            const auto& versions = ev->Get()->Record.GetVersions();
            UNIT_ASSERT_VALUES_EQUAL(versions.size(), 3);

            for (int i = 0; i < versions.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(TRowVersion::FromProto(versions[i]), TRowVersion(i + 1, 0));
            }
        }
        {
            env.Send<TEvWorker::TEvPoll>(writer, MakeTxIdResult({
                {TRowVersion(10, 0), 1},
            }));
        }
        {
            auto ev = env.Send<TEvService::TEvHeartbeat>(writer, new TEvWorker::TEvData("TestSource", {
                TRecord(order++, R"({"resolved":[10,0]})"),
            }));
            UNIT_ASSERT_VALUES_EQUAL(TRowVersion::FromProto(ev->Get()->Record.GetVersion()), TRowVersion(10, 0));
            env.GetRuntime().GrabEdgeEvent<TEvWorker::TEvPoll>(env.GetSender());
        }

        env.Send<TEvService::TEvGetTxId>(writer, new TEvWorker::TEvData("TestSource", {
            TRecord(order++, R"({"key":[1], "update":{"value":"10"}, "ts":[11,0]})"),
            TRecord(order++, R"({"key":[2], "update":{"value":"20"}, "ts":[12,0]})"),
        }));

        env.Send<TEvService::TEvGetTxId>(writer, new TEvWorker::TEvData("TestSource", {
            TRecord(order++, R"({"key":[1], "update":{"value":"10"}, "ts":[21,0]})"),
            TRecord(order++, R"({"key":[2], "update":{"value":"20"}, "ts":[22,0]})"),
        }));

        env.Send<TEvWorker::TEvPoll>(writer, MakeTxIdResult({
            {TRowVersion(20, 0), 2},
            {TRowVersion(30, 0), 3},
        }));

        env.Send<TEvWorker::TEvPoll>(writer, new TEvWorker::TEvData("TestSource", {
            TRecord(order++, R"({"key":[1], "update":{"value":"10"}, "ts":[13,0]})"),
            TRecord(order++, R"({"key":[2], "update":{"value":"20"}, "ts":[23,0]})"),
        }));

        env.Send<TEvService::TEvHeartbeat>(writer, new TEvWorker::TEvData("TestSource", {
            TRecord(order++, R"({"resolved":[30,0]})"),
        }));
        env.GetRuntime().GrabEdgeEvent<TEvWorker::TEvPoll>(env.GetSender());
    }
}

}
