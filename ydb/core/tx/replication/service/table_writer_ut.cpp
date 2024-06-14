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

        using TRecord = TEvWorker::TEvData::TRecord;
        env.Send<TEvWorker::TEvPoll>(writer, new TEvWorker::TEvData("TestSource", {
            TRecord(1, R"({"key":[1], "update":{"value":"10"}})"),
            TRecord(2, R"({"key":[2], "update":{"value":"20"}})"),
            TRecord(3, R"({"key":[3], "update":{"value":"30"}})"),
        }));
    }

    Y_UNIT_TEST(SupportedTypes) {
        TEnv env;
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
                {.Name = "uuid_value", .Type = "Uuid"}
            },
        }));

        auto writer = env.GetRuntime().Register(CreateLocalTableWriter(env.GetPathId("/Root/Table")));
        env.Send<TEvWorker::TEvHandshake>(writer, new TEvWorker::TEvHandshake());

        using TRecord = TEvWorker::TEvData::TRecord;
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
        }));
    }
}

}
