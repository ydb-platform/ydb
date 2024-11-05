#include "s3_writer.h"
#include "worker.h"

#include <ydb/core/tx/replication/ut_helpers/test_env.h>
#include <ydb/core/tx/replication/ut_helpers/test_table.h>
#include <ydb/core/wrappers/ut_helpers/s3_mock.h>
#include <ydb/core/wrappers/s3_wrapper.h>
#include <ydb/core/wrappers/s3_storage_config.h>

#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/string/printf.h>

namespace NKikimr::NReplication::NService {

Y_UNIT_TEST_SUITE(S3Writer) {
    using namespace NTestHelpers;

    Y_UNIT_TEST(WriteTableS3) {
        using namespace NWrappers::NTestHelpers;
        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        TS3Mock s3Mock({}, TS3Mock::TSettings(port));
        UNIT_ASSERT(s3Mock.Start());

        TString settings = Sprintf(R"(
            endpoint: "localhost:%d"
            scheme: HTTP
            bucket: "TEST"
            items {
                source_path: "/MyRoot/Table"
                destination_prefix: ""
            }
        )", port);
        Ydb::Export::ExportToS3Settings request;
        UNIT_ASSERT(google::protobuf::TextFormat::ParseFromString(settings, &request));

        auto config = std::make_shared<NWrappers::NExternalStorage::TS3ExternalStorageConfig>(request);

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

        TString writerUuid = "AtufpxzetsqaVnEuozdXpD"; // basically base58-encoded uuid4

        auto writer = env.GetRuntime().Register(CreateS3Writer(config, "/MyRoot/Table", writerUuid));
        env.Send<TEvWorker::TEvHandshake>(writer, new TEvWorker::TEvHandshake());

        UNIT_ASSERT_VALUES_EQUAL(s3Mock.GetData().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(s3Mock.GetData().at("/TEST/writer.AtufpxzetsqaVnEuozdXpD.json"),
                                 R"({"finished":false,"table_name":"/MyRoot/Table","writer_name":"AtufpxzetsqaVnEuozdXpD"})");

        using TRecord = TEvWorker::TEvData::TRecord;
        env.Send<TEvWorker::TEvPoll>(writer, new TEvWorker::TEvData({
            TRecord(1, R"({"key":[1], "update":{"value":"10"}})"),
            TRecord(2, R"({"key":[2], "update":{"value":"20"}})"),
            TRecord(3, R"({"key":[3], "update":{"value":"30"}})"),
        }));

        UNIT_ASSERT_VALUES_EQUAL(s3Mock.GetData().size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(s3Mock.GetData().at("/TEST/writer.AtufpxzetsqaVnEuozdXpD.json"),
                                 R"({"finished":false,"table_name":"/MyRoot/Table","writer_name":"AtufpxzetsqaVnEuozdXpD"})");
        UNIT_ASSERT_VALUES_EQUAL(s3Mock.GetData().at("/TEST/part.1.AtufpxzetsqaVnEuozdXpD.jsonl"),
                                 R"({"key":[1], "update":{"value":"10"}})" "\n"
                                 R"({"key":[2], "update":{"value":"20"}})" "\n"
                                 R"({"key":[3], "update":{"value":"30"}})" "\n");

        auto res = env.Send<TEvWorker::TEvGone>(writer, new TEvWorker::TEvData({}));

        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, TEvWorker::TEvGone::DONE);
        UNIT_ASSERT_VALUES_EQUAL(s3Mock.GetData().size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(s3Mock.GetData().at("/TEST/writer.AtufpxzetsqaVnEuozdXpD.json"),
                                 R"({"finished":true,"table_name":"/MyRoot/Table","writer_name":"AtufpxzetsqaVnEuozdXpD"})");
        UNIT_ASSERT_VALUES_EQUAL(s3Mock.GetData().at("/TEST/part.1.AtufpxzetsqaVnEuozdXpD.jsonl"),
                                 R"({"key":[1], "update":{"value":"10"}})" "\n"
                                 R"({"key":[2], "update":{"value":"20"}})" "\n"
                                 R"({"key":[3], "update":{"value":"30"}})" "\n");
    }

    // TODO test all retry behavior
}

}
