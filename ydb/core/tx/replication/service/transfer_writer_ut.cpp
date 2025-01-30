#include "service.h"
#include "transfer_writer.h"
#include "worker.h"

#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/tx/replication/ut_helpers/test_env.h>
#include <ydb/core/tx/replication/ut_helpers/test_table.h>

#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/fq/libs/row_dispatcher/purecalc_compilation/compile_service.h>

#include <util/string/printf.h>
#include <util/string/strip.h>

namespace NKikimr::NReplication::NService {

Y_UNIT_TEST_SUITE(TransferWriter) {
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

        NKikimrReplication::TReplicationConfig config;
        auto* target = config.MutableTransferSpecific()->AddTargets();
        target->SetTransformLambda("$__ydb_transfer_lambda = ($x) -> { RETURN <|key:$x._offset, value:$x._data|>; }; ");

        const TPathId tablePathId = env.GetPathId("/Root/Table");
        const TString tableName = "/Root/Table";
        const TString writerName = "writerName";

        auto compiler = env.GetRuntime().Register(NFq::NRowDispatcher::CreatePurecalcCompileService({}, MakeIntrusive<NMonitoring::TDynamicCounters>()));

        auto writer = env.GetRuntime().Register(CreateTransferWriter(config, tablePathId, tableName, writerName, compiler));
        env.Send<TEvWorker::TEvHandshake>(writer, new TEvWorker::TEvHandshake());

        env.Send<TEvWorker::TEvPoll>(writer, new TEvWorker::TEvData("TestSource", {
            TRecord(1, R"({"key":[1], "update":{"value":"10"}})"),
            TRecord(2, R"({"key":[2], "update":{"value":"20"}})"),
            TRecord(3, R"({"key":[3], "update":{"value":"30"}})"),
        }));
    }

}

}
