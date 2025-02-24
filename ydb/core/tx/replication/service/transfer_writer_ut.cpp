#include "service.h"
#include "transfer_writer.h"
#include "worker.h"

#include <ydb/core/fq/libs/row_dispatcher/purecalc_compilation/compile_service.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/tx/replication/ut_helpers/test_env.h>
#include <ydb/core/tx/replication/ut_helpers/test_table.h>

#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/string/printf.h>
#include <util/string/strip.h>

namespace NKikimr::NReplication::NService {

Y_UNIT_TEST_SUITE(TransferWriter) {
    using namespace NTestHelpers;
    using TRecord = TEvWorker::TEvData::TRecord;

    TRecord Record(ui64 offset, const TString& data) {
        return TRecord(offset, data, TInstant::Zero(), "MessageGroupId", "ProducerId", 13 /* seqNo */);
    }

    Y_UNIT_TEST(Write_ColumnTable) {
        TEnv env;
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_SERVICE, NLog::PRI_DEBUG);
        env.GetRuntime().SetLogPriority(NKikimrServices::FQ_ROW_DISPATCHER, NLog::PRI_DEBUG);

        env.CreateColumnTable("/Root", *MakeColumnTableDescription(TTestTableDescription{
            .Name = "Table",
            .KeyColumns = {"key"},
            .Columns = {
                {.Name = "value", .Type = "Utf8"},
                {.Name = "key", .Type = "Uint32"},
            },
        }));

        auto lambda = R"(
            $__ydb_transfer_lambda = ($x) -> {
                RETURN [
                    <|
                        key:CAST($x._offset As Uint32)
                        , value:CAST($x._data AS Utf8)
                    |>
                ];
            };
        )";

        const TPathId tablePathId = env.GetPathId("/Root/Table");

        auto compiler = env.GetRuntime().Register(NFq::NRowDispatcher::CreatePurecalcCompileService({}, MakeIntrusive<NMonitoring::TDynamicCounters>()));

        auto writer = env.GetRuntime().Register(CreateTransferWriter(lambda, tablePathId, compiler));
        env.Send<TEvWorker::TEvHandshake>(writer, new TEvWorker::TEvHandshake());

        env.Send<TEvWorker::TEvPoll>(writer, new TEvWorker::TEvData(0, "TestSource", {
            Record(1, R"({"key":[1], "update":{"value":"10"}})"),
            Record(2, R"({"key":[2], "update":{"value":"20"}})"),
            Record(3, R"({"key":[3], "update":{"value":"30"}})"),
        }));
    }

}

}
