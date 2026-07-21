#include <ydb/core/fq/libs/common/rows_proto_splitter.h>
#include <ydb/core/kqp/common/kqp_script_executions.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <ydb/public/api/protos/ydb_query.pb.h>
#include <ydb/public/api/protos/ydb_scripting.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/api/protos/ydb_value.pb.h>

namespace {

void ExerciseFetchScriptResults(const Ydb::Query::FetchScriptResultsRequest& request) {
    TString error;
    (void)NKikimr::NKqp::ScriptExecutionIdFromOperation(request.operation_id(), error);
    (void)request.rows_limit();
    (void)request.fetch_token().size();
    (void)request.result_set_index();
}

void ExerciseExecuteYqlScript(const Ydb::Scripting::ExecuteYqlRequest& request) {
    (void)request.script().size();
    (void)request.parameters_size();
    (void)request.syntax();
    if (request.has_operation_params()) {
        (void)request.operation_params().ByteSizeLong();
    }
}

void ExerciseScanQuery(const Ydb::Table::ExecuteScanQueryRequest& request) {
    (void)request.query().ByteSizeLong();
    (void)request.parameters_size();
    (void)request.mode();
    (void)request.collect_stats();
    (void)request.collect_full_diagnostics();
}

void ExerciseResultSplit(const Ydb::ResultSet& resultSet, FuzzedDataProvider& fdp) {
    const ui64 chunkLimit = fdp.ConsumeIntegralInRange<ui64>(1, 1 << 20);
    const ui64 headerProtoByteSize = fdp.ConsumeIntegralInRange<ui64>(0, 4096);
    const ui64 maxRowsPerChunk = fdp.ConsumeIntegralInRange<ui64>(1, 4096);
    NFq::TRowsProtoSplitter splitter(resultSet, chunkLimit, headerProtoByteSize, maxRowsPerChunk);
    const auto split = splitter.Split();
    for (const auto& part : split.ResultSets) {
        (void)part.ByteSizeLong();
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size > 128 * 1024) {
        return 0;
    }

    FuzzedDataProvider fdp(data, size);

    Ydb::Scripting::ExecuteYqlRequest executeYql;
    Ydb::Query::FetchScriptResultsRequest fetchScriptResults;
    Ydb::Table::ExecuteScanQueryRequest scanQuery;
    Ydb::ResultSet resultSet;

    const bool executeYqlParsed = executeYql.ParseFromString(fdp.ConsumeRandomLengthString(size));
    const bool fetchScriptResultsParsed = fetchScriptResults.ParseFromString(fdp.ConsumeRandomLengthString(size));
    const bool scanQueryParsed = scanQuery.ParseFromString(fdp.ConsumeRandomLengthString(size));
    const bool resultSetParsed = resultSet.ParseFromString(fdp.ConsumeRemainingBytesAsString());
    (void)executeYqlParsed;
    (void)fetchScriptResultsParsed;
    (void)scanQueryParsed;
    (void)resultSetParsed;

    try {
        ExerciseExecuteYqlScript(executeYql);
    } catch (...) {
    }
    try {
        ExerciseFetchScriptResults(fetchScriptResults);
    } catch (...) {
    }
    try {
        ExerciseScanQuery(scanQuery);
    } catch (...) {
    }
    try {
        ExerciseResultSplit(resultSet, fdp);
    } catch (...) {
    }

    return 0;
}
