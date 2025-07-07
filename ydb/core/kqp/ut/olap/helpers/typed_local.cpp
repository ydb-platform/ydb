#include "get_value.h"
#include "query_executor.h"
#include "typed_local.h"

#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>

#include <ydb/public/api/protos/ydb_table.pb.h>

namespace NKikimr::NKqp {

TString TTypedLocalHelper::GetTestTableSchema() const {
    TString result;
    if (TypeName) {
        result = R"(Columns { Name: "field" Type: ")" + TypeName + "\"}";
    }
    if (TypeName1) {
        result += R"(Columns { Name: "field1" Type: ")" + TypeName1 + "\"}";
    }
    result += R"(
            Columns { Name: "pk_int" Type: "Int64" NotNull: true }
            Columns { Name: "ts" Type: "Timestamp" }
            KeyColumnNames: "pk_int"
        )";
    return result;
}

TString TTypedLocalHelper::GetMultiColumnTestTableSchema(ui32 reps) const {
    TString result;
    result += R"(
            Columns { Name: "pk_int" Type: "Int64" NotNull: true }
            Columns { Name: "ts" Type: "Timestamp" }
        )";
    for (ui32 i = 0; i < reps; i++) {
        TString strNum = ToString(i);
        result += "Columns {Name: \"field_utf" + strNum + "\" Type: \"Utf8\"}\n";
        result += "Columns {Name: \"field_int" + strNum + "\" Type: \"Int64\"}\n";
        result += "Columns {Name: \"field_uint" + strNum + "\" Type: \"Uint8\"}\n";
        result += "Columns {Name: \"field_float" + strNum + "\" Type: \"Float\"}\n";
        result += "Columns {Name: \"field_double" + strNum + "\" Type: \"Double\"}\n";
    }
    result += R"(
            KeyColumnNames: "pk_int"
    )";
    return result;
}

void TTypedLocalHelper::CreateMultiColumnOlapTableWithStore(ui32 reps, ui32 storeShardsCount, ui32 tableShardsCount) {
    CreateSchemaOlapTablesWithStore(GetMultiColumnTestTableSchema(reps), { TableName }, "olapStore", storeShardsCount, tableShardsCount);
}

void TTypedLocalHelper::ExecuteSchemeQuery(const TString& alterQuery, const NYdb::EStatus expectedStatus /*= EStatus::SUCCESS*/) const {
    auto session = KikimrRunner.GetTableClient().CreateSession().GetValueSync().GetSession();
    auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), expectedStatus, alterResult.GetIssues().ToString());
}

TString TTypedLocalHelper::GetQueryResult(const TString& request) const {
    auto db = KikimrRunner.GetQueryClient();
    auto result = db.ExecuteQuery(request, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
    const TString output = FormatResultSetYson(result.GetResultSet(0));
    Cout << output << Endl;
    return output;
}

void TTypedLocalHelper::PrintCount() {
    const TString selectQuery = "SELECT COUNT(*), MAX(pk_int), MIN(pk_int) FROM `" + TablePath + "`";

    auto tableClient = KikimrRunner.GetTableClient();
    auto rows = ExecuteScanQuery(tableClient, selectQuery);
    for (auto&& r : rows) {
        for (auto&& c : r) {
            Cerr << c.first << ":" << Endl << c.second.GetProto().DebugString() << Endl;
        }
    }
}

NKikimr::NKqp::TTypedLocalHelper::TDistribution TTypedLocalHelper::GetDistribution(const bool verbose /*= false*/) {
    const TString selectQuery =
        "PRAGMA Kikimr.OptUseFinalizeByKey='true';SELECT COUNT(*) as c, field FROM `" + TablePath + "` GROUP BY field ORDER BY field";

    auto tableClient = KikimrRunner.GetTableClient();
    auto rows = ExecuteScanQuery(tableClient, selectQuery, verbose);
    ui32 count = 0;
    std::optional<ui32> minCount;
    std::optional<ui32> maxCount;
    std::set<TString> groups;
    for (auto&& r : rows) {
        for (auto&& c : r) {
            if (c.first == "c") {
                const ui64 v = GetUint64(c.second);
                count += v;
                if (!minCount || *minCount > v) {
                    minCount = v;
                }
                if (!maxCount || *maxCount < v) {
                    maxCount = v;
                }
            } else if (c.first == "field") {
                Y_ABORT_UNLESS(groups.emplace(c.second.GetProto().DebugString()).second);
            }
            if (verbose) {
                Cerr << c.first << ":" << Endl << c.second.GetProto().DebugString() << Endl;
            }
        }
    }
    Y_ABORT_UNLESS(maxCount);
    Y_ABORT_UNLESS(minCount);
    return TDistribution(count, *minCount, *maxCount, groups.size());
}

void TTypedLocalHelper::GetVolumes(
    ui64& rawBytes, ui64& bytes, ui64& portionsCount, const bool verbose /*= false*/, const std::vector<TString> columnNames /*= {}*/) {
    TString selectQuery = "SELECT * FROM `" + TablePath + "/.sys/primary_index_stats` WHERE Activity == 1";
    if (columnNames.size()) {
        selectQuery += " AND EntityName IN ('" + JoinSeq("','", columnNames) + "')";
    }

    auto tableClient = KikimrRunner.GetTableClient();

    std::optional<ui64> rawBytesPred;
    std::optional<ui64> bytesPred;
    std::set<ui32> portionIds;
    while (true) {
        auto rows = ExecuteScanQuery(tableClient, selectQuery);
        rawBytes = 0;
        bytes = 0;
        for (auto&& r : rows) {
            if (verbose) {
                Cerr << "-------" << Endl;
            }
            for (auto&& c : r) {
                if (c.first == "RawBytes") {
                    rawBytes += GetUint64(c.second);
                }
                if (c.first == "BlobRangeSize") {
                    bytes += GetUint64(c.second);
                }
                if (verbose) {
                    Cerr << c.first << ":" << Endl << c.second.GetProto().DebugString() << Endl;
                }
                if (c.first == "PortionId") {
                    portionIds.emplace(GetUint64(c.second));
                }
            }
        }
        if (rawBytesPred && *rawBytesPred == rawBytes && bytesPred && *bytesPred == bytes) {
            break;
        } else {
            rawBytesPred = rawBytes;
            bytesPred = bytes;
            Cerr << "Wait changes: " << bytes << "/" << rawBytes << Endl;
            Sleep(TDuration::Seconds(5));
        }
    }
    portionsCount = portionIds.size();
    Cerr << bytes << "/" << rawBytes << "/" << portionIds.size() << Endl;
}

void TTypedLocalHelper::GetCount(ui64& count) {
    const TString selectQuery = "SELECT COUNT(*) as a FROM `" + TablePath + "`";

    auto tableClient = KikimrRunner.GetTableClient();

    auto rows = ExecuteScanQuery(tableClient, selectQuery);
    for (auto&& r : rows) {
        for (auto&& c : r) {
            if (c.first == "a") {
                count = GetUint64(c.second);
            }
        }
    }
}

void TTypedLocalHelper::FillPKOnly(const double pkKff /*= 0*/, const ui32 numRows /*= 800000*/) const {
    std::vector<NArrow::NConstruction::IArrayBuilder::TPtr> builders;
    builders.emplace_back(
        NArrow::NConstruction::TSimpleArrayConstructor<NArrow::NConstruction::TIntSeqFiller<arrow::Int64Type>>::BuildNotNullable(
            "pk_int", numRows * pkKff));
    NArrow::NConstruction::TRecordBatchConstructor batchBuilder(builders);
    std::shared_ptr<arrow::RecordBatch> batch = batchBuilder.BuildBatch(numRows);
    TBase::SendDataViaActorSystem(TablePath, batch);
}

void TTypedLocalHelper::GetStats(std::vector<NJson::TJsonValue>& stats, const bool verbose /*= false*/) {
    TString selectQuery = "SELECT * FROM `" + TablePath + "/.sys/primary_index_portion_stats` WHERE Activity == 1";
    auto tableClient = KikimrRunner.GetTableClient();
    auto rows = ExecuteScanQuery(tableClient, selectQuery, verbose);
    for (auto&& r : rows) {
        for (auto&& c : r) {
            if (c.first == "Stats") {
                NJson::TJsonValue jsonStore;
                AFL_VERIFY(NJson::ReadJsonFastTree(GetUtf8(c.second), &jsonStore));
                stats.emplace_back(jsonStore);
            }
        }
    }
}

void TTypedLocalHelper::TSimultaneousWritingSession::SendDataViaActorSystem(TString testTable, std::shared_ptr<arrow::RecordBatch> batch,
    const Ydb::StatusIds_StatusCode expectedStatus /*= = Ydb::StatusIds::SUCCESS*/) const {
    auto* runtime = KikimrRunner.GetTestServer().GetRuntime();

    UNIT_ASSERT(batch);
    UNIT_ASSERT(batch->num_rows());
    auto data = NArrow::SerializeBatchNoCompression(batch);
    UNIT_ASSERT(!data.empty());
    TString serializedSchema = NArrow::SerializeSchema(*batch->schema());
    UNIT_ASSERT(serializedSchema);

    Ydb::Table::BulkUpsertRequest request;
    request.mutable_arrow_batch_settings()->set_schema(serializedSchema);
    request.set_data(data);
    request.set_table(testTable);

    using TEvBulkUpsertRequest = NGRpcService::TGrpcRequestOperationCall<Ydb::Table::BulkUpsertRequest, Ydb::Table::BulkUpsertResponse>;
    auto future = NRpcService::DoLocalRpc<TEvBulkUpsertRequest>(std::move(request), "", "", runtime->GetActorSystem(0));
    Responses.fetch_add(1);
    auto* responsesLocal = &Responses;
    future.Subscribe([responsesLocal, expectedStatus](const NThreading::TFuture<Ydb::Table::BulkUpsertResponse> f) mutable {
        responsesLocal->fetch_add(-1);
        auto op = f.GetValueSync().operation();
        if (op.status() != Ydb::StatusIds::SUCCESS) {
            for (auto& issue : op.issues()) {
                Cerr << issue.message() << " ";
            }
            Cerr << "\n";
        }
        UNIT_ASSERT_VALUES_EQUAL(op.status(), expectedStatus);
    });
}

void TTypedLocalHelper::TSimultaneousWritingSession::Finalize() {
    AFL_VERIFY(!Finished);
    Finished = true;
    auto* runtime = KikimrRunner.GetTestServer().GetRuntime();
    TDispatchOptions options;
    options.CustomFinalCondition = [&]() {
        return Responses.load() == 0;
    };

    runtime->DispatchEvents(options);
}

}   // namespace NKikimr::NKqp
