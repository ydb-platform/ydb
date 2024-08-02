#include "typed_local.h"
#include "query_executor.h"
#include "get_value.h"

namespace NKikimr::NKqp {

TString TTypedLocalHelper::GetTestTableSchema() const {
    TString result;
    if (TypeName) {
        result = R"(Columns { Name: "field" Type: ")" + TypeName + "\"}";
    }
    result += R"(
            Columns { Name: "pk_int" Type: "Int64" NotNull: true }
            Columns { Name: "ts" Type: "Timestamp" }
            KeyColumnNames: "pk_int"
            Engine: COLUMN_ENGINE_REPLACING_TIMESERIES
        )";
    return result;
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
    const TString selectQuery = "PRAGMA Kikimr.OptUseFinalizeByKey='true';SELECT COUNT(*) as c, field FROM `" + TablePath + "` GROUP BY field ORDER BY field";

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

void TTypedLocalHelper::GetVolumes(ui64& rawBytes, ui64& bytes, const bool verbose /*= false*/, const std::vector<TString> columnNames /*= {}*/) {
    TString selectQuery = "SELECT * FROM `" + TablePath + "/.sys/primary_index_stats` WHERE Activity = true";
    if (columnNames.size()) {
        selectQuery += " AND EntityName IN ('" + JoinSeq("','", columnNames) + "')";
    }

    auto tableClient = KikimrRunner.GetTableClient();

    std::optional<ui64> rawBytesPred;
    std::optional<ui64> bytesPred;
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
    Cerr << bytes << "/" << rawBytes << Endl;
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
    builders.emplace_back(NArrow::NConstruction::TSimpleArrayConstructor<NArrow::NConstruction::TIntSeqFiller<arrow::Int64Type>>::BuildNotNullable("pk_int", numRows * pkKff));
    NArrow::NConstruction::TRecordBatchConstructor batchBuilder(builders);
    std::shared_ptr<arrow::RecordBatch> batch = batchBuilder.BuildBatch(numRows);
    TBase::SendDataViaActorSystem(TablePath, batch);
}

void TTypedLocalHelper::GetStats(std::vector<NJson::TJsonValue>& stats, const bool verbose /*= false*/) {
    TString selectQuery = "SELECT * FROM `" + TablePath + "/.sys/primary_index_portion_stats` WHERE Activity = true";
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

}