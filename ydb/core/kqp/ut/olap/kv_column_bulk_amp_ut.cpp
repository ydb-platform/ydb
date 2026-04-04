#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/ut/olap/helpers/get_value.h>
#include <ydb/core/kqp/ut/olap/helpers/query_executor.h>

#include <ydb/library/workload/kv/kv.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {

namespace {

void ExecuteBulkUpsert(NYdb::NTable::TTableClient& db, NYdbWorkload::TQueryInfo& q) {
    Y_ABORT_UNLESS(q.TableOperation);
    auto st = q.TableOperation(db);
    UNIT_ASSERT_C(st.IsSuccess(), st.GetIssues().ToString());
}

} // namespace

Y_UNIT_TEST_SUITE(KvWorkloadColumnBulkAmp) {
    Y_UNIT_TEST(AmplificationBelowThreshold) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        auto tableClient = kikimr.GetTableClient();
        auto session = tableClient.CreateSession().GetValueSync().GetSession();

        NYdbWorkload::TKvWorkloadParams params;
        params.DbPath = "/Root";
        params.SetStoreType(NYdbWorkload::TKvWorkloadParams::EStoreType::Column);
        params.TableName = "kv_column_bulk_amp";
        params.MaxFirstKey = 128;
        params.StringLen = 512;
        params.RowsCnt = 64;
        params.ColumnsCnt = 2;
        params.IntColumnsCnt = 1;
        params.KeyColumnsCnt = 1;

        auto workloadGen = params.CreateGenerator();
        {
            auto res = session.ExecuteSchemeQuery(TString(workloadGen->GetDDLQueries())).GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        }

        const int bulkType = static_cast<int>(NYdbWorkload::TKvWorkloadGenerator::EType::BulkUpsertRandom);
        for (int i = 0; i < 40; ++i) {
            auto qlist = workloadGen->GetWorkload(bulkType);
            UNIT_ASSERT(!qlist.empty());
            for (auto& q : qlist) {
                ExecuteBulkUpsert(tableClient, q);
            }
        }

        const TString tablePath = TStringBuilder() << params.DbPath << "/" << params.TableName;
        const TString countQuery = TStringBuilder() << "SELECT COUNT(*) AS RowCnt FROM `" << tablePath << "`;";
        const TString bytesQuery = TStringBuilder() << R"(
            SELECT SUM(BlobRangeSize) AS Bytes
            FROM `)" << tablePath << R"(/.sys/primary_index_stats`
            WHERE Activity == 1
        )";

        ui64 rowCnt = 0;
        ui64 storedBytes = 0;
        for (int attempt = 0; attempt < 60; ++attempt) {
            auto cntRows = ExecuteScanQuery(tableClient, countQuery, false);
            UNIT_ASSERT(!cntRows.empty());
            rowCnt = GetUint64(cntRows[0].at("RowCnt"));
            if (rowCnt == 0) {
                Sleep(TDuration::Seconds(1));
                continue;
            }
            auto byteRows = ExecuteScanQuery(tableClient, bytesQuery, false);
            UNIT_ASSERT(!byteRows.empty());
            storedBytes = GetUint64(byteRows[0].at("Bytes"));
            if (storedBytes > 0) {
                break;
            }
            Sleep(TDuration::Seconds(1));
        }

        UNIT_ASSERT_C(rowCnt > 0, "expected data rows after bulk upsert");
        UNIT_ASSERT_C(storedBytes > 0, "expected primary_index_stats BlobRangeSize after bulk upsert");

        const ui64 logicalBytes = rowCnt * (8 + params.StringLen);
        const double amp = static_cast<double>(storedBytes) / static_cast<double>(logicalBytes);
        UNIT_ASSERT_C(amp < 1.5,
            TStringBuilder() << "amplification too high: " << amp << " stored=" << storedBytes << " logical=" << logicalBytes
                             << " rows=" << rowCnt);
    }
}

} // namespace NKikimr::NKqp
