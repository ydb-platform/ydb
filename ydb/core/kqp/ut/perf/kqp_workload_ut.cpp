#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

#include <ydb/library/workload/workload_factory.h>
#include <ydb/library/workload/stock_workload.h>
#include <ydb/library/workload/kv_workload.h>

#include <library/cpp/threading/local_executor/local_executor.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

constexpr size_t REPEATS = NSan::PlainOrUnderSanitizer(3, 1);

void ExecuteQuery(TTableClient& db, TSession& session, NYdbWorkload::TQueryInfo& queryInfo) {
    if (queryInfo.UseReadRows) {
        auto selectResult = db.ReadRows(queryInfo.TablePath, std::move(*queryInfo.KeyToRead))
            .GetValueSync();
    } else {
        auto result = session.ExecuteDataQuery(TString(queryInfo.Query),
            TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), queryInfo.Params).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess() || result.GetStatus() == NYdb::EStatus::PRECONDITION_FAILED
            || result.GetStatus() == NYdb::EStatus::ABORTED, result.GetIssues().ToString()
                << " status: " << int(result.GetStatus()));
    }
}

void Test(NYdbWorkload::EWorkload workloadType) {
    auto settings = TKikimrSettings().SetWithSampleTables(false);
    auto kikimr = TKikimrRunner{settings};
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    std::unique_ptr<NYdbWorkload::TWorkloadParams> params;
    if (workloadType == NYdbWorkload::EWorkload::STOCK) {
        auto stockParams = std::make_unique<NYdbWorkload::TStockWorkloadParams>();
        stockParams->ProductCount = 100;
        stockParams->Quantity = 1000;
        stockParams->OrderCount = 100;
        stockParams->Limit = 10;
        stockParams->MinPartitions = 40;
        stockParams->PartitionsByLoad = true;
        params = std::move(stockParams);
    } else if (workloadType == NYdbWorkload::EWorkload::KV) {
        params = std::make_unique<NYdbWorkload::TKvWorkloadParams>();
    } else {
        UNIT_ASSERT(false);
    }
    UNIT_ASSERT(params);
    params->DbPath = "/Root";
    auto workloadQueryGen = NYdbWorkload::TWorkloadFactory().GetWorkloadQueryGenerator(workloadType, params.get());

    auto result = session.ExecuteSchemeQuery(workloadQueryGen->GetDDLQueries()).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    auto queriesList = workloadQueryGen->GetInitialData();
    for (const auto& queryInfo : queriesList) {
        auto result = session.ExecuteDataQuery(TString(queryInfo.Query),
            TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), queryInfo.Params).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }
    int maxType = 0;
    if (workloadType == NYdbWorkload::EWorkload::STOCK) {
        maxType = static_cast<int>(NYdbWorkload::TStockWorkloadGenerator::EType::MaxType);
    } else if (workloadType == NYdbWorkload::EWorkload::KV) {
        maxType = static_cast<int>(NYdbWorkload::TKvWorkloadGenerator::EType::MaxType);
    }
    for (int type = 0; type < maxType; ++type) {
        size_t InFlight = 10;
        NPar::LocalExecutor().RunAdditionalThreads(InFlight);
        NPar::LocalExecutor().ExecRange([&db, type, &params, workloadType](int /*id*/) {
            TTimer t;
            auto workloadQueryGen = NYdbWorkload::TWorkloadFactory().GetWorkloadQueryGenerator(workloadType, params.get());
            auto session = db.CreateSession().GetValueSync().GetSession();
            for (size_t i = 0; i < REPEATS; ++i) {
                auto queriesList = workloadQueryGen->GetWorkload(type);
                for (auto& queryInfo : queriesList) {
                    ExecuteQuery(db, session, queryInfo);
                }
            }
        }, 0, InFlight, NPar::TLocalExecutor::WAIT_COMPLETE | NPar::TLocalExecutor::MED_PRIORITY);
    }
    result = session.ExecuteSchemeQuery(workloadQueryGen->GetCleanDDLQueries()).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

Y_UNIT_TEST_SUITE(KqpWorkload) {
    Y_UNIT_TEST(STOCK) {
        Test(NYdbWorkload::EWorkload::STOCK);
    }

    Y_UNIT_TEST(KV) {
        Test(NYdbWorkload::EWorkload::KV);
    }
}
}
