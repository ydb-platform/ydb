#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

#include <ydb/library/workload/workload_factory.h>
#include <ydb/library/workload/stock_workload.h>
#include <ydb/library/workload/kv_workload.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

constexpr size_t REPEATS = NSan::PlainOrUnderSanitizer(3, 1);

void Test(NYdbWorkload::EWorkload workloadType) {
    auto settings = TKikimrSettings().SetWithSampleTables(false);
    auto kikimr = TKikimrRunner{settings};
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    NYdbWorkload::TWorkloadFactory factory;

    std::shared_ptr<NYdbWorkload::IWorkloadQueryGenerator> workloadQueryGen;

    if (workloadType == NYdbWorkload::EWorkload::STOCK) {
        NYdbWorkload::TStockWorkloadParams params;
        params.ProductCount = 100;
        params.Quantity = 1000;
        params.OrderCount = 100;
        params.Limit = 10;
        params.MinPartitions = 40;
        params.PartitionsByLoad = true;
        params.DbPath = "/Root";
        workloadQueryGen = factory.GetWorkloadQueryGenerator(workloadType, &params);
    } else if (workloadType == NYdbWorkload::EWorkload::KV) {
        NYdbWorkload::TKvWorkloadParams params;
        params.DbPath = "/Root";
        workloadQueryGen = factory.GetWorkloadQueryGenerator(workloadType, &params);
    } else {
        UNIT_ASSERT(false);
    }

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
        TTimer t;
        for (size_t i = 0; i < REPEATS; ++i) {
            queriesList = workloadQueryGen->GetWorkload(type);
            for (const auto& queryInfo : queriesList) {
                auto result = session.ExecuteDataQuery(TString(queryInfo.Query),
                    TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), queryInfo.Params).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess() || result.GetStatus() == NYdb::EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
            }
        }
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
