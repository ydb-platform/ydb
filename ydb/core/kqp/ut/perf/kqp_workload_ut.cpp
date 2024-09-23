#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

#include <ydb/library/workload/abstract/workload_factory.h>
#include <ydb/library/workload/stock/stock.h>
#include <ydb/library/workload/stock/stock.h_serialized.h>
#include <ydb/library/workload/kv/kv.h>
#include <ydb/library/workload/kv/kv.h_serialized.h>

#include <library/cpp/threading/local_executor/local_executor.h>
#include <util/generic/serialized_enum.h>

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

void Test(const TString& workloadType) {
    auto settings = TKikimrSettings().SetWithSampleTables(false);
    auto kikimr = TKikimrRunner{settings};
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    auto params = NYdbWorkload::TWorkloadFactory::MakeHolder(workloadType);
    UNIT_ASSERT(params);
    if (auto* stockParams = dynamic_cast<NYdbWorkload::TStockWorkloadParams*>(params.Get())) {
        stockParams->ProductCount = 100;
        stockParams->Quantity = 1000;
        stockParams->OrderCount = 100;
        stockParams->Limit = 10;
        stockParams->MinPartitions = 40;
        stockParams->PartitionsByLoad = true;
    }
    params->DbPath = "/Root";
    auto workloadQueryGen = params->CreateGenerator();

    auto result = session.ExecuteSchemeQuery(workloadQueryGen->GetDDLQueries()).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    auto queriesList = workloadQueryGen->GetInitialData();
    for (const auto& queryInfo : queriesList) {
        auto result = session.ExecuteDataQuery(TString(queryInfo.Query),
            TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), queryInfo.Params).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }
    int maxType = 0;
    if (workloadType == "stock") {
        maxType = GetEnumItemsCount<NYdbWorkload::TStockWorkloadGenerator::EType>();
    } else if (workloadType == "kv") {
        maxType = GetEnumItemsCount<NYdbWorkload::TKvWorkloadGenerator::EType>();
    }
    for (int type = 0; type < maxType; ++type) {
        size_t InFlight = 10;
        NPar::LocalExecutor().RunAdditionalThreads(InFlight);
        NPar::LocalExecutor().ExecRange([&db, type, &params, workloadType](int /*id*/) {
            TTimer t;
            auto workloadQueryGen = params->CreateGenerator();
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
        Test("stock");
    }

    Y_UNIT_TEST(KV) {
        Test("kv");
    }
}
}
