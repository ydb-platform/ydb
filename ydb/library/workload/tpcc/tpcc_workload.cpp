#include "tpcc_workload.h"

#include <ydb/library/workload/tpcc/tpcc_util.h>
#include <ydb/library/workload/tpcc/tpcc_thread_resource.h>
#include <ydb/library/workload/tpcc/load_data/customer.h>
#include <ydb/library/workload/tpcc/load_data/district.h>
#include <ydb/library/workload/tpcc/load_data/history.h>
#include <ydb/library/workload/tpcc/load_data/item.h>
#include <ydb/library/workload/tpcc/load_data/new_order.h>
#include <ydb/library/workload/tpcc/load_data/oorder.h>
#include <ydb/library/workload/tpcc/load_data/order_line.h>
#include <ydb/library/workload/tpcc/load_data/stock.h>
#include <ydb/library/workload/tpcc/load_data/warehouse.h>

#include <ydb/public/lib/ydb_cli/commands/ydb_common.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <util/string/printf.h>
#include <library/cpp/logger/stream_creator.h>


namespace NYdbWorkload {
namespace NTPCC {

TTPCCWorkload::TTPCCWorkload(std::shared_ptr<NYdb::TDriver>& driver, TTPCCWorkloadParams& params)
    : Params(params)
    , Driver(driver)
    , Log(TCoutLogBackendCreator().CreateLogBackend())
    , Seed(Now().MicroSeconds())
    , Rng(Seed)
{
    std::string message = "Seed: " + std::to_string(Seed) + "\n";
    Log.Write(message.c_str(), message.size());
}

TString TTPCCWorkload::CreateTablesQuery() {
    TStringStream query;
    query << "--!syntax_v1";
    TString whParts = PartitionByKeys(Params.Warehouses, Params.Threads);
    query << TWarehouseLoadQueryGenerator::GetCreateDDL(TString(Params.DbPath));
    query << TItemLoadQueryGenerator::GetCreateDDL(TString(Params.DbPath));
    query << TDistrictLoadQueryGenerator::GetCreateDDL(TString(Params.DbPath));
    query << TStockLoadQueryGenerator::GetCreateDDL(TString(Params.DbPath), Params.Threads, whParts);
    query << TCustomerLoadQueryGenerator::GetCreateDDL(TString(Params.DbPath), Params.Threads, whParts);
    query << TOorderLoadQueryGenerator::GetCreateDDL(TString(Params.DbPath), Params.Threads, whParts);
    query << TOrderLineLoadQueryGenerator::GetCreateDDL(TString(Params.DbPath), Params.Threads, whParts);
    query << TNewOrderLoadQueryGenerator::GetCreateDDL(TString(Params.DbPath), Params.Threads, whParts);
    query << THistoryLoadQueryGenerator::GetCreateDDL(TString(Params.DbPath), Params.Threads, whParts);
    return query.Str();
}

TString TTPCCWorkload::CleanTablesQuery() {
    TStringStream query;
    query << THistoryLoadQueryGenerator::GetCleanDDL();
    query << TNewOrderLoadQueryGenerator::GetCleanDDL();
    query << TOrderLineLoadQueryGenerator::GetCleanDDL();
    query << TOorderLoadQueryGenerator::GetCleanDDL();
    query << TCustomerLoadQueryGenerator::GetCleanDDL();
    query << TDistrictLoadQueryGenerator::GetCleanDDL();
    query << TStockLoadQueryGenerator::GetCleanDDL();
    query << TItemLoadQueryGenerator::GetCleanDDL();
    query << TWarehouseLoadQueryGenerator::GetCleanDDL();
    return query.Str();
}

int TTPCCWorkload::InitTables() {
    try {
        // Create Tables

        std::string message = "Creating tables...\n";
        Log.Write(message.c_str(), message.size());

        LoadThreadPool = std::make_unique<TLoadThreadPool>(Driver);

        auto tableClientSettings = NTable::TClientSettings()
                            .SessionPoolSettings(
                                NTable::TSessionPoolSettings()
                                    .MaxActiveSessions(1));

        auto db = std::make_unique<NTable::TTableClient>(*Driver, tableClientSettings);

        auto sessionResult = db->GetSession().GetValueSync();
        ThrowOnError(sessionResult);

        auto result = sessionResult.GetSession().ExecuteSchemeQuery(CreateTablesQuery()).GetValueSync();
        ThrowOnError(result);

        message = "DONE.\nLoading data...\n";
        Log.Write(message.c_str(), message.size());

        auto started = Now();


        // Load Data

        auto genParams = Params;
        LoadThreadPool->Start(Params.Threads);

        genParams.DbPath = Params.DbPath + "/warehouse";
        LoadThreadPool->SafeAddAndOwn(MakeHolder<TLoadTask>(
            std::make_unique<TWarehouseLoadQueryGenerator>(genParams, Rng.GenRand()))
        );

        genParams.DbPath = Params.DbPath + "/item";
        LoadThreadPool->SafeAddAndOwn(MakeHolder<TLoadTask>(
            std::make_unique<TItemLoadQueryGenerator>(genParams, Rng.GenRand()))
        );

        genParams.DbPath = Params.DbPath + "/district";
        LoadThreadPool->SafeAddAndOwn(MakeHolder<TLoadTask>(
            std::make_unique<TDistrictLoadQueryGenerator>(genParams, Rng.GenRand()))
        );

        for (i32 threadNum = 1; threadNum <= Params.Threads; ++threadNum) {
            genParams.DbPath = Params.DbPath + "/customer";
            LoadThreadPool->SafeAddAndOwn(MakeHolder<TLoadTask>(
                std::make_unique<TCustomerLoadQueryGenerator>(genParams, threadNum, Rng.GenRand()))
            )
;
        }
        for (i32 threadNum = 1; threadNum <= Params.Threads; ++threadNum) {
            genParams.DbPath = Params.DbPath + "/history";
            LoadThreadPool->SafeAddAndOwn(MakeHolder<TLoadTask>(
                std::make_unique<THistoryLoadQueryGenerator>(genParams, threadNum, Rng.GenRand()))
            )
;
        }
        for (i32 threadNum = 1; threadNum <= Params.Threads; ++threadNum) {
            genParams.DbPath = Params.DbPath + "/new_order";
            LoadThreadPool->SafeAddAndOwn(MakeHolder<TLoadTask>(
                std::make_unique<TNewOrderLoadQueryGenerator>(genParams, threadNum, Rng.GenRand()))
            )
;
        }
        for (i32 threadNum = 1; threadNum <= Params.Threads; ++threadNum) {
            genParams.DbPath = Params.DbPath + "/oorder";
            LoadThreadPool->SafeAddAndOwn(MakeHolder<TLoadTask>(
                std::make_unique<TOorderLoadQueryGenerator>(genParams, threadNum, Rng.GenRand()))
            )
;
        }
        for (i32 threadNum = 1; threadNum <= Params.Threads; ++threadNum) {
            genParams.DbPath = Params.DbPath + "/stock";
            LoadThreadPool->SafeAddAndOwn(MakeHolder<TLoadTask>(
                std::make_unique<TStockLoadQueryGenerator>(genParams, threadNum, Rng.GenRand()))
            )
;
        }
        for (i32 threadNum = 1; threadNum <= Params.Threads; ++threadNum) {
            genParams.DbPath = Params.DbPath + "/order_line";
            LoadThreadPool->SafeAddAndOwn(MakeHolder<TLoadTask>(
                std::make_unique<TOrderLineLoadQueryGenerator>(genParams, threadNum, Rng.GenRand()))
            )
;
        }
        LoadThreadPool->Stop();

        message = "Duration:" + (Now() - started).ToString() + "\nDONE\nConfiguring partitioning\n";
        Log.Write(message.c_str(), message.size());

        ConfigurePartitioning();

        message = "DONE\n";
        Log.Write(message.c_str(), message.size());

    } catch (const TYdbErrorException& ex) {
        Cerr << ex;
        return EXIT_FAILURE;
    } catch (...) {
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}

int TTPCCWorkload::CleanTables() {
    try {
        auto db = NTable::TTableClient(*Driver);

        auto sessionResult = db.GetSession().GetValueSync();
        ThrowOnError(sessionResult);

        auto result = sessionResult.GetSession().ExecuteSchemeQuery(CleanTablesQuery()).GetValueSync();
        ThrowOnError(result);
    } catch (TYdbErrorException ex) {
        Cerr << ex;
        return EXIT_FAILURE;
    } catch (...) {
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}

void TTPCCWorkload::ConfigurePartitioning() {
    static const std::vector<TString> heavyTables {
        "customer", "history", "new_order",
        "oorder", "order_line", "stock"
    };

    auto tableClientSettings = NTable::TClientSettings()
                        .SessionPoolSettings(
                            NTable::TSessionPoolSettings()
                                .MaxActiveSessions(1));

    TThreadResource rsc(std::make_shared<NTable::TTableClient>(*Driver, tableClientSettings));

    TString query;
    for (auto table: heavyTables) {
        query += Sprintf(R"(
            ALTER TABLE `%s` SET (
                AUTO_PARTITIONING_BY_LOAD = %s,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = %s,
                AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = %s
            );
        )", table.c_str(), Params.AutoPartitioning ? "ENABLED" : "DISABLED"
          , ToString(Params.MinPartitions).c_str(), ToString(Params.MaxPartitions).c_str());
    }

    TStatus result = rsc.Session->ExecuteSchemeQuery(query.c_str()).GetValueSync();
    if (!result.IsSuccess()) {
        ythrow yexception() << "Configuring partitioning failed with issues: " << result.GetIssues().ToString();
    }

    std::vector<ui32> prevPartition(heavyTables.size());
    TVector<NThreading::TFuture<void>> toWait;

    while (true) {
        toWait.clear();

        std::atomic_bool done = true;
        for (ui32 i = 0; i < heavyTables.size(); ++i) {
            NThreading::TFuture<void> future = rsc.Session->DescribeTable(
                Params.DbPath + "/" + heavyTables[i],
                NTable::TDescribeTableSettings().WithTableStatistics(true)
            ).Apply(
                [&prevPartition, i, &done](const NTable::TAsyncDescribeTableResult& future) {
                    NTable::TDescribeTableResult result = future.GetValueSync();

                    if (!result.IsSuccess()) {
                        ythrow yexception() << "Getting describe table failed with issues: " << result.GetIssues().ToString();
                    }

                    ui32 partCount = result.GetTableDescription().GetPartitionsCount();
                    if (partCount != prevPartition[i]) {
                        prevPartition[i] = partCount;
                        done.store(false);
                    }
                }
            );
            toWait.emplace_back(std::move(future));
        }

        NThreading::WaitExceptionOrAll(toWait).GetValueSync();

        if (done.load()) {
            break;
        } else {
            Sleep(TDuration::Seconds(1));
        }
    }
}

TString TTPCCWorkload::PartitionByKeys(i32 keysCount, i32 partsCount) {
    TStringStream partition;
    for (i32 i = 1; i < partsCount; i++) {
        partition << i * keysCount / partsCount;
        if (i != partsCount - 1) {
            partition << ", ";
        }
    }
    return partition.Str();
}

int TTPCCWorkload::RunWorkload() {
    Params.OrderLineIdConst = Rng.GenRand() % 8192;
    return EXIT_FAILURE;
}

}
}
