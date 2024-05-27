#pragma once

#include <ydb/library/workload/tpcc/config.h>
#include <ydb/library/workload/tpcc/procedures/tables.h>
#include <ydb/library/workload/tpcc/procedures/procedure.h>

namespace NYdbWorkload {
namespace NTPCC {

class TStockLevelProcedure : public IProcedure {
public:
    enum StockLevelStage {
        BEGIN,
        START_TRANSACTION,
        GET_DISTRICT,
        GET_STOCK_COUNT,
        END
    };

    struct StockLevelResource : public TProcedureResource {
        i32 WarehouseId = 0;
        i32 DistrictId = 0;
        i32 Threshold = 0;
    };

public:
    TStockLevelProcedure(std::shared_ptr<NTable::TTableClient> tableClient,
                         TTerminal& terminal, TLog& log, bool debug, ui64 seed);
    ~TStockLevelProcedure() = default;

    NThreading::TFuture<void> Run(std::shared_ptr<TProcedureResource> resource) override;

    bool NotStarted() override;

    bool Finished() override;

    void StartOver() override;

    TString GetStageName() override;

private:
    void NextStage() override;
    
    NThreading::TFuture<void> RunCurrentStage(TSession session) override;

    NThreading::TFuture<void> GetStockCount(i32 whId, i32 distId, i32 orderId, i32 threshold, NTable::TSession& session);

    StockLevelStage Stage;
    std::shared_ptr<StockLevelResource> Resource;

    TDistrictData District;
    i32 StockCount;
};

}
}
