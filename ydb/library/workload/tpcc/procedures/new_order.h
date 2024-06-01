#pragma once

#include <ydb/library/workload/tpcc/config.h>
#include <ydb/library/workload/tpcc/procedures/tables.h>
#include <ydb/library/workload/tpcc/procedures/procedure.h>

namespace NYdbWorkload {
namespace NTPCC {

using namespace NYdb;

class TNewOrderProcedure : public IProcedure {
public:
    enum NewOrderStage {
        BEGIN,
        START_TRANSACTION,
        GET_CUSTOMER,
        GET_WAREHOUSE,
        GET_DISTRICT,
        UPDATE_DISTRICT,
        INSERT_OPEN_ORDER,
        INSERT_NEW_ORDER,
        GET_ITEM_PRICES,
        GET_STOCKS,
        INSERT_ORDER_LINE,
        UPDATE_STOCK,
        END
    };

    struct NewOrderResource : public TProcedureResource {
        i32 WarehouseId = 0;
        i32 DistrictId = 0;
        i32 CustomerId = 0;
        i32 ItemCount = 0;
        bool AllLocal = false;

        std::vector<i32> ItemIds;
        std::vector<i32> SupplierWarehouseIds;
        std::vector<i32> OrderQuantities;
    };
public:
    TNewOrderProcedure(std::shared_ptr<NTable::TTableClient> tableClient,
                       TTerminal& terminal, TLog& log, bool debug, ui64 seed);
    ~TNewOrderProcedure() = default;

    NThreading::TFuture<void> Run(std::shared_ptr<TProcedureResource> resource) override;

    bool NotStarted() override;

    bool Finished() override;

    void StartOver() override;

    TString GetStageName() override;

private:
    void NextStage() override;
    
    NThreading::TFuture<void> RunCurrentStage(TSession session) override;

    NThreading::TFuture<void> UpdateDistrict(i32 whId, i32 distId, NTable::TSession& session);

    NThreading::TFuture<void> GetItemPrices(const std::vector<i32>& itemIds, NTable::TSession& session);
    NThreading::TFuture<void> GetStocks(const std::vector<i32>& itemIds, const std::vector<i32>& supplierWarehouseIds,
                                        const std::vector<i32>& orderQuantities, NTable::TSession& session);
    NThreading::TFuture<void> InsertOrderLine(NTable::TSession& session);
    NThreading::TFuture<void> UpdateStock(NTable::TSession& session);

    TString GetDistInfo(i32 d_id, const TStockData& stock);

    NewOrderStage Stage;
    std::shared_ptr<NewOrderResource> Resource;

    TCustomerData Customer;
    TDistrictData District;
    TWarehouseData Warehouse;

    i32 Position = 0;
    i32 ItemCount;
    std::vector<double> ItemPrices;
    std::vector<TStockData> Stocks;
};

}
}
