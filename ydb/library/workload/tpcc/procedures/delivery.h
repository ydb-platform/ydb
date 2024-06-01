#pragma once

#include <ydb/library/workload/tpcc/config.h>
#include <ydb/library/workload/tpcc/procedures/tables.h>
#include <ydb/library/workload/tpcc/procedures/procedure.h>

namespace NYdbWorkload {
namespace NTPCC {

using namespace NYdb;

class TDeliveryProcedure : public IProcedure {
public:
    enum DeliveryStage {
        BEGIN,
        START_TRANSACTION,
        GET_ORDER_IDS,
        GET_CUSTOMER_DATA_IDS,
        GET_CUSTOMER_DATA,
        GET_ORDER_LINE_DATA,
        DELETE_ORDER,
        UPDATE_CARRIER_ID,
        UPDATE_DELIVERY_DATE,
        UPDATE_BALANCE_AND_DELIVERY,
        END
    };

    struct OrderLineCollection {
        double TotalAmount = 0.0;
        std::vector<i32> LineNumbers;
    };

    struct DeliveryResource : public TProcedureResource {
        i32 WarehouseId = 0;
        i32 WarehouseCount = 0;
        i32 CarrierId = 0;
    };

public:
    TDeliveryProcedure(std::shared_ptr<NTable::TTableClient> tableClient, 
                       TTerminal& terminal, TLog& log, bool debug, ui64 seed);

    ~TDeliveryProcedure() = default;

    NThreading::TFuture<void> Run(std::shared_ptr<TProcedureResource> resource) override;

    bool NotStarted() override;

    bool Finished() override;

    void StartOver() override;

    TString GetStageName() override;

private:
    void NextStage() override;

    NThreading::TFuture<void> RunCurrentStage(TSession session) override;

    NThreading::TFuture<void> GetOrderIds(i32 whId, NTable::TSession session);
    NThreading::TFuture<void> GetCustomerDataIds(i32 whId, NTable::TSession session);
    NThreading::TFuture<void> GetCustomerData(i32 whId, NTable::TSession session);
    NThreading::TFuture<void> GetOrderLineCollection(i32 whId, NTable::TSession session);
    NThreading::TFuture<void> DeleteOrder(i32 whId, NTable::TSession session);
    NThreading::TFuture<void> UpdateCarrierId(i32 whId, i32 carrierId, NTable::TSession session);
    NThreading::TFuture<void> UpdateDeliveryDate(i32 whId, NTable::TSession session);
    NThreading::TFuture<void> UpdateBalanceAndDelivery(i32 whId, NTable::TSession session);

    void NextDistrictId();
    bool CheckDistrictId();

    DeliveryStage Stage;
    std::shared_ptr<DeliveryResource> Resource;

    i32 DistrictId = 1;
    std::vector<i32> OrderIds;
    std::vector<TCustomerData> CustomerData;
    std::vector<OrderLineCollection> OrderLineCollections;
};

}
}
