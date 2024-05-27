#pragma once

#include <ydb/library/workload/tpcc/config.h>
#include <ydb/library/workload/tpcc/procedures/tables.h>
#include <ydb/library/workload/tpcc/procedures/procedure.h>

namespace NYdbWorkload {
namespace NTPCC {

using namespace NYdb;

class TOrderStatusProcedure : public IProcedure {
public:
    enum OrderStatusStage {
        BEGIN,
        START_TRANSACTION,
        GET_CUSTOMER,
        GET_ORDER_DETAILS,
        GET_ORDER_LINES,
        END
    };

    struct OrderStatusResource : public TProcedureResource {
        i32 WarehouseId = 0;
        i32 DistrictId = 0;

        i32 CustomerIdDelta = 0;
    };
public:
    TOrderStatusProcedure(std::shared_ptr<NTable::TTableClient> tableClient, 
                          TTerminal& terminal, TLog& log, bool debug, ui64 seed);
    ~TOrderStatusProcedure() = default;

    NThreading::TFuture<void> Run(std::shared_ptr<TProcedureResource> resource) override;

    bool NotStarted() override;

    bool Finished() override;

    void StartOver() override;

    TString GetStageName() override;

private:
    void NextStage() override;
    
    NThreading::TFuture<void> RunCurrentStage(TSession session) override;

    NThreading::TFuture<void> GetCustomer(i32 whId, i32 distId, i32 customerIdDelta,
                                          NTable::TSession& session);
    NThreading::TFuture<void> GetOrderDetails(i32 whId, i32 distId, 
                                              NTable::TSession& session);
    NThreading::TFuture<void> GetOrderLines(i32 whId, i32 distId, i32 orderId, 
                                            NTable::TSession& session);

    OrderStatusStage Stage;
    std::shared_ptr<OrderStatusResource> Resource;

    TCustomerData Customer;
    TOorderData Oorder;
    std::vector<TString> OrderLines;
};

}
}
