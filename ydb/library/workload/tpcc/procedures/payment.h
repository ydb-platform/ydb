#pragma once

#include <ydb/library/workload/tpcc/config.h>
#include <ydb/library/workload/tpcc/procedures/tables.h>
#include <ydb/library/workload/tpcc/procedures/procedure.h>

namespace NYdbWorkload {
namespace NTPCC {

class TPaymentProcedure : public IProcedure {
public:
    enum PaymentStage {
        BEGIN,
        START_TRANSACTION,
        GET_WAREHOUSE,
        GET_DISTRICT,
        UPDATE_WAREHOUSE,
        UPDATE_DISTRICT,
        GET_CUSTOMER,
        GET_CUSTOMER_DATA,
        UPDATE_BALANCE,
        INSERT_HISTORY,
        END
    };

    struct PaymentResource : public TProcedureResource {
        i32 WarehouseId = 0;
        i32 DistrictId = 0;
        double PaymentAmount = 0.0;
        i32 WarehouseCount = 0;
        i32 CustomerWhId = 0;
        i32 CustomerDistId = 0;

        i32 CustomerIdDelta = 0;
    };

public:
    TPaymentProcedure(std::shared_ptr<NTable::TTableClient> tableClient, 
                      TTerminal& terminal, TLog& log, bool debug, ui64 seed);
    ~TPaymentProcedure() = default;

    NThreading::TFuture<void> Run(std::shared_ptr<TProcedureResource> resource) override;

    bool NotStarted() override;

    bool Finished() override;

    void StartOver() override;

    TString GetStageName() override;

private:
    void NextStage() override;

    NThreading::TFuture<void> RunCurrentStage(TSession session) override;

    NThreading::TFuture<void> UpdateWarehouse(i32 whId, double paymentAmount, NTable::TSession& session);
    NThreading::TFuture<void> UpdateDistrict(i32 whId, i32 distId, double paymentAmount,
                                             NTable::TSession& session);
    NThreading::TFuture<void> GetCustomer(i32 whId, i32 distId, i32 customerIdDelta, NTable::TSession& session);
    NThreading::TFuture<void> GetCustomerData(i32 custWhId, i32 custDistId, NTable::TSession& session);
    NThreading::TFuture<void> UpdateBalance(i32 whId, i32 distId, NTable::TSession& session);
    NThreading::TFuture<void> InsertHistory(i32 whId, i32 distId, i32 custWhId, i32 custDistId, double paymentAmount,
                                            NTable::TSession& session);

    PaymentStage Stage;
    std::shared_ptr<PaymentResource> Resource;

    TCustomerData Customer;
    TDistrictData District;
    TWarehouseData Warehouse;
};

}
}
