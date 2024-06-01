#include "payment.h"

#include <ydb/library/workload/tpcc/util.h>

#include <util/string/cast.h>


namespace NYdbWorkload {
namespace NTPCC {

TPaymentProcedure::TPaymentProcedure(std::shared_ptr<NTable::TTableClient> tableClient,
                                     TTerminal& terminal, TLog& log, bool debug, ui64 seed) 
    : IProcedure(EProcedureType::Payment, terminal, log, tableClient, debug, seed)
    , Stage(BEGIN)
{
}

NThreading::TFuture<void> TPaymentProcedure::Run(std::shared_ptr<TProcedureResource> resource) {
    Resource = std::dynamic_pointer_cast<PaymentResource>(std::move(resource));
    
    // After each stage, the next one will be called
    return RunStages();
}

NThreading::TFuture<void> TPaymentProcedure::RunCurrentStage(TSession session) {
    NThreading::TFuture<void> future;

    switch (Stage) {
        case PaymentStage::START_TRANSACTION:
            future = StartTransaction(session, DebugMode);
            break;
        case PaymentStage::GET_WAREHOUSE:
            future = Warehouse.GetWarehouse(Transaction, Log, Terminal, Resource->WarehouseId, session);
            break;
        case PaymentStage::GET_DISTRICT:
            future = District.GetDistrict(Transaction, Log, Terminal, Resource->WarehouseId, Resource->DistrictId, session);
            break;
        case PaymentStage::UPDATE_WAREHOUSE:
            future = UpdateWarehouse(Resource->WarehouseId, Resource->PaymentAmount, session);
            break;
        case PaymentStage::UPDATE_DISTRICT:
            future = UpdateDistrict(Resource->WarehouseId, Resource->DistrictId, Resource->PaymentAmount, session);
            break;
        case PaymentStage::GET_CUSTOMER:
            future = GetCustomer(Resource->CustomerWhId, Resource->CustomerDistId, Resource->CustomerIdDelta, session);
            break;
        case PaymentStage::GET_CUSTOMER_DATA:
            if (Customer.Credit.equal("BC")) {
                future = Customer.GetCustomerDataById(
                    Transaction, Log, Terminal, Resource->CustomerWhId, Resource->CustomerDistId,
                    Customer.Id, session
                );
            } else {
                future = NThreading::MakeFuture();
            }
            break;
        case PaymentStage::UPDATE_BALANCE:
        {
            Customer.Balance -= Resource->PaymentAmount;
            Customer.YtdPayment += Resource->PaymentAmount;
            Customer.PaymentCount++;

            TStringStream stream;
            stream << Customer.Id << " " << Resource->CustomerDistId << " " << Resource->CustomerWhId << " " << Resource->DistrictId << " "
                   << Resource->WarehouseId << " " << Resource->PaymentAmount << " | " << Customer.Data;
            Customer.Data = stream.Str();
            if (Customer.Data.Size() > 500) {
                Customer.Data = Customer.Data.substr(0, 500);
            }

            future = UpdateBalance(Resource->CustomerWhId, Resource->CustomerDistId, session);
            break;
        }
        case PaymentStage::INSERT_HISTORY:
            future = InsertHistory(Resource->WarehouseId, Resource->DistrictId, Resource->CustomerWhId,
                                 Resource->CustomerDistId, Resource->PaymentAmount, session);
            break;
        case PaymentStage::END:
            future = CommitTransaction(DebugMode);
            break;
        default:
            return NThreading::MakeErrorFuture<void>(std::make_exception_ptr(yexception() << "Not implemented stage for Payment"));
    }

    return future;
}

TString TPaymentProcedure::GetStageName() {
    TStringStream stream;
    stream << Stage << "(" << static_cast<i32>(Stage) << ")";
    return stream.Str();
}

bool TPaymentProcedure::NotStarted() {
    return Stage == PaymentStage::BEGIN;
}

bool TPaymentProcedure::Finished() {
    return Stage == PaymentStage::END;
}

void TPaymentProcedure::NextStage() {    
    Y_ENSURE(Stage != PaymentStage::END, "The procedure is finished, but there was an attempt to get the next stage");

    Stage = static_cast<PaymentStage>(static_cast<int>(Stage) + 1);
}

void TPaymentProcedure::StartOver() {
    Stage = PaymentStage::BEGIN;
}

NThreading::TFuture<void> TPaymentProcedure::UpdateWarehouse(i32 whId, double paymentAmount,
                                                             NTable::TSession& session) {
    std::string funcName = __func__;
    
    TString query = R"(
        --!syntax_v1

        DECLARE $whId AS Int32;
        DECLARE $paymentAmount AS Double;

        UPDATE `warehouse`
           SET W_YTD = W_YTD + $paymentAmount
         WHERE W_ID = $whId;
    )";

    return Terminal.ApplyByThreadPool(
        session.PrepareDataQuery(query),
        [this, whId, paymentAmount, funcName](const TAsyncPrepareQueryResult& future){
            auto dataQuery = future.GetValue().GetQuery();

            auto params = dataQuery.GetParamsBuilder()
                .AddParam("$whId")
                    .Int32(whId)
                    .Build()
                .AddParam("$paymentAmount")
                    .Double(paymentAmount)
                    .Build()
                .Build();

            auto processing = [this, whId, paymentAmount, funcName](const NTable::TAsyncDataQueryResult& future) {
                NTable::TDataQueryResult result = future.GetValue();
                TStringStream stream;
                stream << "Failed to update WAREHOUSE [W_ID=" << whId << ", PAYMENT_AMOUNT=" << paymentAmount << "]";
                ThrowOnError(result, Log, funcName + stream.Str());
            };

            return Terminal.ExecuteAndProcessingDataQuery(dataQuery, std::move(params), Transaction, processing, TString(funcName));
        }
    );
}

NThreading::TFuture<void> TPaymentProcedure::UpdateDistrict(i32 whId, i32 distId, double paymentAmount,
                                                            NTable::TSession& session) {
    std::string funcName = __func__;
    
    TString query = R"(
        --!syntax_v1

        DECLARE $whId AS Int32;
        DECLARE $distId AS Int32;
        DECLARE $paymentAmount AS Double;

        UPDATE `district`
           SET D_YTD = D_YTD + $paymentAmount
         WHERE D_W_ID = $whId
           AND D_ID = $distId;
    )";

    return Terminal.ApplyByThreadPool(
        session.PrepareDataQuery(query),
        [this, whId, distId, paymentAmount, funcName](const TAsyncPrepareQueryResult& future){
            auto dataQuery = future.GetValue().GetQuery();
    
            auto params = dataQuery.GetParamsBuilder()
                .AddParam("$whId")
                    .Int32(whId)
                    .Build()
                .AddParam("$distId")
                    .Int32(distId)
                    .Build()
                .AddParam("$paymentAmount")
                    .Double(paymentAmount)
                    .Build()
                .Build();

            auto processing = [this, whId, distId, paymentAmount, funcName](const NTable::TAsyncDataQueryResult& future) {
                NTable::TDataQueryResult result = future.GetValue();
                TStringStream stream;
                stream << "Failed to update DISTRICT [W_ID=" << whId << ", D_ID=" << distId << ", PAYMENT_AMOUNT=" << paymentAmount << "]";
                ThrowOnError(result, Log, funcName + stream.Str());
            };

            return Terminal.ExecuteAndProcessingDataQuery(dataQuery, std::move(params), Transaction, processing, TString(funcName));
        }
    );
}

NThreading::TFuture<void> TPaymentProcedure::GetCustomer(i32 whId, i32 distId, i32 customerIdDelta, NTable::TSession& session) {
    i32 random = UniformRandom32(1, 100, Rng);
    if (random <= 60) {
        TString lastName = GetNonUniformRandomLastNameForRun(Rng);
        return Customer.GetCustomerByName(Transaction, Log, Terminal, whId, distId, lastName, session);
    } else {
        i32 custId = GetNonUniformCustomerId(customerIdDelta, Rng);
        return Customer.GetCustomerById(Transaction, Log, Terminal, whId, distId, custId, session);
    }
}

NThreading::TFuture<void> TPaymentProcedure::UpdateBalance(i32 whId, i32 distId, NTable::TSession& session) {
    std::string funcName = __func__;
    
    TString query = R"(
        --!syntax_v1

        DECLARE $whId AS Int32;
        DECLARE $distId AS Int32;
        DECLARE $custId AS Int32;
        DECLARE $balance AS Double;
        DECLARE $ytdPayment AS Double;
        DECLARE $paymentCount AS Int32;
        DECLARE $data AS Utf8;

        UPSERT INTO `customer`
         (C_W_ID, C_D_ID, C_ID, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA)
         VALUES ($whId, $distId, $custId, $balance, $ytdPayment, $paymentCount, $data);
    )";

    return Terminal.ApplyByThreadPool(
        session.PrepareDataQuery(query),
        [this, whId, distId, funcName](const TAsyncPrepareQueryResult& future){
            auto dataQuery = future.GetValue().GetQuery();

            auto params = dataQuery.GetParamsBuilder()
                .AddParam("$whId")
                    .Int32(whId)
                    .Build()
                .AddParam("$distId")
                    .Int32(distId)
                    .Build()
                .AddParam("$custId")
                    .Int32(Customer.Id)
                    .Build()
                .AddParam("$balance")
                    .Double(Customer.Balance)
                    .Build()
                .AddParam("$ytdPayment")
                    .Double(Customer.YtdPayment)
                    .Build()
                .AddParam("$paymentCount")
                    .Int32(Customer.PaymentCount)
                    .Build()
                .AddParam("$data")
                    .Utf8(Customer.Data)
                    .Build()
                .Build();

            auto processing = [this, whId, distId, funcName](const NTable::TAsyncDataQueryResult& future) {
                NTable::TDataQueryResult result = future.GetValue();
                TStringStream stream;
                stream << "Failed to update CUSTOMER [W_ID=" << whId << ", D_ID=" << distId << ", C_ID=" << Customer.Id << "]";
                ThrowOnError(result, Log, funcName + stream.Str());
            };

            return Terminal.ExecuteAndProcessingDataQuery(dataQuery, std::move(params), Transaction, processing, TString(funcName));
        }
    );
}

NThreading::TFuture<void> TPaymentProcedure::InsertHistory(i32 whId, i32 distId, i32 custWhId, i32 custDistId, double paymentAmount,
                                                           NTable::TSession& session) {
    std::string funcName = __func__;
    
    TString whName = Warehouse.Name;
    TString distName = District.Name;
    if (whName.Size() > 10) {
        whName = whName.substr(0, 10);
    }
    if (distName.Size() > 10) {
        distName = distName.substr(0, 10);
    }

    TString histData = whName + "    " + distName;

    TString query = R"(
        --!syntax_v1

        DECLARE $custId AS Int32;
        DECLARE $custWhId AS Int32;
        DECLARE $custDistId AS Int32;
        DECLARE $whId AS Int32;
        DECLARE $distId AS Int32;
        DECLARE $date AS Timestamp;
        DECLARE $paymentAmount AS Double;
        DECLARE $data AS Utf8;
        DECLARE $nano AS Int64;
        
        UPSERT INTO `history`
         (H_C_D_ID, H_C_W_ID, H_C_ID, H_D_ID, H_W_ID, H_DATE, H_AMOUNT, H_DATA, H_C_NANO_TS)
         VALUES ($custDistId, $custWhId, $custId, $distId, $whId, $date, $paymentAmount, $data, $nano);
    )";

    return Terminal.ApplyByThreadPool(
        session.PrepareDataQuery(query),
        [this, custWhId, custDistId, whId, distId, paymentAmount, funcName](const TAsyncPrepareQueryResult& future){
            auto dataQuery = future.GetValue().GetQuery();
    
            auto params = dataQuery.GetParamsBuilder()
                .AddParam("$custDistId")
                    .Int32(custDistId)
                    .Build()
                .AddParam("$custWhId")
                    .Int32(custWhId)
                    .Build()
                .AddParam("$custId")
                    .Int32(Customer.Id)
                    .Build()
                .AddParam("$distId")
                    .Int32(distId)
                    .Build()
                .AddParam("$whId")
                    .Int32(whId)
                    .Build()
                .AddParam("$date")
                    .Timestamp(Now())
                    .Build()
                .AddParam("$paymentAmount")
                    .Double(paymentAmount)
                    .Build()
                .AddParam("$data")
                    .Utf8(Customer.Data)
                    .Build()
                .AddParam("$nano")
                    .Int64(Now().NanoSeconds())
                    .Build()
                .Build();

            auto processing = [this, whId, distId, custWhId, custDistId, funcName](const NTable::TAsyncDataQueryResult& future) {
                NTable::TDataQueryResult result = future.GetValue();
                TStringStream stream;
                stream << "Failed to update HISTORY [W_ID=" << whId << ", D_ID=" << distId << ", C_ID=" << Customer.Id
                        << ", C_W_ID=" << custWhId << ", C_D_ID=" << custDistId <<  "]";
                ThrowOnError(result, Log, funcName + stream.Str());
            };

            return Terminal.ExecuteAndProcessingDataQuery(dataQuery, std::move(params), Transaction, processing, TString(funcName));
        }
    );
}



}
}
