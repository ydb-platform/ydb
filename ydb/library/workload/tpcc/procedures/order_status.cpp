#include "order_status.h"

#include <ydb/library/workload/tpcc/util.h>


namespace NYdbWorkload {
namespace NTPCC {

TOrderStatusProcedure::TOrderStatusProcedure(std::shared_ptr<NTable::TTableClient> tableClient,
                                             TTerminal& terminal, TLog& log, bool debug, ui64 seed) 
    : IProcedure(EProcedureType::OrderStatus, terminal, log, tableClient, debug, seed)
    , Stage(BEGIN)
{
}

NThreading::TFuture<void> TOrderStatusProcedure::Run(std::shared_ptr<TProcedureResource> resource) {
    Resource = std::dynamic_pointer_cast<OrderStatusResource>(std::move(resource));
    
    // After each stage, the next one will be called
    return RunStages();
}

NThreading::TFuture<void> TOrderStatusProcedure::RunCurrentStage(TSession session) {
    NThreading::TFuture<void> future;

    switch (Stage) {
        case OrderStatusStage::START_TRANSACTION:
            future = StartTransaction(session, DebugMode);
            break;
        case OrderStatusStage::GET_CUSTOMER:
            future = GetCustomer(Resource->WarehouseId, Resource->DistrictId, Resource->CustomerIdDelta, session);
            break;
        case OrderStatusStage::GET_ORDER_DETAILS:
            future = GetOrderDetails(Resource->WarehouseId, Resource->DistrictId, session);
            break;
        case OrderStatusStage::GET_ORDER_LINES:
            future = GetOrderLines(Resource->WarehouseId, Resource->DistrictId, Oorder.Id, session);
            break;
        case OrderStatusStage::END:
            future = CommitTransaction(DebugMode);
            break;
        default:
            return NThreading::MakeErrorFuture<void>(std::make_exception_ptr(yexception() << "Not implemented stage for OrderStatus"));
    }

    return future;
}

TString TOrderStatusProcedure::GetStageName() {
    TStringStream stream;
    stream << Stage << "(" << static_cast<i32>(Stage) << ")";
    return stream.Str();
}

bool TOrderStatusProcedure::NotStarted() {
    return Stage == OrderStatusStage::BEGIN;
}

bool TOrderStatusProcedure::Finished() {
    return Stage == OrderStatusStage::END;
}

void TOrderStatusProcedure::NextStage() {    
    Y_ENSURE(Stage != OrderStatusStage::END, "The procedure is finished, but there was an attempt to get the next stage");

    Stage = static_cast<OrderStatusStage>(static_cast<int>(Stage) + 1);
}

void TOrderStatusProcedure::StartOver() {
    Stage = OrderStatusStage::BEGIN;

    OrderLines.clear();
}

NThreading::TFuture<void> TOrderStatusProcedure::GetCustomer(i32 whId, i32 distId, i32 customerIdDelta,
                                                             NTable::TSession& session) {
    i32 random = UniformRandom32(1, 100, Rng);
    if (random <= 60) {
        TString lastName = GetNonUniformRandomLastNameForRun(Rng);
        return Customer.GetCustomerByName(Transaction, Log, Terminal, whId, distId, lastName, session);
    } else {
        i32 custId = GetNonUniformCustomerId(customerIdDelta, Rng);
        return Customer.GetCustomerById(Transaction, Log, Terminal, whId, distId, custId, session);
    }
}

NThreading::TFuture<void> TOrderStatusProcedure::GetOrderDetails(i32 whId, i32 distId, NTable::TSession& session) {
    std::string funcName = __func__;
    
    TString query = R"(
        --!syntax_v1

        DECLARE $whId AS Int32;
        DECLARE $distId AS Int32;
        DECLARE $custId AS Int32;

        SELECT O_W_ID, O_D_ID, O_C_ID, O_ID, O_CARRIER_ID, O_ENTRY_D
          FROM `oorder` VIEW idx_order as idx
         WHERE idx.O_W_ID = $whId
           AND idx.O_D_ID = $distId
           AND idx.O_C_ID = $custId
         ORDER BY idx.O_W_ID DESC, idx.O_D_ID DESC, idx.O_C_ID DESC, idx.O_ID DESC
         LIMIT 1;
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
                .Build();


            auto processing = [this, whId, distId, funcName](const NTable::TAsyncDataQueryResult& future) {
                NTable::TDataQueryResult result = future.GetValue();
                TStringStream stream;
                stream << "Failed to retrieve OORDER records [W_ID=" << whId << ", D_ID=" << distId << ", C_ID=" << Customer.Id << "]";
                ThrowOnError(result, Log, funcName + stream.Str());

                TResultSetParser parser(result.GetResultSet(0));
                if (!parser.TryNextRow()) {
                    throw yexception() << "No order records for OORDER [W_ID=" << whId << ", D_ID=" << distId << ", C_ID=" << Customer.Id << "]";
                }
                Oorder.Id = parser.ColumnParser("O_ID").GetInt32();
                Oorder.CarrierId = parser.ColumnParser("O_CARRIER_ID").GetOptionalInt32().GetOrElse(0);
                Oorder.EntryD = parser.ColumnParser("O_ENTRY_D").GetOptionalTimestamp().GetOrElse(TInstant::Zero());
            };

            return Terminal.ExecuteAndProcessingDataQuery(dataQuery, std::move(params), Transaction, processing, TString(funcName));
        }
    );
}

NThreading::TFuture<void> TOrderStatusProcedure::GetOrderLines(i32 whId, i32 distId, i32 orderId,
                                                               NTable::TSession& session) {
    std::string funcName = __func__;
    
    TString query = R"(
        --!syntax_v1

        DECLARE $whId AS Int32;
        DECLARE $distId AS Int32;
        DECLARE $orderId AS Int32;

        SELECT OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D
          FROM `order_line`
         WHERE OL_O_ID = $orderId
           AND OL_D_ID = $distId
           AND OL_W_ID = $whId;
    )";
    
    return Terminal.ApplyByThreadPool(
        session.PrepareDataQuery(query),
        [this, whId, distId, orderId, funcName](const TAsyncPrepareQueryResult& future){
            auto dataQuery = future.GetValue().GetQuery();

            auto params = dataQuery.GetParamsBuilder()
                .AddParam("$whId")
                    .Int32(whId)
                    .Build()
                .AddParam("$distId")
                    .Int32(distId)
                    .Build()
                .AddParam("$orderId")
                    .Int32(orderId)
                    .Build()
                .Build();


            auto processing = [this, whId, distId, orderId, funcName](const NTable::TAsyncDataQueryResult& future) {
                NTable::TDataQueryResult result = future.GetValue();
                TStringStream stream;
                stream << "Failed to retrieve ORDER_LINE records [W_ID=" << whId << ", D_ID=" << distId << ", O_ID=" << orderId << "]";
                ThrowOnError(result, Log, funcName + stream.Str());

                TResultSetParser parser(result.GetResultSet(0));
                while (parser.TryNextRow()) {        
                    TStringStream str;
                    str << "[";
                    str << parser.ColumnParser("OL_SUPPLY_W_ID").GetOptionalInt32().GetOrElse(0) << " - ";
                    str << parser.ColumnParser("OL_I_ID").GetOptionalInt32().GetOrElse(0) << " - ";
                    str << parser.ColumnParser("OL_QUANTITY").GetOptionalDouble().GetOrElse(0.0) << " - ";

                    double amount = parser.ColumnParser("OL_AMOUNT").GetOptionalDouble().GetOrElse(0.0);
                    std::string strAmount = std::to_string(amount);
                    if (strAmount.size() > 6) {
                        strAmount = strAmount.substr(0, 6);
                    }
                    str << strAmount << " - ";

                    std::string strDelivery;
                    TInstant deliveryOpt = parser.ColumnParser("OL_DELIVERY_D").GetOptionalTimestamp().GetOrElse(TInstant::Zero());
                    if (deliveryOpt == TInstant::Zero()) {
                        strDelivery = TInstant::Max().ToString();
                    } else {
                        strDelivery = deliveryOpt.ToString();
                    }
                    str << strDelivery << " - ";
                    str << "]";
                    OrderLines.push_back(str.Str());
                }
            };

            return Terminal.ExecuteAndProcessingDataQuery(dataQuery, std::move(params), Transaction, processing, TString(funcName));
        }
    );
}


}
}
