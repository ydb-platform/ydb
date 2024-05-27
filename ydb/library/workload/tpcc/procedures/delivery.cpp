#include "delivery.h"

#include <util/string/cast.h>

namespace NYdbWorkload {
namespace NTPCC {

TDeliveryProcedure::TDeliveryProcedure(std::shared_ptr<TTableClient> tableClient, 
                                       TTerminal& terminal, TLog& log, bool debug, ui64 seed) 
    : IProcedure(EProcedureType::Delivery, terminal, log, tableClient, debug, seed)
    , Stage(BEGIN)
{
}

NThreading::TFuture<void> TDeliveryProcedure::Run(std::shared_ptr<TProcedureResource> resource) {
    Resource = std::dynamic_pointer_cast<DeliveryResource>(std::move(resource));
    
    // After each stage, the next one will be called
    return RunStages();
}

NThreading::TFuture<void> TDeliveryProcedure::RunCurrentStage(TSession session) {
    NThreading::TFuture<void> future;

    switch (Stage) {
        case DeliveryStage::START_TRANSACTION:
            future = StartTransaction(session, DebugMode);
            break;
        case DeliveryStage::GET_ORDER_IDS:
            future = GetOrderIds(Resource->WarehouseId, session);
            break;
        case DeliveryStage::GET_CUSTOMER_DATA_IDS:
            future = GetCustomerDataIds(Resource->WarehouseId, session);
            break;
        case DeliveryStage::GET_CUSTOMER_DATA:
            future = GetCustomerData(Resource->WarehouseId, session);
            break;
        case DeliveryStage::GET_ORDER_LINE_DATA:
            future = GetOrderLineCollection(Resource->WarehouseId, session);
            break;
        case DeliveryStage::DELETE_ORDER:
            future = DeleteOrder(Resource->WarehouseId, session);
            break;
        case DeliveryStage::UPDATE_CARRIER_ID:
            future = UpdateCarrierId(Resource->WarehouseId, Resource->CarrierId, session);
            break;
        case DeliveryStage::UPDATE_DELIVERY_DATE:
            future = UpdateDeliveryDate(Resource->WarehouseId, session);
            break;
        case DeliveryStage::UPDATE_BALANCE_AND_DELIVERY:
            future = UpdateBalanceAndDelivery(Resource->WarehouseId, session);
            break;
        case DeliveryStage::END:
            future = CommitTransaction(DebugMode);
            break;
        default:
            return NThreading::MakeErrorFuture<void>(std::make_exception_ptr(yexception() << "Not implemented stage for Delivery"));
    }

    return future;
}

bool TDeliveryProcedure::NotStarted() {
    return Stage == DeliveryStage::BEGIN;
}

bool TDeliveryProcedure::Finished() {
    return Stage == DeliveryStage::END;
}

void TDeliveryProcedure::NextStage() {
    Y_ENSURE(Stage != DeliveryStage::END, "The procedure is finished, but there was an attempt to get the next stage");

    switch (Stage) {
        case BEGIN:
        case START_TRANSACTION:
        case UPDATE_CARRIER_ID:
        case UPDATE_DELIVERY_DATE:
        case UPDATE_BALANCE_AND_DELIVERY:
        case END:
            Stage = static_cast<DeliveryStage>(static_cast<int>(Stage) + 1);
            break;
        default:
            NextDistrictId();
            if (!CheckDistrictId()) {
                Stage = static_cast<DeliveryStage>(static_cast<int>(Stage) + 1);
            }
    }
}

TString TDeliveryProcedure::GetStageName() {
    TStringStream stream;
    stream << Stage << "(" << static_cast<i32>(Stage) << ")";
    return stream.Str();
}

void TDeliveryProcedure::StartOver() {
    Stage = DeliveryStage::BEGIN;

    DistrictId = 1;
    OrderIds.clear();
    CustomerData.clear();
    OrderLineCollections.clear();
}

void TDeliveryProcedure::NextDistrictId() {
    do {
        ++DistrictId;
    } while (DistrictId <= EWorkloadConstants::TPCC_DIST_PER_WH 
             && Stage != DeliveryStage::GET_ORDER_IDS
             && OrderIds[DistrictId - 1] == -1);
}

bool TDeliveryProcedure::CheckDistrictId() {
    while (static_cast<ui32>(DistrictId) <= OrderIds.size()
            && OrderIds[DistrictId - 1] == -1) {
        ++DistrictId;
    }
    if (DistrictId > EWorkloadConstants::TPCC_DIST_PER_WH) {
        DistrictId = 1;
        return 0;
    }
    return 1;
}

NThreading::TFuture<void> TDeliveryProcedure::GetOrderIds(i32 whId, NTable::TSession session) {
    std::string funcName = __func__;
    
    TString query = R"(
        --!syntax_v1

        DECLARE $whId AS Int32;
        DECLARE $distId AS Int32;

        SELECT NO_W_ID, NO_D_ID, NO_O_ID 
         FROM `new_order`
         WHERE NO_W_ID = $whId
           AND NO_D_ID = $distId
         ORDER BY NO_W_ID ASC, NO_D_ID ASC, NO_O_ID ASC
        LIMIT 1;
    )";

    return Terminal.ApplyByThreadPool(
        session.PrepareDataQuery(query),
        [this, whId, funcName](const TAsyncPrepareQueryResult& future){
            auto dataQuery = future.GetValue().GetQuery();
    
            auto params = dataQuery.GetParamsBuilder()
                .AddParam("$whId")
                    .Int32(whId)
                    .Build()
                .AddParam("$distId")
                    .Int32(DistrictId)
                    .Build()
                .Build();

            auto processing = [this, whId, funcName](const NTable::TAsyncDataQueryResult& future) {
                NTable::TDataQueryResult result = future.GetValue();

                TStringStream stream;
                stream << "Failed to retrieve NEW_ORDER records [W_ID=" << whId << ", D_ID=" << DistrictId << "]";
                ThrowOnError(result, Log, funcName + stream.Str());
                
                if (DebugMode) {
                    Log.Write(TLOG_DEBUG, "In GetOrderIds: Order.size()=" + std::to_string(OrderIds.size()));
                }
                
                TResultSetParser parser(result.GetResultSet(0));
                if (!parser.TryNextRow()) {
                    // This district has no new orders. This can happen but should be rare
                    if (DebugMode) {
                        Log.Write(TLOG_DEBUG, "NEW_ORDER not found [W_ID=" + std::to_string(whId) + ", D_ID=" + std::to_string(DistrictId) + "]");
                    }
                    OrderIds.push_back(-1);
                } else {
                    OrderIds.push_back(parser.ColumnParser("NO_O_ID").GetInt32());
                }
            };

            return Terminal.ExecuteAndProcessingDataQuery(dataQuery, std::move(params), Transaction, processing, TString(funcName));
        }
    );
}

NThreading::TFuture<void> TDeliveryProcedure::GetCustomerDataIds(i32 whId, NTable::TSession session) {
    if (DistrictId == 1) {
        CustomerData.assign(OrderIds.size(), TCustomerData());
    }
    if (!CheckDistrictId()) {
        return NThreading::MakeFuture();
    }
    i32 distPos = DistrictId - 1;

    auto idFuture = TOorderData::GetCustomerId(Transaction, Log, Terminal, whId, DistrictId, OrderIds[distPos], session);

    return idFuture.Apply(
            [this, distPos] (const NThreading::TFuture<i32> &future) {
                CustomerData[distPos].Id = future.GetValue();
            }
        );
}


NThreading::TFuture<void> TDeliveryProcedure::GetCustomerData(i32 whId, NTable::TSession session) {
    std::string funcName = __func__;
    
    TString query = R"(
        --!syntax_v1

        DECLARE $whId AS Int32;
        DECLARE $distId AS Int32;
        DECLARE $custId AS Int32;

        SELECT C_BALANCE, C_DELIVERY_CNT
          FROM `customer`
         WHERE C_W_ID = $whId
           AND C_D_ID = $distId
           AND C_ID = $custId;
    )";

    return Terminal.ApplyByThreadPool(
        session.PrepareDataQuery(query),
        [this, whId, funcName](const TAsyncPrepareQueryResult& future){
            auto dataQuery = future.GetValue().GetQuery();

            if (!CheckDistrictId()) {
                return NThreading::MakeFuture();
            }
            auto distPos = DistrictId - 1;

            auto params = dataQuery.GetParamsBuilder()
                .AddParam("$whId")
                    .Int32(whId)
                    .Build()
                .AddParam("$distId")
                    .Int32(DistrictId)
                    .Build()
                .AddParam("$custId")
                    .Int32(CustomerData[distPos].Id)
                    .Build()
                .Build();

            auto processing = [this, whId, distPos, funcName](const NTable::TAsyncDataQueryResult& future) {
                NTable::TDataQueryResult result = future.GetValue();
                TStringStream stream;
                stream << "Failed to retrieve CUSTOMER records [W_ID=" << whId << ", D_ID=" << DistrictId << ", C_ID=" << CustomerData[distPos].Id << "]";
                ThrowOnError(result, Log, funcName + stream.Str());

                TResultSetParser parser(result.GetResultSet(0));
                if (!parser.TryNextRow()) {
                    throw yexception() << stream.Str();
                }
                CustomerData[distPos].Balance = parser.ColumnParser("C_BALANCE").GetOptionalDouble().GetOrElse(0.0);
                CustomerData[distPos].DeliveryCount = parser.ColumnParser("C_DELIVERY_CNT").GetOptionalInt32().GetOrElse(0);
            };

            return Terminal.ExecuteAndProcessingDataQuery(dataQuery, std::move(params), Transaction, processing, TString(funcName));
        }
    );
}

NThreading::TFuture<void> TDeliveryProcedure::GetOrderLineCollection(i32 whId, NTable::TSession session) {
    std::string funcName = __func__;
    
    TString query = R"(
        --!syntax_v1

        DECLARE $whId AS Int32;
        DECLARE $distId AS Int32;
        DECLARE $orderId AS Int32;

        SELECT OL_NUMBER, OL_AMOUNT
          FROM order_line
         WHERE OL_O_ID = $orderId
           AND OL_D_ID = $distId
           AND OL_W_ID = $whId;
    )";

    return Terminal.ApplyByThreadPool(
        session.PrepareDataQuery(query),
        [this, whId, funcName](const TAsyncPrepareQueryResult& future){
            auto dataQuery = future.GetValue().GetQuery();

            if (DistrictId == 1) {
                OrderLineCollections.assign(OrderIds.size(), OrderLineCollection());
            }
            if (!CheckDistrictId()) {
                return NThreading::MakeFuture();
            }
            auto distPos = DistrictId - 1;
            
            auto params = dataQuery.GetParamsBuilder()
                .AddParam("$whId")
                    .Int32(whId)
                    .Build()
                .AddParam("$distId")
                    .Int32(DistrictId)
                    .Build()
                .AddParam("$orderId")
                    .Int32(OrderIds[distPos])
                    .Build()
                .Build();

            auto processing = [this, whId, distPos, funcName](const NTable::TAsyncDataQueryResult& future) {
                NTable::TDataQueryResult result = future.GetValue();
                TStringStream stream;
                stream << "Failed to retrieve ORDER_LINE records [W_ID=" << whId << ", D_ID=" << DistrictId << ", O_ID=" << OrderIds[distPos] << "]";
                ThrowOnError(result, Log, funcName + stream.Str());

                TResultSetParser parser(result.GetResultSet(0));
                while (parser.TryNextRow()) {
                    OrderLineCollections[distPos].TotalAmount += parser.ColumnParser("OL_NUMBER").GetInt32();
                    OrderLineCollections[distPos].LineNumbers.push_back(parser.ColumnParser("OL_AMOUNT").GetOptionalDouble().GetOrElse(0.0));
                }
                if (OrderLineCollections[distPos].LineNumbers.empty()) {
                    throw yexception() << stream.Str();
                }
            };

            return Terminal.ExecuteAndProcessingDataQuery(dataQuery, std::move(params), Transaction, processing, TString(funcName));
        }
    );
}

NThreading::TFuture<void> TDeliveryProcedure::DeleteOrder(i32 whId, NTable::TSession session) {
    std::string funcName = __func__;

    TString query = R"(
        --!syntax_v1

        DECLARE $whId AS Int32;
        DECLARE $distId AS Int32;
        DECLARE $orderId AS Int32;

        DELETE FROM `new_order` 
        WHERE NO_O_ID = $orderId
          AND NO_D_ID = $distId
          AND NO_W_ID = $whId;
    )";

    return Terminal.ApplyByThreadPool(
        session.PrepareDataQuery(query),
        [this, whId, funcName](const TAsyncPrepareQueryResult& future){
            auto dataQuery = future.GetValue().GetQuery();

            if (!CheckDistrictId()) {
                return NThreading::MakeFuture();
            }
            auto distPos = DistrictId - 1;
            
            auto params = dataQuery.GetParamsBuilder()
                .AddParam("$whId")
                    .Int32(whId)
                    .Build()
                .AddParam("$distId")
                    .Int32(DistrictId)
                    .Build()
                .AddParam("$orderId")
                    .Int32(OrderIds[distPos])
                    .Build()
                .Build();

            auto processing = [this, whId, distPos, funcName](const NTable::TAsyncDataQueryResult& future) {
                NTable::TDataQueryResult result = future.GetValue();
                TStringStream stream;
                stream << "Failed to delete NEW_ORDER records [W_ID=" << whId << ", D_ID=" << DistrictId << ", O_ID=" << OrderIds[distPos] << "]";
                ThrowOnError(result, Log, funcName + stream.Str());
            };
            
            return Terminal.ExecuteAndProcessingDataQuery(dataQuery, std::move(params), Transaction, processing, TString(funcName));
        }
    );
}

NThreading::TFuture<void> TDeliveryProcedure::UpdateCarrierId(i32 whId, i32 carrierId, NTable::TSession session) {
    std::string funcName = __func__; 

    TString query = R"(--!syntax_v1
        DECLARE $rows AS List<Struct<O_CARRIER_ID:Int32, O_D_ID:Int32, O_ID:Int32, O_W_ID:Int32>>;
        UPSERT INTO `oorder` (O_CARRIER_ID, O_D_ID, O_ID, O_W_ID) 
        SELECT O_CARRIER_ID, O_D_ID, O_ID, O_W_ID FROM AS_TABLE( $rows );
    )";

    return Terminal.ApplyByThreadPool(
        session.PrepareDataQuery(query),
        [this, whId, carrierId, funcName](const TAsyncPrepareQueryResult& future){
            auto dataQuery = future.GetValue().GetQuery();
    
            TValueBuilder builder;
            builder.BeginList();
            for (i32 distId = 1; distId <= EWorkloadConstants::TPCC_DIST_PER_WH; ++distId) {
                auto distPos = distId - 1;
                if (OrderIds[distPos] == -1) {
                    continue;
                }
                
                builder.AddListItem().BeginStruct()
                    .AddMember("O_CARRIER_ID").Int32(carrierId)
                    .AddMember("O_D_ID").Int32(distId)
                    .AddMember("O_ID").Int32(OrderIds[distPos])
                    .AddMember("O_W_ID").Int32(whId)
                .EndStruct();
            }
            builder.EndList();

            TParamsBuilder params(std::move(dataQuery.GetParamsBuilder().AddParam("$rows", builder.Build())));

            auto processing = [this, whId, carrierId, funcName](const NTable::TAsyncDataQueryResult& future) {
                NTable::TDataQueryResult result = future.GetValue();
                TStringStream stream;
                stream << "Failed to update carrier id [W_ID=" << whId << ", CARRIER_ID=" << carrierId << "]";
                ThrowOnError(result, Log, funcName + stream.Str());
            };

            return Terminal.ExecuteAndProcessingDataQuery(dataQuery, std::move(params.Build()), Transaction, processing, TString(funcName));
        }
    );
}

NThreading::TFuture<void> TDeliveryProcedure::UpdateDeliveryDate(i32 whId, NTable::TSession session) {
    std::string funcName = __func__;

    TString query = R"(--!syntax_v1
        DECLARE $rows AS List<Struct<OL_DELIVERY_D:Timestamp, OL_D_ID:Int32, OL_NUMBER:Int32, OL_O_ID:Int32, OL_W_ID:Int32>>;
        UPSERT INTO `order_line` (OL_DELIVERY_D, OL_D_ID, OL_NUMBER, OL_O_ID, OL_W_ID) 
        SELECT OL_DELIVERY_D, OL_D_ID, OL_NUMBER, OL_O_ID, OL_W_ID FROM AS_TABLE( $rows );
    )";

    return Terminal.ApplyByThreadPool(
        session.PrepareDataQuery(query),
        [this, whId, funcName](const TAsyncPrepareQueryResult& future){
            auto dataQuery = future.GetValue().GetQuery();
            
            TValueBuilder builder;
            builder.BeginList();
            for (i32 distId = 1; distId <= EWorkloadConstants::TPCC_DIST_PER_WH; ++distId) {
                auto distPos = distId - 1;
                if (OrderIds[distPos] == -1) {
                    continue;
                }
                for (auto lineNimber: OrderLineCollections[distPos].LineNumbers) {
                    builder.AddListItem().BeginStruct()
                        .AddMember("OL_DELIVERY_D").Timestamp(Now())
                        .AddMember("OL_D_ID").Int32(distId)
                        .AddMember("OL_NUMBER").Int32(lineNimber)
                        .AddMember("OL_O_ID").Int32(OrderIds[distPos])
                        .AddMember("OL_W_ID").Int32(whId)
                    .EndStruct();
                }
            }
            builder.EndList();

            TParamsBuilder params(std::move(dataQuery.GetParamsBuilder().AddParam("$rows", builder.Build())));

            auto processing = [this, whId, funcName](const NTable::TAsyncDataQueryResult& future) {
                NTable::TDataQueryResult result = future.GetValue();
                TStringStream stream;
                stream << "Failed to update delivery date [W_ID=" << whId << "]";
                ThrowOnError(result, Log, funcName + stream.Str());
            };

            return Terminal.ExecuteAndProcessingDataQuery(dataQuery, std::move(params.Build()), Transaction, processing, TString(funcName));
        }
    );
}

NThreading::TFuture<void> TDeliveryProcedure::UpdateBalanceAndDelivery(i32 whId, NTable::TSession session) {
    std::string funcName = __func__;
    
    TString query = R"(--!syntax_v1
        DECLARE $rows AS List<Struct<C_BALANCE:Double, C_DELIVERY_CNT:Int32, C_D_ID:Int32, C_ID:Int32, C_W_ID:Int32>>;
        UPSERT INTO `customer` (C_BALANCE, C_DELIVERY_CNT, C_D_ID, C_ID, C_W_ID) 
        SELECT C_BALANCE, C_DELIVERY_CNT, C_D_ID, C_ID, C_W_ID FROM AS_TABLE( $rows );
    )";

    return Terminal.ApplyByThreadPool(
        session.PrepareDataQuery(query),
        [this, whId, funcName](const TAsyncPrepareQueryResult& future){
            auto dataQuery = future.GetValue().GetQuery();

            TValueBuilder builder;
            builder.BeginList();
            for (i32 distId = 1; distId <= EWorkloadConstants::TPCC_DIST_PER_WH; ++distId) {
                auto distPos = distId - 1;
                if (CustomerData[distPos].Id == -1) {
                    continue;
                }
                builder.AddListItem().BeginStruct()
                    .AddMember("C_BALANCE").Double(CustomerData[distPos].Balance + OrderLineCollections[distPos].TotalAmount)
                    .AddMember("C_DELIVERY_CNT").Int32(CustomerData[distPos].DeliveryCount + 1)
                    .AddMember("C_D_ID").Int32(distId)
                    .AddMember("C_ID").Int32(CustomerData[distPos].Id)
                    .AddMember("C_W_ID").Int32(whId)
                .EndStruct();
            }
            builder.EndList();

            TParamsBuilder params(std::move(dataQuery.GetParamsBuilder().AddParam("$rows", builder.Build())));

            auto processing = [this, whId, funcName](const NTable::TAsyncDataQueryResult& future) {
                NTable::TDataQueryResult result = future.GetValue();
                TStringStream stream;
                stream << "Failed to update balance [W_ID=" << whId << "]";
                ThrowOnError(result, Log, funcName + stream.Str());
            };

            return Terminal.ExecuteAndProcessingDataQuery(dataQuery, std::move(params.Build()), Transaction, processing, TString(funcName));
        }
    );
}
    
}
}
