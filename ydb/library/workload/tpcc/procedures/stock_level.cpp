#include "stock_level.h"

#include <util/string/cast.h>


namespace NYdbWorkload {
namespace NTPCC {

TStockLevelProcedure::TStockLevelProcedure(std::shared_ptr<NTable::TTableClient> tableClient,
                                           TTerminal& terminal, TLog& log, bool debug, ui64 seed) 
    : IProcedure(EProcedureType::StockLevel, terminal, log, tableClient, debug, seed)
    , Stage(BEGIN)
{
}

NThreading::TFuture<void> TStockLevelProcedure::Run(std::shared_ptr<TProcedureResource> resource) {
    Resource = std::dynamic_pointer_cast<StockLevelResource>(std::move(resource));
    
    // After each stage, the next one will be called
    return RunStages();
}

NThreading::TFuture<void> TStockLevelProcedure::RunCurrentStage(TSession session) {
    NThreading::TFuture<void> future;

    switch (Stage) {
        case StockLevelStage::START_TRANSACTION:
            future = StartTransaction(session, DebugMode);
            break;
        case StockLevelStage::GET_DISTRICT:
            future = District.GetDistrict(Transaction, Log, Terminal, Resource->WarehouseId, Resource->DistrictId, session);
            break;
        case StockLevelStage::GET_STOCK_COUNT:
            future = GetStockCount(Resource->WarehouseId, Resource->DistrictId, District.NextOrderId, Resource->Threshold, session);
            break;
        case StockLevelStage::END:
            future = CommitTransaction(DebugMode);
            break;
        default:
            return NThreading::MakeErrorFuture<void>(std::make_exception_ptr(yexception() << "Not implemented stage for StockLevel"));
    }

    return future;
}

bool TStockLevelProcedure::NotStarted() {
    return Stage == StockLevelStage::BEGIN;
}

bool TStockLevelProcedure::Finished() {
    return Stage == StockLevelStage::END;
}

void TStockLevelProcedure::NextStage() {    
    Y_ENSURE(Stage != StockLevelStage::END, "The procedure is finished, but there was an attempt to get the next stage");

    Stage = static_cast<StockLevelStage>(static_cast<int>(Stage) + 1);
}

TString TStockLevelProcedure::GetStageName() {
    TStringStream stream;
    stream << Stage << "(" << static_cast<i32>(Stage) << ")";
    return stream.Str();
}

void TStockLevelProcedure::StartOver() {
    Stage = StockLevelStage::BEGIN;
}

NThreading::TFuture<void> TStockLevelProcedure::GetStockCount(i32 whId, i32 distId, i32 orderId, i32 threshold,
                                                              NTable::TSession& session) {
    std::string funcName = __func__;

    TString query = R"(
        --!syntax_v1

        DECLARE $whId AS Int32;
        DECLARE $distId AS Int32;
        DECLARE $orderLowerId AS Int32;
        DECLARE $orderUpperId AS Int32;
        DECLARE $quantity AS Int32;

        SELECT COUNT(DISTINCT (s.S_I_ID)) AS STOCK_COUNT
         FROM `order_line` as ol INNER JOIN `stock` as s ON s.S_I_ID = ol.OL_I_ID
         WHERE ol.OL_W_ID = $whId
         AND ol.OL_D_ID = $distId
         AND ol.OL_O_ID < $orderUpperId
         AND ol.OL_O_ID >= $orderLowerId
         AND s.S_W_ID = $whId
         AND s.S_QUANTITY < $quantity;
    )";

    return Terminal.ApplyByThreadPool(
        session.PrepareDataQuery(query),
        [this, whId, distId, orderId, threshold, funcName](const TAsyncPrepareQueryResult& future){
            auto dataQuery = future.GetValue().GetQuery();
            
            auto params = dataQuery.GetParamsBuilder()
                .AddParam("$whId")
                    .Int32(whId)
                    .Build()
                .AddParam("$distId")
                    .Int32(distId)
                    .Build()
                .AddParam("$orderLowerId")
                    .Int32(orderId - 20)
                    .Build()
                .AddParam("$orderUpperId")
                    .Int32(orderId)
                    .Build()
                .AddParam("$quantity")
                    .Int32(threshold)
                    .Build()
                .Build();

            auto processing = [this, whId, distId, orderId, funcName](const NTable::TAsyncDataQueryResult& future) {
                NTable::TDataQueryResult result = future.GetValue();
                TStringStream stream;
                stream << "Failed to get STOCK count [W_ID=" << whId << ", D_ID=" << distId << ", O_ID=" << orderId << "]";
                ThrowOnError(result, Log, funcName + stream.Str());

                TResultSetParser parser(result.GetResultSet(0));
                if (!parser.TryNextRow()) {
                    throw yexception() << stream.Str();
                }
                StockCount = parser.ColumnParser("STOCK_COUNT").GetUint64();
            };

            return Terminal.ExecuteAndProcessingDataQuery(dataQuery, std::move(params), Transaction, processing, TString(funcName));
        }
    );
}

}
}
