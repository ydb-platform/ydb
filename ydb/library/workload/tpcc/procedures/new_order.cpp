#include "new_order.h"

#include <ydb/library/workload/tpcc/procedures/tables.h>

namespace NYdbWorkload {
namespace NTPCC {

TNewOrderProcedure::TNewOrderProcedure(std::shared_ptr<NTable::TTableClient> tableClient,
                                       TTerminal& terminal, TLog& log, bool debug, ui64 seed) 
    : IProcedure(EProcedureType::NewOrder, terminal, log, tableClient, debug, seed)
    , Stage(BEGIN)
{
}

NThreading::TFuture<void> TNewOrderProcedure::Run(std::shared_ptr<TProcedureResource> resource) {
    Resource = std::dynamic_pointer_cast<NewOrderResource>(std::move(resource));
    
    // After each stage, the next one will be called
    return RunStages();
}

NThreading::TFuture<void> TNewOrderProcedure::RunCurrentStage(TSession session) {
    ItemCount = Resource->ItemCount;

    NThreading::TFuture<void> future;
    switch (Stage) {
        case NewOrderStage::START_TRANSACTION:
            future = StartTransaction(session, DebugMode);
            break;
        case NewOrderStage::GET_CUSTOMER:
            future = Customer.GetCustomerById(Transaction, Log, Terminal, Resource->WarehouseId, 
                                              Resource->DistrictId, Resource->CustomerId, session);
            break;
        case NewOrderStage::GET_WAREHOUSE:
            future = Warehouse.GetWarehouse(Transaction, Log, Terminal, Resource->WarehouseId, session);
            break;
        case NewOrderStage::GET_DISTRICT:
            future = District.GetDistrict(Transaction, Log, Terminal, Resource->WarehouseId,
                                 Resource->DistrictId, session);
            break;
        case NewOrderStage::UPDATE_DISTRICT:
            future = UpdateDistrict(Resource->WarehouseId, Resource->DistrictId, session);
            break;
        case NewOrderStage::INSERT_OPEN_ORDER:
            future = TOorderData::InsertOpenOrder(Transaction, Log, Terminal, Resource->WarehouseId,
                                         Resource->DistrictId, Resource->CustomerId, District.NextOrderId,
                                         Resource->ItemCount, Resource->AllLocal, session);
            break;
        case NewOrderStage::INSERT_NEW_ORDER:
            future = TNewOrderData::InsertNewOrder(Transaction, Log, Terminal, Resource->WarehouseId,
                                          Resource->DistrictId, District.NextOrderId, session);
            break;
        case NewOrderStage::GET_ITEM_PRICES:
            future = GetItemPrices(Resource->ItemIds, session);
            break;
        case NewOrderStage::GET_STOCKS:
            future = GetStocks(Resource->ItemIds, Resource->SupplierWarehouseIds, 
                      Resource->OrderQuantities, session);
            break;
        case NewOrderStage::INSERT_ORDER_LINE:
            future = InsertOrderLine(session);
            break;
        case NewOrderStage::UPDATE_STOCK:
            future = UpdateStock(session);
            break;
        case NewOrderStage::END:
            future = CommitTransaction(DebugMode);
            break;
        default:
            return NThreading::MakeErrorFuture<void>(std::make_exception_ptr(yexception() << "Not implemented stage for NewOrder"));
    }

    return future;
}

TString TNewOrderProcedure::GetStageName() {
    TStringStream stream;
    stream << Stage << "(" << static_cast<i32>(Stage) << ")";
    return stream.Str();
}

bool TNewOrderProcedure::NotStarted() {
    return Stage == NewOrderStage::BEGIN;
}

bool TNewOrderProcedure::Finished() {
    return Stage == NewOrderStage::END;
}

void TNewOrderProcedure::NextStage() {    
    Y_ENSURE(Stage != NewOrderStage::END, "The procedure is finished, but there was an attempt to get the next stage");

    switch (Stage) {
        case BEGIN:
        case START_TRANSACTION:
        case GET_CUSTOMER:
        case GET_WAREHOUSE:
        case GET_DISTRICT:
        case UPDATE_DISTRICT:
        case INSERT_OPEN_ORDER:
        case INSERT_NEW_ORDER:
        case INSERT_ORDER_LINE:
        case UPDATE_STOCK:
        case END:
            Stage = static_cast<NewOrderStage>(static_cast<int>(Stage) + 1);
            break;
        default:
            ++Position;
            if (Position == ItemCount) {
                Position = 0;
                Stage = static_cast<NewOrderStage>(static_cast<int>(Stage) + 1);
            }
    }
}

void TNewOrderProcedure::StartOver() {
    Stage = NewOrderStage::BEGIN;

    Position = 0;
    ItemPrices.clear();
    Stocks.clear();
}

NThreading::TFuture<void> TNewOrderProcedure::GetItemPrices(const std::vector<i32>& itemIds, NTable::TSession& session) {
    if (ItemPrices.empty()) {
        ItemPrices.assign(ItemCount, 0.0);
    }
    
    NThreading::TFuture<double> priceFuture = TItemData::GetItemPrice(Transaction, Log, Terminal, itemIds[Position], session);

    return priceFuture.Apply(
        [this](const NThreading::TFuture<double>& future) {
            ItemPrices[Position] = future.GetValue();
        }
    );
}

NThreading::TFuture<void> TNewOrderProcedure::GetStocks(const std::vector<i32>& itemIds, const std::vector<i32>& supplierWarehouseIds,
                                   const std::vector<i32>& orderQuantities, NTable::TSession& session) {
    if (Stocks.empty()) {
        Stocks.assign(ItemCount, TStockData());
    }

    i32 itemId = itemIds[Position];
    i32 whId = supplierWarehouseIds[Position];
    i32 quantity = orderQuantities[Position];

    auto stockFuture = Stocks[Position].GetStock(Transaction, Log, Terminal, whId, itemId, session);

    return stockFuture.Apply(
        [this, quantity](const NThreading::TFuture<void>& future) {
            future.GetValue();

            if (Stocks[Position].Quantity - quantity >= 10) {
                Stocks[Position].Quantity -= quantity;
            } else  {
                Stocks[Position].Quantity += 91 - quantity;
            }
        }
    );
}

NThreading::TFuture<void> TNewOrderProcedure::UpdateDistrict(i32 whId, i32 distId, NTable::TSession& session) {
    std::string funcName = __func__;
    
    TString query = R"(
        --!syntax_v1

        DECLARE $whId AS Int32;
        DECLARE $distId AS Int32;
        DECLARE $distNextOId AS Int32;

        UPSERT INTO `district` (D_W_ID, D_ID, D_NEXT_O_ID)
        VALUES ($whId, $distId, $distNextOId);
    )";

    return Terminal.ApplyByThreadPool(
        session.PrepareDataQuery(query),
        [this, whId, distId, funcName](const TAsyncPrepareQueryResult& future){
            auto dataQuery = future.GetValue().GetQuery();

            auto distNextOId = District.NextOrderId + 1;
            
            auto params = dataQuery.GetParamsBuilder()
                .AddParam("$whId")
                    .Int32(whId)
                    .Build()
                .AddParam("$distId")
                    .Int32(distId)
                    .Build()
                .AddParam("$distNextOId")
                    .Int32(distNextOId)
                    .Build()
                .Build();

            auto processing = [this, whId, distId, distNextOId, funcName](const NTable::TAsyncDataQueryResult& future) {
                NTable::TDataQueryResult result = future.GetValue();
                TStringStream stream;
                stream << "Failed to update DISTRICT [W_ID=" << whId << ", D_ID=" << distId << ", O_ID=" << distNextOId << "]";
                ThrowOnError(result, Log, funcName + stream.Str());
            };

            return Terminal.ExecuteAndProcessingDataQuery(dataQuery, std::move(params), Transaction, processing, TString(funcName));
        }
    );
}

NThreading::TFuture<void> TNewOrderProcedure::InsertOrderLine(NTable::TSession& session) {
    std::string funcName = __func__;
    
    TString query = R"(--!syntax_v1
        DECLARE $rows AS List<Struct<OL_AMOUNT:Double, OL_DELIVERY_D:Timestamp, OL_DIST_INFO:Utf8, 
                                     OL_D_ID:Int32, OL_I_ID:Int32, OL_NUMBER:Int32, OL_O_ID:Int32, 
                                     OL_SUPPLY_W_ID:Int32, OL_QUANTITY:Double, OL_W_ID:Int32>>;

        UPSERT INTO `order_line` (OL_AMOUNT, OL_DELIVERY_D, OL_DIST_INFO, OL_D_ID, OL_I_ID, 
                                  OL_NUMBER, OL_O_ID, OL_QUANTITY, OL_SUPPLY_W_ID, OL_W_ID) 
        SELECT OL_AMOUNT, OL_DELIVERY_D, OL_DIST_INFO, OL_D_ID, OL_I_ID,
               OL_NUMBER, OL_O_ID, OL_QUANTITY, OL_SUPPLY_W_ID, OL_W_ID
        FROM AS_TABLE( $rows );
    )";

    return Terminal.ApplyByThreadPool(
        session.PrepareDataQuery(query),
        [this, funcName](const TAsyncPrepareQueryResult& future){
            auto dataQuery = future.GetValue().GetQuery();
    
            TValueBuilder builder;
            builder.BeginList();
            for (i32 olNumber = 1; olNumber <= Resource->ItemCount; ++olNumber) {
                i32 olSupplyWhId = Resource->SupplierWarehouseIds[olNumber - 1];
                i32 olItemId = Resource->ItemIds[olNumber - 1];
                i32 olQuantity = Resource->OrderQuantities[olNumber - 1];
                double olAmount = olQuantity * ItemPrices[olNumber - 1];
                TString distInfo = GetDistInfo(Resource->DistrictId, Stocks[olNumber - 1]);
                
                builder.AddListItem().BeginStruct()
                    .AddMember("OL_AMOUNT").Double(olAmount)
                    .AddMember("OL_DELIVERY_D").Timestamp(Now())
                    .AddMember("OL_DIST_INFO").Utf8(distInfo)
                    .AddMember("OL_D_ID").Int32(Resource->DistrictId)
                    .AddMember("OL_I_ID").Int32(olItemId)
                    .AddMember("OL_NUMBER").Int32(olNumber)
                    .AddMember("OL_O_ID").Int32(District.NextOrderId)
                    .AddMember("OL_QUANTITY").Double(olQuantity)
                    .AddMember("OL_SUPPLY_W_ID").Int32(olSupplyWhId)
                    .AddMember("OL_W_ID").Int32(Resource->WarehouseId)
                .EndStruct();
            }
            builder.EndList();

            TParamsBuilder params(std::move(dataQuery.GetParamsBuilder().AddParam("$rows", builder.Build())));

            auto processing = [this, funcName](const NTable::TAsyncDataQueryResult& future) {
                auto result = future.GetValue();
                TStringStream stream;
                stream << "BulkUpsert to ORDER_LINE failed with issues: " << result.GetIssues().ToString();
                ThrowOnError(result, Log, funcName + stream.Str());
            };

            return Terminal.ExecuteAndProcessingDataQuery(dataQuery, std::move(params.Build()), Transaction, processing, TString(funcName));
        }
    );
}

NThreading::TFuture<void> TNewOrderProcedure::UpdateStock(NTable::TSession& session) {
    std::string funcName = __func__;
    
    TString query = R"(--!syntax_v1
        DECLARE $rows AS List<Struct<S_I_ID:Int32, S_ORDER_CNT:Int32, S_QUANTITY:Int32,
                                     S_REMOTE_CNT:Int32, S_W_ID:Int32, S_YTD:Double>>;
        UPSERT INTO `stock` (S_I_ID, S_ORDER_CNT, S_QUANTITY, S_REMOTE_CNT, S_W_ID, S_YTD) 
        SELECT S_I_ID, S_ORDER_CNT, S_QUANTITY, S_REMOTE_CNT, S_W_ID, S_YTD from AS_TABLE( $rows );
    )";

    return Terminal.ApplyByThreadPool(
        session.PrepareDataQuery(query),
        [this, funcName](const TAsyncPrepareQueryResult& future){
            auto dataQuery = future.GetValue().GetQuery();
    
            TValueBuilder builder;
            builder.BeginList();
            for (i32 olNumber = 1; olNumber <= Resource->ItemCount; ++olNumber) {
                i32 olSupplyWhId = Resource->SupplierWarehouseIds[olNumber - 1];
                i32 olQuantity = Resource->OrderQuantities[olNumber - 1];
                TStockData& stock = Stocks[olNumber - 1];
                TString distInfo = GetDistInfo(Resource->DistrictId, Stocks[olNumber - 1]);

                builder.AddListItem().BeginStruct()
                    .AddMember("S_I_ID").Int32(Resource->DistrictId)
                    .AddMember("S_ORDER_CNT").Int32(stock.OrderCount + 1)
                    .AddMember("S_QUANTITY").Int32(olQuantity)
                    .AddMember("S_REMOTE_CNT").Int32(stock.RemoteCount + (olSupplyWhId == Resource->WarehouseId))
                    .AddMember("S_W_ID").Int32(Resource->WarehouseId)
                    .AddMember("S_YTD").Double(stock.Ytd + olQuantity)
                .EndStruct();
            }
            builder.EndList();
            
            TParamsBuilder params(std::move(dataQuery.GetParamsBuilder().AddParam("$rows", builder.Build())));

            auto processing = [this, funcName](const NTable::TAsyncDataQueryResult& future) {
                auto result = future.GetValue();
                TStringStream stream;
                stream << "BulkUpsert to STOCK failed with issues: " << result.GetIssues().ToString();
                ThrowOnError(result, Log, funcName + stream.Str());
            };

            return Terminal.ExecuteAndProcessingDataQuery(dataQuery, std::move(params.Build()), Transaction, processing, TString(funcName));
        }
    );
}

TString TNewOrderProcedure::GetDistInfo(i32 d_id, const TStockData& stock) {
    switch (d_id) {
        case 1:
            return stock.Dist01;
        case 2:
            return stock.Dist02;
        case 3:
            return stock.Dist03;
        case 4:
            return stock.Dist04;
        case 5:
            return stock.Dist05;
        case 6:
            return stock.Dist06;
        case 7:
            return stock.Dist07;
        case 8:
            return stock.Dist08;
        case 9:
            return stock.Dist09;
        case 10:
            return stock.Dist10;
        default:
            return "";
    }
}


}
}
