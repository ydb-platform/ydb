#include "tables.h"

#include <ydb/library/workload/tpcc/util.h>
#include <ydb/library/workload/tpcc/error.h>

#include <ydb/public/sdk/cpp/client/ydb_types/exceptions/exceptions.h>


namespace NYdbWorkload {
namespace NTPCC {

NThreading::TFuture<void> TWarehouseData::GetWarehouse(TMaybe<TTransaction>& transaction, TLog& log, TTerminal& terminal,
                                                       i32 whId, TSession& session) {
    std::string funcName = __func__;

    TString query = R"(
        --!syntax_v1

        DECLARE $whId AS Int32;

        SELECT W_YTD, W_TAX, W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP
        FROM `warehouse`
        WHERE W_ID = $whId;
    )";

    return terminal.ApplyByThreadPool(
        session.PrepareDataQuery(query),
        [this, &transaction, &log, &terminal, whId, funcName](const TAsyncPrepareQueryResult& future) {
            auto dataQuery = future.GetValue().GetQuery();
    
            auto params = dataQuery.GetParamsBuilder()
                .AddParam("$whId")
                    .Int32(whId)
                    .Build()
                .Build();

            auto processing = [this, &log, whId, funcName](const TAsyncDataQueryResult& future) {
                TDataQueryResult result = future.GetValue();
                TStringStream stream;
                stream << "Failed to retrieve WAREHOUSE records [W_ID=" << whId << "]";
                ThrowOnError(result, log, funcName + stream.Str());


                TResultSetParser parser(result.GetResultSet(0));
                if (!parser.TryNextRow()) {
                    throw yexception() << stream.Str();
                }
                Id = whId;
                Ytd = parser.ColumnParser("W_YTD").GetOptionalDouble().GetOrElse(0.0);
                Tax = parser.ColumnParser("W_TAX").GetOptionalDouble().GetOrElse(0.0);
                Name = parser.ColumnParser("W_NAME").GetOptionalUtf8().GetOrElse("");
                Street1 = parser.ColumnParser("W_STREET_1").GetOptionalUtf8().GetOrElse("");
                Street2 = parser.ColumnParser("W_STREET_2").GetOptionalUtf8().GetOrElse("");
                City = parser.ColumnParser("W_CITY").GetOptionalUtf8().GetOrElse("");
                State = parser.ColumnParser("W_STATE").GetOptionalUtf8().GetOrElse("");
                Zip = parser.ColumnParser("W_ZIP").GetOptionalUtf8().GetOrElse("");
            };

            return terminal.ExecuteAndProcessingDataQuery(dataQuery, std::move(params), transaction, processing, TString(funcName));
        }
    );
}


NThreading::TFuture<void> TDistrictData::GetDistrict(TMaybe<TTransaction>& transaction, TLog& log, TTerminal& terminal,
                                                     i32 whId, i32 distId, TSession& session) {
    std::string funcName = __func__;

    TString query = R"(
        --!syntax_v1

        DECLARE $whId AS Int32;
        DECLARE $distId AS Int32;

        SELECT D_W_ID, D_ID, D_YTD, D_TAX, D_NEXT_O_ID, D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP
        FROM `district`
        WHERE D_W_ID = $whId AND D_ID = $distId;
    )";

    return terminal.ApplyByThreadPool(
        session.PrepareDataQuery(query),
        [this, &transaction, &log, &terminal, whId, distId, funcName](const TAsyncPrepareQueryResult& future) {
            auto dataQuery = future.GetValue().GetQuery();
    
            auto params = dataQuery.GetParamsBuilder()
                .AddParam("$whId")
                    .Int32(whId)
                    .Build()
                .AddParam("$distId")
                    .Int32(distId)
                    .Build()
                .Build();

            auto processing = [this, &log, whId, distId, funcName](const TAsyncDataQueryResult& future) {
                TDataQueryResult result = future.GetValue();
                TStringStream stream;
                stream << "Failed to retrieve DISTRICT records [W_ID=" << whId << ", D_ID=" << distId << "]";
                ThrowOnError(result, log, funcName + stream.Str());

                TResultSetParser parser(result.GetResultSet(0));
                if (!parser.TryNextRow()) {
                    throw yexception() << stream.Str();
                }
                Id = distId;
                WhId = whId;
                Ytd = parser.ColumnParser("D_YTD").GetOptionalDouble().GetOrElse(0.0);
                Tax = parser.ColumnParser("D_TAX").GetOptionalDouble().GetOrElse(0.0);
                NextOrderId = parser.ColumnParser("D_NEXT_O_ID").GetOptionalInt32().GetOrElse(0);
                Name = parser.ColumnParser("D_NAME").GetOptionalUtf8().GetOrElse("");
                Street1 = parser.ColumnParser("D_STREET_1").GetOptionalUtf8().GetOrElse("");
                Street2 = parser.ColumnParser("D_STREET_2").GetOptionalUtf8().GetOrElse("");
                City = parser.ColumnParser("D_CITY").GetOptionalUtf8().GetOrElse("");
                State = parser.ColumnParser("D_STATE").GetOptionalUtf8().GetOrElse("");
                Zip = parser.ColumnParser("D_ZIP").GetOptionalUtf8().GetOrElse("");
            };

            return terminal.ExecuteAndProcessingDataQuery(dataQuery, std::move(params), transaction, processing, TString(funcName));
        }
    );
}


NThreading::TFuture<double> TItemData::GetItemPrice(TMaybe<TTransaction>& transaction, TLog& log, TTerminal& terminal,
                                                    i32 itemId, TSession& session) {
    std::string funcName = __func__;

    TString query = R"(
        --!syntax_v1

        DECLARE $itemId AS Int32;

        SELECT I_PRICE
        FROM `item`
        WHERE I_ID = $itemId;
    )";

    return terminal.ApplyByThreadPool(
        session.PrepareDataQuery(query),
        [&transaction, &log, &terminal, itemId, funcName](const TAsyncPrepareQueryResult& future) {
            auto dataQuery = future.GetValue().GetQuery();
    
            auto params = dataQuery.GetParamsBuilder()
                .AddParam("$itemId")
                    .Int32(itemId)
                    .Build()
                .Build();

            auto processing = [&log, itemId, funcName](const TAsyncDataQueryResult& future) {
                TDataQueryResult result = future.GetValue();
                TStringStream stream;
                stream << "Failed to retrieve ITEM records [I_ID=" << itemId << "]";
                ThrowOnError(result, log, funcName + stream.Str());

                TResultSetParser parser(result.GetResultSet(0));
                if (!parser.TryNextRow()) {
                    if (itemId == EWorkloadConstants::TPCC_INVALID_ITEM_ID) {
                        // This is an expected error: this is an expected new order rollback
                        throw yexception() << "EXPECTED new order rollback: I_ID=" << itemId << " not found!";
                    } else {
                        throw yexception() << stream.Str();
                    }
                }

                try {
                    return parser.ColumnParser("I_PRICE").GetOptionalDouble().GetOrElse(0.0);
                } catch (const std::exception& ex) {
                    throw yexception() << stream.Str() << ": " << ex.what();
                } catch (...) {
                    throw yexception() << stream.Str();
                }
                return 0.0;
            };

            return terminal.ExecuteAndProcessingDataQuery(dataQuery, std::move(params), transaction, processing, TString(funcName));
        }
    );
}


NThreading::TFuture<void> TCustomerData::GetCustomerDataById(TMaybe<TTransaction>& transaction, TLog& log, TTerminal& terminal,
                                                             i32 whId, i32 distId, i32 custId, TSession& session) {
    std::string funcName = __func__;

    TString query = R"(
        --!syntax_v1

        DECLARE $whId AS Int32;
        DECLARE $distId AS Int32;
        DECLARE $custId AS Int32;

        SELECT C_DATA
          FROM `customer`
         WHERE C_W_ID = $whId
           AND C_D_ID = $distId
           AND C_ID = $custId;
    )";

    return terminal.ApplyByThreadPool(
        session.PrepareDataQuery(query),
        [this, &transaction, &log, &terminal, whId, distId, custId, funcName](const TAsyncPrepareQueryResult& future) {
            auto dataQuery = future.GetValue().GetQuery();
    
            auto params = dataQuery.GetParamsBuilder()
                .AddParam("$whId")
                    .Int32(whId)
                    .Build()
                .AddParam("$distId")
                    .Int32(distId)
                    .Build()
                .AddParam("$custId")
                    .Int32(custId)
                    .Build()
                .Build();

            auto processing = [this, &log, whId, distId, custId, funcName](const TAsyncDataQueryResult& future) {
                TDataQueryResult result = future.GetValue();
                TStringStream stream;
                stream << "Failed to retrieve CUSTOMER records [W_ID=" << whId << ", D_ID=" << distId << ", C_ID=" << custId << "]";
                ThrowOnError(result, log, funcName + stream.Str());

                TResultSetParser parser(result.GetResultSet(0));
                if (!parser.TryNextRow()) {
                    throw yexception() << stream.Str();
                }

                try {
                    Data = parser.ColumnParser("C_DATA").GetOptionalUtf8().GetOrElse("");
                } catch (const std::exception& ex) {
                    throw yexception() << stream.Str() << ": " << ex.what();
                } catch (...) {
                    throw yexception() << stream.Str();
                }
            };

            return terminal.ExecuteAndProcessingDataQuery(dataQuery, std::move(params), transaction, processing, TString(funcName));
        }
    );
}


NThreading::TFuture<void> TCustomerData::GetCustomerById(TMaybe<TTransaction>& transaction, TLog& log, TTerminal& terminal,
                                                         i32 whId, i32 distId, i32 custId, TSession& session) {
    std::string funcName = __func__;

    TString query = R"(
        --!syntax_v1

        DECLARE $whId AS Int32;
        DECLARE $distId AS Int32;
        DECLARE $custId AS Int32;

        SELECT C_DISCOUNT, C_CREDIT, C_LAST, C_FIRST, C_CREDIT_LIM,
               C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DELIVERY_CNT, C_STREET_1, C_STREET_2,
               C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_MIDDLE, C_DATA
          FROM `customer`
         WHERE C_W_ID = $whId
           AND C_D_ID = $distId
           AND C_ID = $custId;
    )";

    return terminal.ApplyByThreadPool(
        session.PrepareDataQuery(query),
        [this, &transaction, &log, &terminal, whId, distId, custId, funcName](const TAsyncPrepareQueryResult& future) {
            auto dataQuery = future.GetValue().GetQuery();
    
            auto params = dataQuery.GetParamsBuilder()
                .AddParam("$whId")
                    .Int32(whId)
                    .Build()
                .AddParam("$distId")
                    .Int32(distId)
                    .Build()
                .AddParam("$custId")
                    .Int32(custId)
                    .Build()
                .Build();

            auto processing = [this, &log, whId, distId, custId, funcName](const TAsyncDataQueryResult& future) {
                TDataQueryResult result = future.GetValue();
                TStringStream stream;
                stream << "Failed to retrieve CUSTOMER records [W_ID=" << whId << ", D_ID=" << distId << ", C_ID=" << custId << "]";
                ThrowOnError(result, log, funcName + stream.Str());

                TResultSetParser parser(result.GetResultSet(0));
                if (!parser.TryNextRow()) {
                    throw yexception() << stream.Str();
                }

                Id = custId;
                DistId = distId;
                WhId = whId;
                try {
                    PaymentCount = parser.ColumnParser("C_PAYMENT_CNT").GetOptionalInt32().GetOrElse(0);
                    DeliveryCount = parser.ColumnParser("C_DELIVERY_CNT").GetOptionalInt32().GetOrElse(0);
                    Since = parser.ColumnParser("C_SINCE").GetOptionalTimestamp().GetOrElse(TInstant::Zero());
                    Discount = parser.ColumnParser("C_DISCOUNT").GetOptionalDouble().GetOrElse(0.0);
                    CreditLimit = parser.ColumnParser("C_CREDIT_LIM").GetOptionalDouble().GetOrElse(0.0);
                    Balance = parser.ColumnParser("C_BALANCE").GetOptionalDouble().GetOrElse(0.0);
                    YtdPayment = parser.ColumnParser("C_YTD_PAYMENT").GetOptionalDouble().GetOrElse(0.0);
                    Credit = parser.ColumnParser("C_CREDIT").GetOptionalUtf8().GetOrElse("");
                    Last = parser.ColumnParser("C_LAST").GetOptionalUtf8().GetOrElse("");
                    First = parser.ColumnParser("C_FIRST").GetOptionalUtf8().GetOrElse("");
                    Street1 = parser.ColumnParser("C_STREET_1").GetOptionalUtf8().GetOrElse("");
                    Street2 = parser.ColumnParser("C_STREET_1").GetOptionalUtf8().GetOrElse("");
                    City = parser.ColumnParser("C_CITY").GetOptionalUtf8().GetOrElse("");
                    State = parser.ColumnParser("C_STATE").GetOptionalUtf8().GetOrElse("");
                    Zip = parser.ColumnParser("C_ZIP").GetOptionalUtf8().GetOrElse("");
                    Phone = parser.ColumnParser("C_PHONE").GetOptionalUtf8().GetOrElse("");
                    Middle = parser.ColumnParser("C_MIDDLE").GetOptionalUtf8().GetOrElse("");
                    Data = parser.ColumnParser("C_DATA").GetOptionalUtf8().GetOrElse("");
                } catch (const std::exception& ex) {
                    throw yexception() << stream.Str() << ": " << ex.what();
                } catch (...) {
                    throw yexception() << stream.Str();
                }
            };

            return terminal.ExecuteAndProcessingDataQuery(dataQuery, std::move(params), transaction, processing, TString(funcName));
        }
    );
}

NThreading::TFuture<void> TCustomerData::GetCustomerByName(TMaybe<TTransaction>& transaction, TLog& log, TTerminal& terminal,
                                                           i32 whId, i32 distId, const TString& lastName, TSession& session) {
    std::string funcName = __func__;

    TString query = R"(
        --!syntax_v1

        DECLARE $whId AS Int32;
        DECLARE $distId AS Int32;
        DECLARE $lastName AS Utf8;

        SELECT C_W_ID, C_D_ID, C_LAST, C_ID, C_DISCOUNT, C_CREDIT, C_FIRST, C_CREDIT_LIM,
               C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DELIVERY_CNT, C_STREET_1, C_STREET_2,
               C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_MIDDLE, C_DATA
          FROM `customer` VIEW idx_customer_name AS idx
         WHERE idx.C_W_ID = $whId
           AND idx.C_D_ID = $distId
           AND idx.C_LAST = $lastName
         ORDER BY idx.C_FIRST;
    )";

    return terminal.ApplyByThreadPool(
        session.PrepareDataQuery(query),
        [this, &transaction, &log, &terminal, whId, distId, lastName, funcName](const TAsyncPrepareQueryResult& future) {
            auto dataQuery = future.GetValue().GetQuery();
    
            auto params = dataQuery.GetParamsBuilder()
                .AddParam("$whId")
                    .Int32(whId)
                    .Build()
                .AddParam("$distId")
                    .Int32(distId)
                    .Build()
                .AddParam("$lastName")
                    .Utf8(lastName)
                    .Build()
                .Build();

            auto processing = [this, &log, whId, distId, lastName, funcName](const TAsyncDataQueryResult& future) {
                TDataQueryResult result = future.GetValue();
                TStringStream stream;
                stream << "Failed to retrieve CUSTOMER records [W_ID=" << whId << ", D_ID=" << distId << ", LAST_NAME=" << lastName << "]";
                ThrowOnError(result, log, funcName + stream.Str());

                std::vector<TCustomerData> customers;
                TResultSetParser parser(result.GetResultSet(0));
                while (parser.TryNextRow()) {
                    TCustomerData cust;
                    cust.Id = parser.ColumnParser("C_ID").GetInt32();
                    cust.DistId = whId;
                    cust.WhId = distId;
                    cust.PaymentCount = parser.ColumnParser("C_PAYMENT_CNT").GetOptionalInt32().GetOrElse(0);
                    cust.DeliveryCount = parser.ColumnParser("C_DELIVERY_CNT").GetOptionalInt32().GetOrElse(0);
                    cust.Since = parser.ColumnParser("C_SINCE").GetOptionalTimestamp().GetOrElse(TInstant::Zero());
                    cust.Discount = parser.ColumnParser("C_DISCOUNT").GetOptionalDouble().GetOrElse(0.0);
                    cust.CreditLimit = parser.ColumnParser("C_CREDIT_LIM").GetOptionalDouble().GetOrElse(0.0);
                    cust.Balance = parser.ColumnParser("C_BALANCE").GetOptionalDouble().GetOrElse(0.0);
                    cust.YtdPayment = parser.ColumnParser("C_YTD_PAYMENT").GetOptionalDouble().GetOrElse(0.0);
                    cust.Credit = parser.ColumnParser("C_CREDIT").GetOptionalUtf8().GetOrElse("");
                    cust.Last = lastName;
                    cust.First = parser.ColumnParser("C_FIRST").GetOptionalUtf8().GetOrElse("");
                    cust.Street1 = parser.ColumnParser("C_STREET_1").GetOptionalUtf8().GetOrElse("");
                    cust.Street2 = parser.ColumnParser("C_STREET_1").GetOptionalUtf8().GetOrElse("");
                    cust.City = parser.ColumnParser("C_CITY").GetOptionalUtf8().GetOrElse("");
                    cust.State = parser.ColumnParser("C_STATE").GetOptionalUtf8().GetOrElse("");
                    cust.Zip = parser.ColumnParser("C_ZIP").GetOptionalUtf8().GetOrElse("");
                    cust.Phone = parser.ColumnParser("C_PHONE").GetOptionalUtf8().GetOrElse("");
                    cust.Middle = parser.ColumnParser("C_MIDDLE").GetOptionalUtf8().GetOrElse("");
                    cust.Data = parser.ColumnParser("C_DATA").GetOptionalUtf8().GetOrElse("");
                    
                    customers.emplace_back(std::move(cust));
                }

                if (customers.empty()) {
                    throw yexception() << "CUSTOMER not found [W_ID=" << whId << ", D_ID=" << distId << ", LAST_NAME=" << lastName << "]";
                }
                
                i32 index = customers.size() / 2;
                if (customers.size() % 2 == 0) {
                    --index;
                }

                *this = customers[index];
            };

            return terminal.ExecuteAndProcessingDataQuery(dataQuery, std::move(params), transaction, processing, TString(funcName));
        }
    );
}

NThreading::TFuture<void> TStockData::GetStock(TMaybe<TTransaction>& transaction, TLog& log, TTerminal& terminal,
                                               i32 whId, i32 itemId, TSession& session) {
    std::string funcName = __func__;

    TString query = R"(
        --!syntax_v1

        DECLARE $itemId AS Int32;
        DECLARE $whId AS Int32;

        SELECT S_QUANTITY, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DATA,
               S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05,
               S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10
          FROM `stock`
         WHERE S_I_ID = $itemId
           AND S_W_ID = $whId;
    )";

    return terminal.ApplyByThreadPool(
        session.PrepareDataQuery(query),
        [this, &transaction, &log, &terminal, whId, itemId, funcName](const TAsyncPrepareQueryResult& future) {
            auto dataQuery = future.GetValue().GetQuery();

            auto params = dataQuery.GetParamsBuilder()
                .AddParam("$itemId")
                    .Int32(itemId)
                    .Build()
                .AddParam("$whId")
                    .Int32(whId)
                    .Build()
                .Build();

            auto processing = [this, &log, whId, itemId, funcName](const TAsyncDataQueryResult& future) {
                TDataQueryResult result = future.GetValue();

                TStringStream stream;
                stream << "Failed to retrieve STOCK records [W_ID=" << whId << ", I_ID=" << itemId << "]";
                ThrowOnError(result, log, funcName + stream.Str());
                
                TResultSetParser parser(result.GetResultSet(0));
                if (!parser.TryNextRow()) {
                    throw yexception() << stream.Str();
                }

                WhId = whId;
                ItemId = itemId;
                try {
                    Quantity = parser.ColumnParser("S_QUANTITY").GetOptionalInt32().GetOrElse(0);
                    Ytd = parser.ColumnParser("S_YTD").GetOptionalDouble().GetOrElse(0.0);
                    OrderCount = parser.ColumnParser("S_ORDER_CNT").GetOptionalInt32().GetOrElse(0);
                    RemoteCount = parser.ColumnParser("S_REMOTE_CNT").GetOptionalInt32().GetOrElse(0);
                    Data = parser.ColumnParser("S_DATA").GetOptionalUtf8().GetOrElse("");
                    Dist01 = parser.ColumnParser("S_DIST_01").GetOptionalUtf8().GetOrElse("");
                    Dist02 = parser.ColumnParser("S_DIST_02").GetOptionalUtf8().GetOrElse("");
                    Dist03 = parser.ColumnParser("S_DIST_03").GetOptionalUtf8().GetOrElse("");
                    Dist04 = parser.ColumnParser("S_DIST_04").GetOptionalUtf8().GetOrElse("");
                    Dist05 = parser.ColumnParser("S_DIST_05").GetOptionalUtf8().GetOrElse("");
                    Dist06 = parser.ColumnParser("S_DIST_06").GetOptionalUtf8().GetOrElse("");
                    Dist07 = parser.ColumnParser("S_DIST_07").GetOptionalUtf8().GetOrElse("");
                    Dist08 = parser.ColumnParser("S_DIST_08").GetOptionalUtf8().GetOrElse("");
                    Dist09 = parser.ColumnParser("S_DIST_09").GetOptionalUtf8().GetOrElse("");
                    Dist10 = parser.ColumnParser("S_DIST_10").GetOptionalUtf8().GetOrElse("");
                } catch (const std::exception& ex) {
                    throw yexception() << stream.Str() << ": " << ex.what();
                } catch (...) {
                    throw yexception() << stream.Str();
                }
            };
            
            return terminal.ExecuteAndProcessingDataQuery(dataQuery, std::move(params), transaction, processing, TString(funcName));
        }
    );
}

NThreading::TFuture<i32> TOorderData::GetCustomerId(TMaybe<TTransaction>& transaction, TLog& log, TTerminal& terminal,
                                                    i32 whId, i32 distId, i32 orderId, TSession& session) {
    std::string funcName = __func__;

    TString query = R"(
        --!syntax_v1

        DECLARE $whId AS Int32;
        DECLARE $distId AS Int32;
        DECLARE $orderId AS Int32;

        SELECT O_C_ID
          FROM `oorder`
         WHERE O_ID = $orderId
           AND O_D_ID = $distId
           AND O_W_ID = $whId;
    )";

    return terminal.ApplyByThreadPool(
        session.PrepareDataQuery(query),
        [&transaction, &log, &terminal, whId, distId, orderId, funcName](const TAsyncPrepareQueryResult& future) {
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

            auto processing = [&log, whId, distId, orderId, funcName](const TAsyncDataQueryResult& future) {
                TDataQueryResult result = future.GetValue();
                TStringStream stream;
                stream << "Failed to retrieve OORDER records [W_ID=" << whId << ", D_ID=" << distId << ", O_ID=" << orderId << "]";
                ThrowOnError(result, log, funcName + stream.Str());

                TResultSetParser parser(result.GetResultSet(0));
                if (!parser.TryNextRow()) {
                    throw yexception() << stream.Str();
                }
                return parser.ColumnParser("O_C_ID").GetOptionalInt32().GetOrElse(0);
            };

            return terminal.ExecuteAndProcessingDataQuery(dataQuery, std::move(params), transaction, processing, TString(funcName));
        }
    );
}

NThreading::TFuture<void> TNewOrderData::InsertNewOrder(TMaybe<TTransaction>& transaction, TLog& log, TTerminal& terminal,
                                                        i32 whId, i32 distId, i32 distNextOId, TSession& session) {
    std::string funcName = __func__;

    TString query = R"(
        --!syntax_v1

        DECLARE $whId AS Int32;
        DECLARE $distId AS Int32;
        DECLARE $distNextOId AS Int32;

        UPSERT INTO `new_order`
        (NO_O_ID, NO_D_ID, NO_W_ID)
        VALUES ($distNextOId, $distId, $whId);
    )";
    
    return terminal.ApplyByThreadPool(
        session.PrepareDataQuery(query),
        [&transaction, &log, &terminal, whId, distId, distNextOId, funcName](const TAsyncPrepareQueryResult& future) {
            auto dataQuery = future.GetValue().GetQuery();
    
            auto params = dataQuery.GetParamsBuilder()
                .AddParam("$distNextOId")
                    .Int32(distNextOId)
                    .Build()
                .AddParam("$distId")
                    .Int32(distId)
                    .Build()
                .AddParam("$whId")
                    .Int32(whId)
                    .Build()
                .Build();

            auto processing = [&log, whId, distId, distNextOId, funcName](const TAsyncDataQueryResult& future) {
                TDataQueryResult result = future.GetValue();
                TStringStream stream;
                stream << "Failed to insert NEW_ORDER record [W_ID=" << whId << ", D_ID=" << distId << ", O_ID=" << distNextOId << "]";
                ThrowOnError(result, log, funcName + stream.Str());
            };

            return terminal.ExecuteAndProcessingDataQuery(dataQuery, std::move(params), transaction, processing, TString(funcName));
        }
    );
}

NThreading::TFuture<void> TOorderData::InsertOpenOrder(TMaybe<TTransaction>& transaction, TLog& log, TTerminal& terminal,
                                                       i32 whId, i32 distId, i32 custId, i32 distNextOId,
                                                       i32 itemCount, i32 allLocal, TSession& session) {
    std::string funcName = __func__;

    TString query = R"(
        --!syntax_v1

        DECLARE $whId AS Int32;
        DECLARE $distId AS Int32;
        DECLARE $custId AS Int32;
        DECLARE $itemCount AS Int32;
        DECLARE $allLocal AS Int32;
        DECLARE $distNextOId AS Int32;
        DECLARE $entryD AS Timestamp;

        UPSERT INTO `oorder`
        (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL)
        VALUES ($distNextOId, $distId, $whId, $custId, $entryD, $itemCount, $allLocal);
    )";
    
    return terminal.ApplyByThreadPool(
        session.PrepareDataQuery(query),
        [&transaction, &log, &terminal, whId, distId, custId, itemCount, allLocal, distNextOId, funcName](const TAsyncPrepareQueryResult& future) {
            auto dataQuery = future.GetValue().GetQuery();
    
            auto params = dataQuery.GetParamsBuilder()
                .AddParam("$whId")
                    .Int32(whId)
                    .Build()
                .AddParam("$distId")
                    .Int32(distId)
                    .Build()
                .AddParam("$custId")
                    .Int32(custId)
                    .Build()
                .AddParam("$itemCount")
                    .Int32(itemCount)
                    .Build()
                .AddParam("$allLocal")
                    .Int32(allLocal)
                    .Build()
                .AddParam("$distNextOId")
                    .Int32(distNextOId)
                    .Build()
                .AddParam("$entryD")
                    .Timestamp(Now())
                    .Build()
                .Build();

            auto processing = [&log, whId, distId, distNextOId, funcName](const TAsyncDataQueryResult& future) {
                TDataQueryResult result = future.GetValue();
                TStringStream stream;
                stream << "Failed to insert OORDER record [W_ID=" << whId << ", D_ID=" << distId << ", O_ID=" << distNextOId << "]";;
                ThrowOnError(result, log, funcName + stream.Str());
            };

            return terminal.ExecuteAndProcessingDataQuery(dataQuery, std::move(params), transaction, processing, TString(funcName));
        }
    );
}

}
}
