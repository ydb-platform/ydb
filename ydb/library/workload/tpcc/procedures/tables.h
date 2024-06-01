#pragma once

#include <ydb/library/workload/tpcc/terminal.h>

#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <library/cpp/threading/future/core/future.h>

#include <util/datetime/base.h>
#include <util/generic/function.h>


namespace NYdbWorkload {
namespace NTPCC {

using namespace NYdb;
using namespace NTable;

struct TCustomerData {
    i32 Id = 0;
    i32 DistId = 0;
    i32 WhId = 0;
    i32 PaymentCount = 0;
    i32 DeliveryCount = 0;
    double Discount = 0;
    double CreditLimit = 0;
    double Balance = 0;
    double YtdPayment = 0;
    TInstant Since;
    TString Credit;
    TString Last;
    TString First;
    TString Street1;
    TString Street2;
    TString City;
    TString State;
    TString Zip;
    TString Phone;
    TString Middle;
    TString Data;

    NThreading::TFuture<void> GetCustomerDataById(TMaybe<TTransaction>& transaction, TLog& log, TTerminal& terminal,
                                                  i32 whId, i32 distId, i32 custId,
                                                  TSession& session);
    NThreading::TFuture<void> GetCustomerById(TMaybe<TTransaction>& transaction, TLog& log, TTerminal& terminal,
                                              i32 whId, i32 distId, i32 custId,
                                              TSession& session);
    NThreading::TFuture<void> GetCustomerByName(TMaybe<TTransaction>& transaction, TLog& log, TTerminal& terminal,
                                                i32 whId, i32 distId, const TString& lastName,
                                                TSession& session);
};

struct TDistrictData {
    i32 Id = 0;
    i32 WhId = 0;
    i32 NextOrderId = 0;
    double Ytd = 0;
    double Tax = 0;
    TString Name;
    TString Street1;
    TString Street2;
    TString City;
    TString State;
    TString Zip;

    NThreading::TFuture<void> GetDistrict(TMaybe<TTransaction>& transaction, TLog& log, TTerminal& terminal,
                                          i32 whId, i32 distId,
                                          TSession& session);
};

struct THistoryData {
    i32 CustId = 0;
    i32 CustDistId = 0;
    i32 CustWhId = 0;
    i32 WhId = 0;
    i32 DistId = 0;
    double Amount = 0;
    TInstant Date;
    TString Data;
};

struct TItemData {
    i32 Id = 0;
    i32 ImId = 0;
    double Price = 0;
    TString Name;
    TString Data;
    static NThreading::TFuture<double> GetItemPrice(TMaybe<TTransaction>& transaction, TLog& log, TTerminal& terminal,
                                                    i32 itemId, TSession& session);
};

struct TNewOrderData {
    i32 WhId = 0;
    i32 DistId = 0;
    i32 OrderId = 0;
    static NThreading::TFuture<void> InsertNewOrder(TMaybe<TTransaction>& transaction, TLog& log, TTerminal& terminal,
                                                    i32 whId, i32 distId, i32 distNextOId,
                                                    TSession& session);
};

struct TOorderData {
    i32 Id = 0;
    i32 WhId = 0;
    i32 DistId = 0;
    i32 CustId = 0;
    i32 CarrierId = 0;
    i32 OlCount = 0;
    i32 AllLocal = 0;
    TInstant EntryD;

    static NThreading::TFuture<i32> GetCustomerId(TMaybe<TTransaction>& transaction, TLog& log, TTerminal& terminal,
                                                  i32 whId, i32 distId, i32 orderId,
                                                  TSession& session);

    static NThreading::TFuture<void> InsertOpenOrder(TMaybe<TTransaction>& transaction, TLog& log, TTerminal& terminal,
                                                     i32 whId, i32 distId, i32 custId, i32 distNextOId,
                                                     i32 itemCount, i32 allLocal, TSession& session);
};

struct TOrderLineData {
    i32 WhId = 0;
    i32 DistId = 0;
    i32 OrderId = 0;
    i32 Number = 0;
    i32 ItemId = 0;
    i32 SupplyWhId = 0;
    i32 Quantity = 0;
    double Amount = 0;
    TInstant DeliveryD;
    TString DistInfo;
};

struct TStockData {
    i32 WhId = 0;
    i32 ItemId = 0;
    i32 OrderCount = 0;
    i32 RemoteCount = 0;
    i32 Quantity = 0;
    double Ytd = 0;
    TString Data;
    TString Dist01;
    TString Dist02;
    TString Dist03;
    TString Dist04;
    TString Dist05;
    TString Dist06;
    TString Dist07;
    TString Dist08;
    TString Dist09;
    TString Dist10;
    NThreading::TFuture<void> GetStock(TMaybe<TTransaction>& transaction, TLog& log, TTerminal& terminal,
                                       i32 whId, i32 itemId, TSession& session);
};

struct TWarehouseData {
    i32 Id = 0;
    double Ytd = 0;
    double Tax = 0;
    TString Name;
    TString Street1;
    TString Street2;
    TString City;
    TString State;
    TString Zip;
    NThreading::TFuture<void> GetWarehouse(TMaybe<TTransaction>& transaction, TLog& log, TTerminal& terminal,
                                           i32 whId, TSession& session);
};

}
}
