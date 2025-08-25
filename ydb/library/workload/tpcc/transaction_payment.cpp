#include "transactions.h"

#include "common_queries.h"
#include "constants.h"
#include "log.h"
#include "util.h"

#include <library/cpp/time_provider/monotonic.h>

#include <util/string/printf.h>

#include <format>
#include <string>

namespace NYdb::NTPCC {

namespace {

//-----------------------------------------------------------------------------

using namespace NYdb::NQuery;

//-----------------------------------------------------------------------------

TAsyncExecuteQueryResult UpdateWarehouse(
    TSession& session, TTransactionContext& context, int warehouseID, double paymentAmount)
{
    auto& Log = context.Log;
    static std::string query = std::format(R"(
        PRAGMA TablePathPrefix("{}");

        DECLARE $w_id AS Int32;
        DECLARE $payment AS Double;

        UPDATE `{}`
           SET W_YTD = W_YTD + $payment
         WHERE W_ID = $w_id
        RETURNING W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_NAME;
    )", context.Path.c_str(), TABLE_WAREHOUSE);

    auto params = TParamsBuilder()
        .AddParam("$w_id").Int32(warehouseID).Build()
        .AddParam("$payment").Double(paymentAmount).Build()
        .Build();

    auto result = session.ExecuteQuery(
        query,
        TTxControl::BeginTx(TTxSettings::SerializableRW()),
        std::move(params));

    LOG_T("Terminal " << context.TerminalID << " waiting for warehouse update result");
    return result;
}

//-----------------------------------------------------------------------------

TAsyncExecuteQueryResult UpdateDistrict(
    TSession& session, const TTransaction& tx, TTransactionContext& context,
    int warehouseID, int districtID, double paymentAmount)
{
    auto& Log = context.Log;
    static std::string query = std::format(R"(
        PRAGMA TablePathPrefix("{}");

        DECLARE $d_w_id AS Int32;
        DECLARE $d_id AS Int32;
        DECLARE $payment AS Double;

        UPDATE `{}`
           SET D_YTD = D_YTD + $payment
         WHERE D_W_ID = $d_w_id
           AND D_ID = $d_id
        RETURNING D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_NAME;
    )", context.Path.c_str(), TABLE_DISTRICT);

    auto params = TParamsBuilder()
        .AddParam("$d_w_id").Int32(warehouseID).Build()
        .AddParam("$d_id").Int32(districtID).Build()
        .AddParam("$payment").Double(paymentAmount).Build()
        .Build();

    auto result = session.ExecuteQuery(
        query,
        TTxControl::Tx(tx),
        std::move(params));

    LOG_T("Terminal " << context.TerminalID << " waiting for district update result");
    return result;
}

//-----------------------------------------------------------------------------

TAsyncExecuteQueryResult GetCustomerCData(
    TSession& session, const TTransaction& tx, TTransactionContext& context,
    int warehouseID, int districtID, int customerID)
{
    auto& Log = context.Log;
    static std::string query = std::format(R"(
        PRAGMA TablePathPrefix("{}");

        DECLARE $c_w_id AS Int32;
        DECLARE $c_d_id AS Int32;
        DECLARE $c_id AS Int32;

        SELECT C_DATA
          FROM `{}`
         WHERE C_W_ID = $c_w_id
           AND C_D_ID = $c_d_id
           AND C_ID = $c_id;
    )", context.Path.c_str(), TABLE_CUSTOMER);

    auto params = TParamsBuilder()
        .AddParam("$c_w_id").Int32(warehouseID).Build()
        .AddParam("$c_d_id").Int32(districtID).Build()
        .AddParam("$c_id").Int32(customerID).Build()
        .Build();

    auto result = session.ExecuteQuery(
        query,
        TTxControl::Tx(tx),
        std::move(params));

    LOG_T("Terminal " << context.TerminalID << " waiting for customer data result");
    return result;
}

//-----------------------------------------------------------------------------

TAsyncExecuteQueryResult UpdateCustomerWithCData(
    TSession& session, const TTransaction& tx, TTransactionContext& context,
    int warehouseID, int districtID, int customerID, double balance, double ytdPayment,
    int paymentCnt, const TString& cData)
{
    auto& Log = context.Log;
    static std::string query = std::format(R"(
        PRAGMA TablePathPrefix("{}");

        DECLARE $c_w_id AS Int32;
        DECLARE $c_d_id AS Int32;
        DECLARE $c_id AS Int32;
        DECLARE $c_balance AS Double;
        DECLARE $c_ytd_payment AS Double;
        DECLARE $c_payment_cnt AS Int32;
        DECLARE $c_data AS Utf8;

        UPSERT INTO `{}`
         (C_W_ID, C_D_ID, C_ID, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA)
         VALUES ($c_w_id, $c_d_id, $c_id, $c_balance, $c_ytd_payment, $c_payment_cnt, $c_data);
    )", context.Path.c_str(), TABLE_CUSTOMER);

    auto params = TParamsBuilder()
        .AddParam("$c_w_id").Int32(warehouseID).Build()
        .AddParam("$c_d_id").Int32(districtID).Build()
        .AddParam("$c_id").Int32(customerID).Build()
        .AddParam("$c_balance").Double(balance).Build()
        .AddParam("$c_ytd_payment").Double(ytdPayment).Build()
        .AddParam("$c_payment_cnt").Int32(paymentCnt).Build()
        .AddParam("$c_data").Utf8(cData).Build()
        .Build();

    auto result = session.ExecuteQuery(
        query,
        TTxControl::Tx(tx),
        std::move(params));

    LOG_T("Terminal " << context.TerminalID << " waiting for customer update with data result");
    return result;
}

//-----------------------------------------------------------------------------

TAsyncExecuteQueryResult UpdateCustomer(
    TSession& session, const TTransaction& tx, TTransactionContext& context,
    int warehouseID, int districtID, int customerID, double balance, double ytdPayment,
    int paymentCnt)
{
    auto& Log = context.Log;
    static std::string query = std::format(R"(
        PRAGMA TablePathPrefix("{}");

        DECLARE $c_w_id AS Int32;
        DECLARE $c_d_id AS Int32;
        DECLARE $c_id AS Int32;
        DECLARE $c_balance AS Double;
        DECLARE $c_ytd_payment AS Double;
        DECLARE $c_payment_cnt AS Int32;

        UPSERT INTO `{}`
         (C_W_ID, C_D_ID, C_ID, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT)
         VALUES ($c_w_id, $c_d_id, $c_id, $c_balance, $c_ytd_payment, $c_payment_cnt);
    )", context.Path.c_str(), TABLE_CUSTOMER);

    auto params = TParamsBuilder()
        .AddParam("$c_w_id").Int32(warehouseID).Build()
        .AddParam("$c_d_id").Int32(districtID).Build()
        .AddParam("$c_id").Int32(customerID).Build()
        .AddParam("$c_balance").Double(balance).Build()
        .AddParam("$c_ytd_payment").Double(ytdPayment).Build()
        .AddParam("$c_payment_cnt").Int32(paymentCnt).Build()
        .Build();

    auto result = session.ExecuteQuery(
        query,
        TTxControl::Tx(tx),
        std::move(params));

    LOG_T("Terminal " << context.TerminalID << " waiting for customer update result");
    return result;
}

//-----------------------------------------------------------------------------

TAsyncExecuteQueryResult InsertHistoryRecord(
    TSession& session, const TTransaction& tx, TTransactionContext& context,
    int customerDistrictID, int customerWarehouseID, int customerID,
    int districtID, int warehouseID, double amount, const TString& data)
{
    auto& Log = context.Log;
    static std::string query = std::format(R"(
        PRAGMA TablePathPrefix("{}");

        DECLARE $h_c_d_id AS Int32;
        DECLARE $h_c_w_id AS Int32;
        DECLARE $h_c_id AS Int32;
        DECLARE $h_d_id AS Int32;
        DECLARE $h_w_id AS Int32;
        DECLARE $h_date AS Timestamp;
        DECLARE $h_amount AS Double;
        DECLARE $h_data AS Utf8;
        DECLARE $h_c_nano_ts AS Int64;

        UPSERT INTO `{}`
         (H_C_D_ID, H_C_W_ID, H_C_ID, H_D_ID, H_W_ID, H_DATE, H_AMOUNT, H_DATA, H_C_NANO_TS)
         VALUES ($h_c_d_id, $h_c_w_id, $h_c_id, $h_d_id, $h_w_id, $h_date, $h_amount, $h_data, $h_c_nano_ts);
    )", context.Path.c_str(), TABLE_HISTORY);

    auto now = std::chrono::system_clock::now();
    auto nanoTs = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();

    auto params = TParamsBuilder()
        .AddParam("$h_c_d_id").Int32(customerDistrictID).Build()
        .AddParam("$h_c_w_id").Int32(customerWarehouseID).Build()
        .AddParam("$h_c_id").Int32(customerID).Build()
        .AddParam("$h_d_id").Int32(districtID).Build()
        .AddParam("$h_w_id").Int32(warehouseID).Build()
        .AddParam("$h_date").Timestamp(TInstant::Now()).Build()
        .AddParam("$h_amount").Double(amount).Build()
        .AddParam("$h_data").Utf8(data).Build()
        .AddParam("$h_c_nano_ts").Int64(nanoTs).Build()
        .Build();

    auto result = session.ExecuteQuery(
        query,
        TTxControl::Tx(tx),
        std::move(params));

    LOG_T("Terminal " << context.TerminalID << " waiting for history insert result");
    return result;
}

} // anonymous

//-----------------------------------------------------------------------------

NThreading::TFuture<TStatus> GetPaymentTask(
    TTransactionContext& context,
    TDuration& latency,
    TSession session)
{
    TMonotonic startTs = TMonotonic::Now();

    TTransactionInflightGuard guard;
    co_await TTaskReady(context.TaskQueue, context.TerminalID);

    auto& Log = context.Log;

    const int warehouseID = context.WarehouseID;
    const int districtID = RandomNumber(DISTRICT_LOW_ID, DISTRICT_HIGH_ID);
    const double paymentAmount = static_cast<double>(RandomNumber(100, 500000)) / 100.0;

    LOG_T("Terminal " << context.TerminalID << " started Payment transaction in "
        << warehouseID << ", " << districtID << ", session: " << session.GetId());

    // Update warehouse YTD

    auto warehouseFuture = UpdateWarehouse(session, context, warehouseID, paymentAmount);
    auto warehouseResult = co_await TSuspendWithFuture(warehouseFuture, context.TaskQueue, context.TerminalID);
    if (!warehouseResult.IsSuccess()) {
        if (ShouldExit(warehouseResult)) {
            LOG_E("Terminal " << context.TerminalID << " warehouse update failed: "
                << warehouseResult.GetIssues().ToOneLineString() << ", session: " << session.GetId());
            RequestStop();
            co_return TStatus(EStatus::CLIENT_INTERNAL_ERROR, NIssue::TIssues());
        }
        LOG_T("Terminal " << context.TerminalID << " warehouse update failed: "
            << warehouseResult.GetIssues().ToOneLineString() << ", session: " << session.GetId());
        co_return warehouseResult;
    }

    auto tx = *warehouseResult.GetTransaction();
    LOG_T("Terminal " << context.TerminalID << " Payment txId " << tx.GetId());

    TResultSetParser warehouseParser(warehouseResult.GetResultSet(0));
    if (!warehouseParser.TryNextRow()) {
        LOG_E("Terminal " << context.TerminalID << " warehouse not found: " << warehouseID << ", session: " << session.GetId());
        RequestStop();
        co_return TStatus(EStatus::CLIENT_INTERNAL_ERROR, NIssue::TIssues());
    }
    std::string warehouseName = *warehouseParser.ColumnParser("W_NAME").GetOptionalUtf8();

    // Update district YTD

    auto districtFuture = UpdateDistrict(session, tx, context, warehouseID, districtID, paymentAmount);
    auto districtResult = co_await TSuspendWithFuture(districtFuture, context.TaskQueue, context.TerminalID);
    if (!districtResult.IsSuccess()) {
        if (ShouldExit(districtResult)) {
            LOG_E("Terminal " << context.TerminalID << " district update failed: "
                << districtResult.GetIssues().ToOneLineString() << ", session: " << session.GetId());
            RequestStop();
            co_return TStatus(EStatus::CLIENT_INTERNAL_ERROR, NIssue::TIssues());
        }
        LOG_T("Terminal " << context.TerminalID << " district update failed: "
            << districtResult.GetIssues().ToOneLineString() << ", session: " << session.GetId());
        co_return districtResult;
    }

    TResultSetParser districtParser(districtResult.GetResultSet(0));
    if (!districtParser.TryNextRow()) {
        LOG_E("Terminal " << context.TerminalID << " district not found: W_ID=" << warehouseID
            << ", D_ID=" << districtID << ", session: " << session.GetId());
        RequestStop();
        co_return TStatus(EStatus::CLIENT_INTERNAL_ERROR, NIssue::TIssues());
    }
    std::string districtName = *districtParser.ColumnParser("D_NAME").GetOptionalUtf8();

    // Determine which warehouse and district the customer belongs to
    int customerDistrictID;
    int customerWarehouseID;

    // 85% are home warehouse
    if (RandomNumber(1, 100) <= 85) {
        customerDistrictID = districtID;
        customerWarehouseID = warehouseID;
    } else {
        customerDistrictID = RandomNumber(DISTRICT_LOW_ID, DISTRICT_HIGH_ID);

        do {
            customerWarehouseID = RandomNumber(1, context.WarehouseCount);
        } while (customerWarehouseID == warehouseID && context.WarehouseCount > 1);
    }

    // Get customer
    TCustomer customer;

    // 60% look up by last name, 40% by customer ID
    if (RandomNumber(1, 100) <= 60) {
        TString lastName = GetNonUniformRandomLastNameForRun();

        auto customersFuture = GetCustomersByLastName(session, tx, context, warehouseID, districtID, lastName);
        auto customersResult = co_await TSuspendWithFuture(customersFuture, context.TaskQueue, context.TerminalID);
        if (!customersResult.IsSuccess()) {
            if (ShouldExit(customersResult)) {
                LOG_E("Terminal " << context.TerminalID << " customers query failed: "
                    << customersResult.GetIssues().ToOneLineString() << ", session: " << session.GetId());
                RequestStop();
                co_return TStatus(EStatus::CLIENT_INTERNAL_ERROR, NIssue::TIssues());
            }
            LOG_T("Terminal " << context.TerminalID << " customers query failed: "
                << customersResult.GetIssues().ToOneLineString() << ", session: " << session.GetId());
            co_return customersResult;
        }

        tx = *customersResult.GetTransaction();

        auto selectedCustomer = SelectCustomerFromResultSet(customersResult.GetResultSet(0));
        if (!selectedCustomer) {
            LOG_E("Terminal " << context.TerminalID << " no customer found by name: "
                << warehouseID << ", " << districtID << ", " << lastName << ", session: " << session.GetId());
            RequestStop();
            co_return TStatus(EStatus::CLIENT_INTERNAL_ERROR, NIssue::TIssues());
        }
        customer = std::move(*selectedCustomer);
    } else {
        int customerID = GetRandomCustomerID();

        auto customerFuture = GetCustomerById(session, tx, context, warehouseID, districtID, customerID);
        auto customerResult = co_await TSuspendWithFuture(customerFuture, context.TaskQueue, context.TerminalID);
        if (!customerResult.IsSuccess()) {
            if (ShouldExit(customerResult)) {
                LOG_E("Terminal " << context.TerminalID << " customer query failed: "
                    << customerResult.GetIssues().ToOneLineString() << ", session: " << session.GetId());
                RequestStop();
                co_return TStatus(EStatus::CLIENT_INTERNAL_ERROR, NIssue::TIssues());
            }
            LOG_T("Terminal " << context.TerminalID << " customer query failed: "
                << customerResult.GetIssues().ToOneLineString() << ", session: " << session.GetId());
            co_return customerResult;
        }

        TResultSetParser customerParser(customerResult.GetResultSet(0));
        if (!customerParser.TryNextRow()) {
            LOG_E("Terminal " << context.TerminalID << " no customer found by id: "
                << warehouseID << ", " << districtID << ", " << customerID << ", session: " << session.GetId());
        }
        customer = ParseCustomerFromResult(customerParser);
        customer.c_id = customerID;
    }

    // Update customer balance

    customer.c_balance -= paymentAmount;
    customer.c_ytd_payment += paymentAmount;
    customer.c_payment_cnt += 1;

    // Check if customer has bad credit
    if (customer.c_credit == "BC") {
        // Get customer data for bad credit case
        auto cDataFuture = GetCustomerCData(session, tx, context, customerWarehouseID, customerDistrictID, customer.c_id);
        auto cDataResult = co_await TSuspendWithFuture(cDataFuture, context.TaskQueue, context.TerminalID);
        if (!cDataResult.IsSuccess()) {
            if (ShouldExit(cDataResult)) {
                LOG_E("Terminal " << context.TerminalID << " customer data query failed: "
                    << cDataResult.GetIssues().ToOneLineString() << ", session: " << session.GetId());
                RequestStop();
                co_return TStatus(EStatus::CLIENT_INTERNAL_ERROR, NIssue::TIssues());
            }
            LOG_T("Terminal " << context.TerminalID << " customer data query failed: "
                << cDataResult.GetIssues().ToOneLineString() << ", session: " << session.GetId());
            co_return cDataResult;
        }

        TResultSetParser cDataParser(cDataResult.GetResultSet(0));
        if (!cDataParser.TryNextRow()) {
            LOG_E("Terminal " << context.TerminalID << " customer data not found: " << customer.c_id << ", session: " << session.GetId());
            RequestStop();
            co_return TStatus(EStatus::CLIENT_INTERNAL_ERROR, NIssue::TIssues());
        }
        customer.c_data = cDataParser.ColumnParser("C_DATA").GetOptionalUtf8().value_or("");

        // Format new C_DATA
        TString newData = Sprintf("%d %d %d %d %d %.2f | %s",
            customer.c_id, customerDistrictID, customerWarehouseID, districtID, warehouseID,
            paymentAmount, customer.c_data.c_str());

        if (newData.length() > 500) {
            newData = newData.substr(0, 500);
        }
        customer.c_data = newData;

        // Update customer with C_DATA
        auto updateCustFuture = UpdateCustomerWithCData(session, tx, context,
            customerWarehouseID, customerDistrictID, customer.c_id,
            customer.c_balance, customer.c_ytd_payment, customer.c_payment_cnt,
            customer.c_data);

        auto updateCustResult = co_await TSuspendWithFuture(updateCustFuture, context.TaskQueue, context.TerminalID);
        if (!updateCustResult.IsSuccess()) {
            if (ShouldExit(updateCustResult)) {
                LOG_E("Terminal " << context.TerminalID << " customer update with data failed: "
                    << updateCustResult.GetIssues().ToOneLineString() << ", session: " << session.GetId());
                RequestStop();
                co_return TStatus(EStatus::CLIENT_INTERNAL_ERROR, NIssue::TIssues());
            }
            LOG_T("Terminal " << context.TerminalID << " customer update with data failed: "
                << updateCustResult.GetIssues().ToOneLineString() << ", session: " << session.GetId());
            co_return updateCustResult;
        }
    } else {
        // Update customer without C_DATA (good credit)
        auto updateCustFuture = UpdateCustomer(session, tx, context,
            customerWarehouseID, customerDistrictID, customer.c_id,
            customer.c_balance, customer.c_ytd_payment, customer.c_payment_cnt);

        auto updateCustResult = co_await TSuspendWithFuture(updateCustFuture, context.TaskQueue, context.TerminalID);
        if (!updateCustResult.IsSuccess()) {
            if (ShouldExit(updateCustResult)) {
                LOG_E("Terminal " << context.TerminalID << " customer update failed: "
                    << updateCustResult.GetIssues().ToOneLineString() << ", session: " << session.GetId());
                RequestStop();
                co_return TStatus(EStatus::CLIENT_INTERNAL_ERROR, NIssue::TIssues());
            }
            LOG_T("Terminal " << context.TerminalID << " customer update failed: "
                << updateCustResult.GetIssues().ToOneLineString() << ", session: " << session.GetId());
            co_return updateCustResult;
        }
    }

    // Insert history record
    TString historyData = warehouseName + "    " + districtName;
    if (historyData.length() > 24) {
        historyData = historyData.substr(0, 24);
    }

    auto historyFuture = InsertHistoryRecord(session, tx, context,
        customerDistrictID, customerWarehouseID, customer.c_id,
        districtID, warehouseID, paymentAmount, historyData);

    auto historyResult = co_await TSuspendWithFuture(historyFuture, context.TaskQueue, context.TerminalID);
    if (!historyResult.IsSuccess()) {
        if (ShouldExit(historyResult)) {
            LOG_E("Terminal " << context.TerminalID << " history insert failed: "
                << historyResult.GetIssues().ToOneLineString() << ", session: " << session.GetId());
            RequestStop();
            co_return TStatus(EStatus::CLIENT_INTERNAL_ERROR, NIssue::TIssues());
        }
        LOG_T("Terminal " << context.TerminalID << " history insert failed: "
            << historyResult.GetIssues().ToOneLineString() << ", session: " << session.GetId());
        co_return historyResult;
    }

    LOG_T("Terminal " << context.TerminalID << " is committing Payment transaction: "
        << "customer " << customer.c_id << ", amount " << paymentAmount << ", session: " << session.GetId());

    auto commitFuture = tx.Commit();
    auto commitResult = co_await TSuspendWithFuture(commitFuture, context.TaskQueue, context.TerminalID);

    TMonotonic endTs = TMonotonic::Now();
    latency = endTs - startTs;

    co_return commitResult;
}

} // namespace NYdb::NTPCC
