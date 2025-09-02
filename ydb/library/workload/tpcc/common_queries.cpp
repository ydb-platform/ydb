#include "common_queries.h"
#include "constants.h"
#include "log.h"
#include "transactions.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <util/string/printf.h>
#include <vector>
#include <format>
#include <string>

namespace NYdb::NTPCC {

//-----------------------------------------------------------------------------

using namespace NYdb::NQuery;

//-----------------------------------------------------------------------------

TCustomer ParseCustomerFromResult(TResultSetParser& parser) {
    TCustomer customer;
    if (parser.ColumnIndex("C_ID") != -1) {
        customer.c_id = parser.ColumnParser("C_ID").GetInt32();
    }
    customer.c_first = *parser.ColumnParser("C_FIRST").GetOptionalUtf8();
    customer.c_middle = *parser.ColumnParser("C_MIDDLE").GetOptionalUtf8();
    if (parser.ColumnIndex("C_LAST") != -1) {
        customer.c_last = *parser.ColumnParser("C_LAST").GetOptionalUtf8();
    }
    customer.c_street_1 = *parser.ColumnParser("C_STREET_1").GetOptionalUtf8();
    customer.c_street_2 = *parser.ColumnParser("C_STREET_2").GetOptionalUtf8();
    customer.c_city = *parser.ColumnParser("C_CITY").GetOptionalUtf8();
    customer.c_state = *parser.ColumnParser("C_STATE").GetOptionalUtf8();
    customer.c_zip = *parser.ColumnParser("C_ZIP").GetOptionalUtf8();
    customer.c_phone = *parser.ColumnParser("C_PHONE").GetOptionalUtf8();
    customer.c_credit = *parser.ColumnParser("C_CREDIT").GetOptionalUtf8();
    customer.c_credit_lim = *parser.ColumnParser("C_CREDIT_LIM").GetOptionalDouble();
    customer.c_discount = *parser.ColumnParser("C_DISCOUNT").GetOptionalDouble();
    customer.c_balance = *parser.ColumnParser("C_BALANCE").GetOptionalDouble();
    customer.c_ytd_payment = *parser.ColumnParser("C_YTD_PAYMENT").GetOptionalDouble();
    customer.c_payment_cnt = *parser.ColumnParser("C_PAYMENT_CNT").GetOptionalInt32();
    customer.c_since = *parser.ColumnParser("C_SINCE").GetOptionalTimestamp();
    return customer;
}

//-----------------------------------------------------------------------------

TAsyncExecuteQueryResult GetCustomerById(
    TSession& session,
    const std::optional<TTransaction>& tx,
    TTransactionContext& context,
    int warehouseID,
    int districtID,
    int customerID)
{
    auto& Log = context.Log;
    static std::string query = std::format(R"(
        PRAGMA TablePathPrefix("{}");

        DECLARE $c_w_id AS Int32;
        DECLARE $c_d_id AS Int32;
        DECLARE $c_id AS Int32;

        SELECT C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2,
               C_CITY, C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM,
               C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE
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

    auto txControl = tx ? TTxControl::Tx(*tx) : TTxControl::BeginTx(TTxSettings::SerializableRW());

    auto result = session.ExecuteQuery(
        query,
        txControl,
        std::move(params));

    LOG_T("Terminal " << context.TerminalID << " waiting for customer by ID result");
    return result;
}

//-----------------------------------------------------------------------------

TAsyncExecuteQueryResult GetCustomersByLastName(
    TSession& session,
    const std::optional<TTransaction>& tx,
    TTransactionContext& context,
    int warehouseID,
    int districtID,
    const TString& lastName)
{
    auto& Log = context.Log;
    static std::string query = std::format(R"(
        PRAGMA TablePathPrefix("{}");

        DECLARE $c_w_id AS Int32;
        DECLARE $c_d_id AS Int32;
        DECLARE $c_last AS Utf8;

        SELECT C_FIRST, C_MIDDLE, C_ID, C_STREET_1, C_STREET_2, C_CITY,
               C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT,
               C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE
          FROM `{}` VIEW `{}` AS idx
         WHERE idx.C_W_ID = $c_w_id
           AND idx.C_D_ID = $c_d_id
           AND idx.C_LAST = $c_last
         ORDER BY idx.C_FIRST;
    )", context.Path.c_str(), TABLE_CUSTOMER, INDEX_CUSTOMER_NAME);

    auto params = TParamsBuilder()
        .AddParam("$c_w_id").Int32(warehouseID).Build()
        .AddParam("$c_d_id").Int32(districtID).Build()
        .AddParam("$c_last").Utf8(lastName).Build()
        .Build();

    auto txControl = tx ? TTxControl::Tx(*tx) : TTxControl::BeginTx(TTxSettings::SerializableRW());
    auto result = session.ExecuteQuery(
        query,
        txControl,
        std::move(params));

    LOG_T("Terminal " << context.TerminalID << " waiting for customers by last name result");
    return result;
}

//-----------------------------------------------------------------------------

std::optional<TCustomer> SelectCustomerFromResultSet(
    const NYdb::TResultSet& resultSet)
{
    TResultSetParser parser(resultSet);
    size_t rowCount = resultSet.RowsCount();

    if (rowCount == 0) {
        return std::nullopt;
    }

    // TPC-C 2.5.2.2: Position n / 2 rounded up to the next integer, but
    // that counts starting from 1.
    size_t index = rowCount / 2;
    if (rowCount % 2 == 0 && index > 0) {
        index--;
    }

    // note "<=" is needed, because initially parser is not set to zero position
    for (size_t i = 0; i <= index; ++i) {
        parser.TryNextRow();
    }

    return ParseCustomerFromResult(parser);
}

} // namespace NYdb::NTPCC
