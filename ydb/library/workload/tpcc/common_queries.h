#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <util/datetime/base.h>
#include <util/generic/fwd.h>

#include <optional>

namespace NYdb::NTPCC {

struct TCustomer {
    int c_id;
    TString c_first;
    TString c_middle;
    TString c_last;
    TString c_street_1;
    TString c_street_2;
    TString c_city;
    TString c_state;
    TString c_zip;
    TString c_phone;
    TString c_credit;
    TString c_data;
    double c_credit_lim;
    double c_discount;
    double c_balance;
    double c_ytd_payment;
    int c_payment_cnt;
    TInstant c_since;
};

struct TTransactionContext;

TCustomer ParseCustomerFromResult(TResultSetParser& parser);

NYdb::NQuery::TAsyncExecuteQueryResult GetCustomerById(
    NQuery::TSession& session,
    const std::optional<NYdb::NQuery::TTransaction>& tx,
    TTransactionContext& context,
    int warehouseID,
    int districtID,
    int customerID);

NYdb::NQuery::TAsyncExecuteQueryResult GetCustomersByLastName(
    NQuery::TSession& session,
    const std::optional<NYdb::NQuery::TTransaction>& tx,
    TTransactionContext& context,
    int warehouseID,
    int districtID,
    const TString& lastName);

// Selects a customer from a result set using the TPC-C rule (middle customer)
// Returns std::nullopt if the result set was empty
std::optional<TCustomer> SelectCustomerFromResultSet(const NYdb::TResultSet& resultSet);

} // namespace NYdb::NTPCC
