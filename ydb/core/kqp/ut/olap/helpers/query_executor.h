#pragma once
#include <ydb/core/testlib/cs_helper.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/value/value.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <library/cpp/json/writer/json_value.h>

namespace NKikimr::NKqp {

TVector<THashMap<TString, NYdb::TValue>> CollectRows(NYdb::NTable::TScanQueryPartIterator& it, NJson::TJsonValue* statInfo = nullptr, NJson::TJsonValue* diagnostics = nullptr);
TVector<THashMap<TString, NYdb::TValue>> ExecuteScanQuery(NYdb::NTable::TTableClient& tableClient, const TString& query, const bool verbose = true);

}