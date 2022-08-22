#pragma once

#include <ydb/public/api/protos/yq.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_value/value.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>

#include <util/generic/string.h>

#include <library/cpp/json/json_writer.h>

namespace NYq {

void FormatResultSet(NJson::TJsonValue& root, const NYdb::TResultSet& resultSet, bool typeNameAsString = false, bool prettyValueFormat = false);
TString FormatSchema(const YandexQuery::Schema& schema);

} // namespace NYq
