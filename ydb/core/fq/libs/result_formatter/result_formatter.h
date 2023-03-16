#pragma once

#include <ydb/public/api/protos/draft/fq.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_value/value.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>

#include <util/generic/string.h>

#include <library/cpp/json/json_writer.h>

namespace NFq {

void FormatResultSet(NJson::TJsonValue& root, const NYdb::TResultSet& resultSet, bool typeNameAsString = false, bool prettyValueFormat = false);
TString FormatSchema(const FederatedQuery::Schema& schema);

} // namespace NFq
