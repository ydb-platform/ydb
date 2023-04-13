#pragma once

#include <ydb/public/api/protos/draft/fq.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_value/value.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>

#include <util/generic/string.h>

#include <library/cpp/json/json_writer.h>

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/minikql/mkql_node.h>

namespace NFq {

void FormatResultSet(NJson::TJsonValue& root, const NYdb::TResultSet& resultSet, bool typeNameAsString = false, bool prettyValueFormat = false);
TString FormatSchema(const FederatedQuery::Schema& schema);
const NYql::TTypeAnnotationNode* MakeStructType(const TVector<std::pair<TString, const NYql::TTypeAnnotationNode*>>& i, NYql::TExprContext& ctx);
NKikimr::NMiniKQL::TType* MakeStructType(const TVector<std::pair<TString, NKikimr::NMiniKQL::TType*>>& items, NKikimr::NMiniKQL::TTypeEnvironment& env);
const NYql::TTypeAnnotationNode* MakeType(NYdb::TTypeParser& parser, NYql::TExprContext& ctx);

} // namespace NFq
