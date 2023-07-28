#pragma once

#include <ydb/library/yql/providers/common/mkql/yql_provider_mkql.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/ast/yql_expr.h>

#include <util/generic/strbuf.h>
#include <util/generic/vector.h>
#include <util/generic/string.h>

#include <utility>

namespace NYql {

NKikimr::NMiniKQL::TRuntimeNode BuildTableOutput(NKikimr::NMiniKQL::TRuntimeNode list, NCommon::TMkqlBuildContext& ctx);
NKikimr::NMiniKQL::TRuntimeNode BuildRuntimeTableInput(TStringBuf callName, NKikimr::NMiniKQL::TType* outItemType, TStringBuf clusterName,
    TStringBuf tableName, TStringBuf spec, bool isTemp, NCommon::TMkqlBuildContext& ctx);

NKikimr::NMiniKQL::TRuntimeNode SortListBy(NKikimr::NMiniKQL::TRuntimeNode list, const TVector<std::pair<TString, bool>>& sortBy, NCommon::TMkqlBuildContext& ctx);

void RegisterYtFileMkqlCompilers(NCommon::TMkqlCallableCompilerBase& compiler);
void RegisterDqYtFileMkqlCompilers(NCommon::TMkqlCallableCompilerBase& compiler);

}
