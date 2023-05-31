#pragma once

#include <ydb/library/yql/ast/yql_expr.h>

#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/common/mkql/yql_type_mkql.h>
#include <ydb/library/yql/providers/common/mkql/yql_provider_mkql.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_node.h>

#include <algorithm>

namespace NYql {

NKikimr::NMiniKQL::TRuntimeNode BuildParseCall(
    TPosition pos,
    NKikimr::NMiniKQL::TRuntimeNode input,
    TMaybe<NKikimr::NMiniKQL::TRuntimeNode> extraColumnsByPathIndex,
    std::unordered_map<TString, ui32>&& metadataColumns,
    const std::string_view& format,
    const std::string_view& compression,
    const std::vector<std::pair<std::string_view, std::string_view>>& formatSettings,
    NKikimr::NMiniKQL::TType* inputType,
    NKikimr::NMiniKQL::TType* parseItemType,
    NKikimr::NMiniKQL::TType* finalItemType,
    NCommon::TMkqlBuildContext& ctx,
    bool useBlocks = false);

TMaybe<NKikimr::NMiniKQL::TRuntimeNode> TryWrapWithParser(const NYql::NNodes::TDqSourceWrapBase& wrapper, NCommon::TMkqlBuildContext& ctx, bool useBlocks = false);
TMaybe<NKikimr::NMiniKQL::TRuntimeNode> TryWrapWithParserForArrowIPCStreaming(const NYql::NNodes::TDqSourceWrapBase& wrapper, NCommon::TMkqlBuildContext& ctx);

}
