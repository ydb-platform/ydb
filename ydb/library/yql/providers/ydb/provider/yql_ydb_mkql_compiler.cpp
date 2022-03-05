#include "yql_ydb_mkql_compiler.h"

#include <ydb/library/yql/providers/ydb/expr_nodes/yql_ydb_expr_nodes.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/common/mkql/yql_type_mkql.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/core/yql_opt_utils.h>

#include <library/cpp/yson/node/node.h>
#include <library/cpp/yson/node/node_io.h>

#include <algorithm>

namespace NYql {

using namespace NKikimr::NMiniKQL;
using namespace NNodes;

namespace {

TRuntimeNode BuildYdbParseCall(TRuntimeNode input, TType* itemType, NCommon::TMkqlBuildContext& ctx)
{
    const auto flow = ctx.ProgramBuilder.ToFlow(ctx.ProgramBuilder.Apply(ctx.ProgramBuilder.Udf("ClickHouseClient.ParseFromYdb", {}, itemType), {input}));
    const auto structType = static_cast<const TStructType*>(itemType);
    return ctx.ProgramBuilder.ExpandMap(flow,
        [&](TRuntimeNode item) {
            TRuntimeNode::TList fields;
            fields.reserve(structType->GetMembersCount());
            auto j = 0U;
            std::generate_n(std::back_inserter(fields), structType->GetMembersCount(), [&](){ return ctx.ProgramBuilder.Member(item, structType->GetMemberName(j++)); });
            return fields;
        });
}

}

void RegisterDqYdbMkqlCompilers(NCommon::TMkqlCallableCompilerBase& compiler, const TYdbState::TPtr&) {
    compiler.ChainCallable(TDqSourceWideWrap::CallableName(),
        [](const TExprNode& node, NCommon::TMkqlBuildContext& ctx) {
            if (const auto wrapper = TDqSourceWideWrap(&node); wrapper.DataSource().Category().Value() == YdbProviderName) {
                const auto input = MkqlBuildExpr(wrapper.Input().Ref(), ctx);
                const auto inputItemType = NCommon::BuildType(wrapper.RowType().Ref(), *wrapper.RowType().Ref().GetTypeAnn()->Cast<TTypeExprType>()->GetType(), ctx.ProgramBuilder);
                return BuildYdbParseCall(input, inputItemType, ctx);
            }

            return TRuntimeNode();
        });
}

}
