#include "yql_clickhouse_mkql_compiler.h"
#include "yql_clickhouse_util.h"

#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/common/mkql/yql_type_mkql.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/core/yql_opt_utils.h>

#include <library/cpp/json/json_writer.h>

#include <algorithm>

namespace NYql {

using namespace NKikimr::NMiniKQL;
using namespace NNodes;

namespace {

TRuntimeNode BuildNativeParseCall(TRuntimeNode input, TType* inputItemType, TType* outputItemType, const std::string_view& timezone, NCommon::TMkqlBuildContext& ctx)
{
    const auto userType = ctx.ProgramBuilder.NewTupleType({ctx.ProgramBuilder.NewTupleType({inputItemType}), ctx.ProgramBuilder.NewStructType({}), outputItemType});
    const auto flow = ctx.ProgramBuilder.ToFlow(ctx.ProgramBuilder.Apply(ctx.ProgramBuilder.Udf("ClickHouseClient.ParseFormat", {}, userType, "Native"), {input, ctx.ProgramBuilder.NewOptional(ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::Utf8>(timezone)) }));
    const auto structType = static_cast<const TStructType*>(outputItemType);
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

void RegisterDqClickHouseMkqlCompilers(NCommon::TMkqlCallableCompilerBase& compiler, const TClickHouseState::TPtr& state) {
    compiler.ChainCallable(TDqSourceWideWrap::CallableName(),
        [state](const TExprNode& node, NCommon::TMkqlBuildContext& ctx) {
            if (const auto wrapper = TDqSourceWideWrap(&node); wrapper.DataSource().Category().Value() == ClickHouseProviderName) {
                const auto input = MkqlBuildExpr(wrapper.Input().Ref(), ctx);
                const auto inputItemType = NCommon::BuildType(wrapper.Input().Ref(), *wrapper.Input().Ref().GetTypeAnn(), ctx.ProgramBuilder);
                const auto outputItemType = NCommon::BuildType(wrapper.RowType().Ref(), *wrapper.RowType().Ref().GetTypeAnn()->Cast<TTypeExprType>()->GetType(), ctx.ProgramBuilder);
                return BuildNativeParseCall(input, inputItemType, outputItemType, state->Timezones[wrapper.DataSource().Cast<TClDataSource>().Cluster().Value()], ctx);
            }

            return TRuntimeNode();
        });
}

}
