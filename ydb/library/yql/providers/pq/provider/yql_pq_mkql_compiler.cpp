#include "yql_pq_mkql_compiler.h"
#include "yql_pq_helpers.h"

#include <ydb/library/yql/providers/pq/common/yql_names.h>
#include <ydb/library/yql/providers/pq/expr_nodes/yql_pq_expr_nodes.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/dq/mkql/parser.h>

namespace NYql {

using namespace NKikimr::NMiniKQL;
using namespace NNodes;

namespace {

bool UseSharedReading(TExprNode::TPtr settings) {
    const auto maybeInnerSettings = FindSetting(settings, "settings");
    if (!maybeInnerSettings) {
        return false;
    }

    const auto maybeSharedReadingSetting = FindSetting(maybeInnerSettings.Cast().Ptr(), SharedReading);
    if (!maybeSharedReadingSetting) {
        return false;
    }

    const TExprNode& value = maybeSharedReadingSetting.Cast().Ref();
    return value.IsAtom() && FromString<bool>(value.Content());
}

TRuntimeNode WrapSharedReading(const TDqSourceWrapBase &wrapper, NCommon::TMkqlBuildContext& ctx) {
    const auto input = MkqlBuildExpr(wrapper.Input().Ref(), ctx);
    const auto flow = ctx.ProgramBuilder.ToFlow(input);

    const TStructExprType* rowType = wrapper.RowType().Ref().GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TStructExprType>();
    const auto* finalItemStructType = static_cast<TStructType*>(NCommon::BuildType(wrapper.RowType().Ref(), *rowType, ctx.ProgramBuilder));

    return ctx.ProgramBuilder.ExpandMap(flow, [&](TRuntimeNode item) -> TRuntimeNode::TList {
        TRuntimeNode::TList fields;
        fields.reserve(finalItemStructType->GetMembersCount());
        for (ui32 i = 0; i < finalItemStructType->GetMembersCount(); ++i) {
            fields.push_back(ctx.ProgramBuilder.Member(item, finalItemStructType->GetMemberName(i)));
        }
        return fields;
    });
}

}

void RegisterDqPqMkqlCompilers(NCommon::TMkqlCallableCompilerBase& compiler) {
    compiler.ChainCallable(TDqSourceWideWrap::CallableName(),
        [](const TExprNode& node, NCommon::TMkqlBuildContext& ctx) {
            if (const auto wrapper = TDqSourceWideWrap(&node); wrapper.DataSource().Category().Value() == PqProviderName) {
                if (const auto maybeSettings = wrapper.Settings()) {
                    if (UseSharedReading(maybeSettings.Cast().Ptr())) {
                        return WrapSharedReading(wrapper, ctx);
                    }
                }

                const auto wrapped = TryWrapWithParser(wrapper, ctx);
                if (wrapped) {
                    return *wrapped;
                }

                const auto input = MkqlBuildExpr(wrapper.Input().Ref(), ctx);
                auto flow = ctx.ProgramBuilder.ToFlow(input);
                return ctx.ProgramBuilder.ExpandMap(flow,
                    [&](TRuntimeNode item) -> TRuntimeNode::TList {
                        return {item};
                    });
            }

            return TRuntimeNode();
        });
}

}
