#include "yql_pq_mkql_compiler.h"
#include "yql_pq_helpers.h"

#include <ydb/library/yql/providers/pq/common/pq_meta_fields.h>
#include <ydb/library/yql/providers/pq/common/yql_names.h>
#include <ydb/library/yql/providers/pq/expr_nodes/yql_pq_expr_nodes.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/dq/mkql/parser.h>
#include <yql/essentials/providers/common/mkql/yql_provider_mkql.h>
#include <yql/essentials/minikql/mkql_node_builder.h>

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
    const auto flow = ctx.ProgramBuilder.ToFlow(input, {});

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

TRuntimeNode BuildWatermarkMetadataMapping(TCoAtomList metadataColumns, NCommon::TMkqlBuildContext& ctx) {
    TRuntimeNode::TList items;
    items.reserve(metadataColumns.Size() * 2);
    for (const auto& metadataColumn : metadataColumns) {
        const auto descriptor = GetPqMetaFieldDescriptorBySysColumn(
            metadataColumn.StringValue(),
            true);
        YQL_ENSURE(descriptor, "Unexpected pq metadata column: " << metadataColumn.StringValue());

        items.push_back(ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::String>(descriptor->SysColumn));
        items.push_back(ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::String>(TStringBuilder() << "__ydb_watermark_" << descriptor->Key));
    }

    return ctx.ProgramBuilder.NewList(ctx.ProgramBuilder.NewDataType(NUdf::EDataSlot::String), items);
}

} // anonymous namespace

void RegisterDqPqMkqlCompilers(NCommon::TMkqlCallableCompilerBase& compiler) {
    compiler.AddCallable(TDqPqPhyParsingWrap::CallableName(),
        [](const TExprNode& node, NCommon::TMkqlBuildContext& ctx) {
            TDqPqPhyParsingWrap pw(&node);

            const auto input = MkqlBuildExpr(pw.Input().Ref(), ctx);
            const auto lambda = [&](TRuntimeNode arg) {
                return MkqlBuildLambda(pw.Lambda().Ref(), ctx, {arg});
            };
            const auto metadataColumns = pw.MetadataColumns();

            const auto& lambdaArg = pw.Lambda().Args().Arg(0).Ref();
            const auto arg = ctx.ProgramBuilder.Arg(
                ctx.BuildType(lambdaArg, *lambdaArg.GetTypeAnn())
            );

            const auto metadataMapping = BuildWatermarkMetadataMapping(metadataColumns, ctx);

            TCallableBuilder callableBuilder(
                ctx.ProgramBuilder.GetTypeEnvironment(),
                "DqPqParsingWrapper",
                ctx.BuildType(node, *node.GetTypeAnn()));
            callableBuilder.Add(input);
            callableBuilder.Add(arg);
            callableBuilder.Add(lambda(arg));
            callableBuilder.Add(metadataMapping);
            return TRuntimeNode(callableBuilder.Build(), false);
        });

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
                auto flow = ctx.ProgramBuilder.ToFlow(input, {});
                return ctx.ProgramBuilder.ExpandMap(flow,
                    [&](TRuntimeNode item) -> TRuntimeNode::TList {
                        return {item};
                    });
            }

            return TRuntimeNode();
        });
}

}
