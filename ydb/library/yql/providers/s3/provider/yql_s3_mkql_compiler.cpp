#include "yql_s3_mkql_compiler.h"

#include <ydb/library/yql/providers/s3/expr_nodes/yql_s3_expr_nodes.h>
#include <ydb/library/yql/providers/common/mkql/parser.h>

#include <util/stream/str.h>

namespace NYql {

using namespace NKikimr::NMiniKQL;
using namespace NNodes;

namespace {


TRuntimeNode BuildSerializeCall(
    TRuntimeNode input,
    const std::string_view& format,
    const std::string_view& /*compression*/,
    TType* inputType,
    NCommon::TMkqlBuildContext& ctx)
{
    const auto inputItemType = AS_TYPE(TFlowType, inputType)->GetItemType();
    if (format == "raw") {
        const auto structType = AS_TYPE(TStructType, inputItemType);
        MKQL_ENSURE(1U == structType->GetMembersCount(), "Expected single column.");
        const auto schemeType = AS_TYPE(TDataType, structType->GetMemberType(0U))->GetSchemeType();
        return ctx.ProgramBuilder.Map(input,
            [&](TRuntimeNode item) {
                const auto member = ctx.ProgramBuilder.Member(item, structType->GetMemberName(0U));
                return NUdf::TDataType<const char*>::Id == schemeType ? member : ctx.ProgramBuilder.ToString(member);
            }
        );
    } else if (format == "json_list") {
        return ctx.ProgramBuilder.FlatMap(ctx.ProgramBuilder.SqueezeToList(input, ctx.ProgramBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui64>::Id)),
            [&ctx] (TRuntimeNode list) {
                const auto userType = ctx.ProgramBuilder.NewTupleType({ctx.ProgramBuilder.NewTupleType({list.GetStaticType()})});
                return ctx.ProgramBuilder.ToString(ctx.ProgramBuilder.Apply(ctx.ProgramBuilder.Udf("Yson2.SerializeJson"), {ctx.ProgramBuilder.Apply(ctx.ProgramBuilder.Udf("Yson2.From", {}, userType), {list})}));
            }
        );
    }

    const auto userType = ctx.ProgramBuilder.NewTupleType({ctx.ProgramBuilder.NewTupleType({ctx.ProgramBuilder.NewStreamType(inputItemType)})});
    return ctx.ProgramBuilder.ToFlow(ctx.ProgramBuilder.Apply(ctx.ProgramBuilder.Udf("ClickHouseClient.SerializeFormat", {}, userType, format), {ctx.ProgramBuilder.FromFlow(input)}));
}

TRuntimeNode SerializeForS3(const TS3SinkOutput& wrapper, NCommon::TMkqlBuildContext& ctx) {
    const auto input = MkqlBuildExpr(wrapper.Input().Ref(), ctx);
    const auto inputItemType = NCommon::BuildType(wrapper.Input().Ref(), *wrapper.Input().Ref().GetTypeAnn(), ctx.ProgramBuilder);
    return BuildSerializeCall(input, wrapper.Format().Value(), "TODO", inputItemType,  ctx);
}

}

void RegisterDqS3MkqlCompilers(NCommon::TMkqlCallableCompilerBase& compiler, const TS3State::TPtr&) {
    compiler.ChainCallable(TDqSourceWideWrap::CallableName(),
        [](const TExprNode& node, NCommon::TMkqlBuildContext& ctx) {
            if (const auto wrapper = TDqSourceWideWrap(&node); wrapper.DataSource().Category().Value() == S3ProviderName) {
                const auto wrapped = TryWrapWithParser(wrapper, ctx);
                if (wrapped) {
                    return *wrapped;
                }
            }

            return TRuntimeNode();
        });

    if (!compiler.HasCallable(TS3SinkOutput::CallableName()))
        compiler.AddCallable(TS3SinkOutput::CallableName(),
            [](const TExprNode& node, NCommon::TMkqlBuildContext& ctx) {
                return SerializeForS3(TS3SinkOutput(&node), ctx);
            });
}

}
