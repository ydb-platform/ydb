#include "yql_s3_mkql_compiler.h"

#include <ydb/library/yql/providers/s3/expr_nodes/yql_s3_expr_nodes.h>
#include <ydb/library/yql/providers/common/mkql/parser.h>

#include <library/cpp/json/json_writer.h>
#include <util/stream/str.h>

namespace NYql {

using namespace NKikimr::NMiniKQL;
using namespace NNodes;

namespace {

TRuntimeNode BuildSerializeCall(
    TRuntimeNode input,
    const std::vector<std::string_view>& keys,
    const std::string_view& format,
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

    TString settings;
    if (!keys.empty()) {
        const std::unordered_set<std::string_view> set(keys.cbegin(), keys.cend());
        const auto structType = AS_TYPE(TStructType, inputItemType);
        MKQL_ENSURE(set.size() < structType->GetMembersCount(), "Expected non key columns.");
        std::vector<std::pair<std::string_view, TType*>> types(structType->GetMembersCount());
        const auto keyType = ctx.ProgramBuilder.NewDataType(NUdf::TDataType<NUdf::TUtf8>::Id);
        for (auto i = 0U; i < types.size(); ++i) {
            const auto& name = structType->GetMemberName(i);
            types[i].first = name;
            types[i].second = set.contains(name) ? keyType : structType->GetMemberType(i);
        }

        if (const auto newStructType = static_cast<TStructType*>(ctx.ProgramBuilder.NewStructType(types)); !newStructType->IsSameType(*structType)) {
            input = ctx.ProgramBuilder.Map(input,
                [&](TRuntimeNode item) {
                    std::vector<std::pair<std::string_view, TRuntimeNode>> members(types.size());
                    for (auto i = 0U; i < members.size(); ++i) {
                        const auto& name = members[i].first = types[i].first;
                        members[i].second = ctx.ProgramBuilder.Member(item, name);
                        if (const auto oldType = structType->GetMemberType(i); !newStructType->GetMemberType(i)->IsSameType(*oldType)) {
                            const auto dataType = AS_TYPE(TDataType, oldType);
                            members[i].second = dataType->GetSchemeType() == NUdf::TDataType<const char*>::Id ?
                                ctx.ProgramBuilder.StrictFromString(members[i].second, newStructType->GetMemberType(i)):
                                ctx.ProgramBuilder.ToString<true>(members[i].second);
                        }
                    }

                    return ctx.ProgramBuilder.NewStruct(newStructType, members);
                }
            );
        }

        TStringOutput stream(settings);
        NJson::TJsonWriter writer(&stream, NJson::TJsonWriterConfig());
        writer.OpenMap();
            writer.WriteKey("keys");
            writer.OpenArray();
                std::for_each(keys.cbegin(), keys.cend(), [&writer](const std::string_view& key){ writer.Write(key); });
            writer.CloseArray();
        writer.CloseMap();
        writer.Flush();
    }

    input = ctx.ProgramBuilder.FromFlow(input);
    const auto userType = ctx.ProgramBuilder.NewTupleType({ctx.ProgramBuilder.NewTupleType({input.GetStaticType()})});
    return ctx.ProgramBuilder.ToFlow(ctx.ProgramBuilder.Apply(ctx.ProgramBuilder.Udf("ClickHouseClient.SerializeFormat", {}, userType, format + settings), {input}));
}

TRuntimeNode SerializeForS3(const TS3SinkOutput& wrapper, NCommon::TMkqlBuildContext& ctx) {
    const auto input = MkqlBuildExpr(wrapper.Input().Ref(), ctx);
    const auto inputItemType = NCommon::BuildType(wrapper.Input().Ref(), *wrapper.Input().Ref().GetTypeAnn(), ctx.ProgramBuilder);
    std::vector<std::string_view> keys;
    keys.reserve(wrapper.KeyColumns().Size());
    wrapper.KeyColumns().Ref().ForEachChild([&](const TExprNode& key){ keys.emplace_back(key.Content()); });
    return BuildSerializeCall(input, keys, wrapper.Format().Value(), inputItemType,  ctx);
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
