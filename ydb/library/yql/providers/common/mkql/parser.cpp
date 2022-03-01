#include "parser.h"
#include "util/generic/maybe.h"

#include <functional>
#include <string_view>
#include <ydb/library/yql/minikql/defs.h>

#include <library/cpp/json/json_writer.h>

namespace NYql {

using namespace NKikimr::NMiniKQL;
using namespace NNodes;

namespace {

const TExprNode& GetFormat(const TExprNode& settings) {
    for (auto i = 0U; i < settings.ChildrenSize(); ++i) {
        const auto& child = *settings.Child(i);
        if (child.Head().IsAtom("format"))
            return child.Tail();
    }
    THROW yexception() << "Unknown format.";
}

std::array<TString, 2U> GetSettings(const TExprNode& settings) {
    for (auto i = 0U; i < settings.ChildrenSize(); ++i) {
        const auto& child = *settings.Child(i);
        if (child.Head().IsAtom("settings")) {
            if (child.Tail().IsList()) {
                TString json, compression;
                TStringOutput stream(json);
                NJson::TJsonWriter writer(&stream, NJson::TJsonWriterConfig());
                writer.OpenMap();
                child.Tail().ForEachChild([&writer, &compression](const TExprNode& pair) {
                    if (pair.Head().IsAtom("compression") && pair.Tail().IsCallable({"String", "Utf8"}))
                        if (const auto& comp = pair.Tail().Head().Content(); !comp.empty())
                            compression = comp;
                        else {
                            writer.WriteKey(pair.Head().Content());
                            writer.Write(comp);
                        }
                    else if (pair.Head().IsAtom() && pair.Tail().IsCallable({"Bool", "Float", "Double", "Int8", "Uint8", "Int16", "Uint16", "Int32", "Uint32", "Int64", "Uint64", "String", "Utf8"})) {
                        writer.WriteKey(pair.Head().Content());
                        if (const auto& type = pair.Tail().Content(); "Bool" == type)
                            writer.Write(FromString<bool>(pair.Tail().Head().Content()));
                        else if ("Float" == type)
                            writer.Write(FromString<float>(pair.Tail().Head().Content()));
                        else if ("Double" == type)
                            writer.Write(FromString<double>(pair.Tail().Head().Content()));
                        else if ("Int8" == type)
                            writer.Write(FromString<i8>(pair.Tail().Head().Content()));
                        else if ("Uint8" == type)
                            writer.Write(FromString<ui8>(pair.Tail().Head().Content()));
                        else if ("Int16" == type)
                            writer.Write(FromString<i16>(pair.Tail().Head().Content()));
                        else if ("Uint16" == type)
                            writer.Write(FromString<ui16>(pair.Tail().Head().Content()));
                        else if ("Int32" == type)
                            writer.Write(FromString<i32>(pair.Tail().Head().Content()));
                        else if ("Uint32" == type)
                            writer.Write(FromString<ui32>(pair.Tail().Head().Content()));
                        else if ("Int64" == type)
                            writer.Write(FromString<i64>(pair.Tail().Head().Content()));
                        else if ("Uint64" == type)
                            writer.Write(FromString<ui64>(pair.Tail().Head().Content()));
                        else
                            writer.Write(pair.Tail().Head().Content());

                    }
                });
                writer.CloseMap();
                writer.Flush();
                if (json == "{}")
                    json.clear();
                return {std::move(json), std::move(compression)};
            }
        }
    }
    return {TString(), TString()};
}

TString ResolveUDFNameByCompression(std::string_view input) {
    if (input == "gzip"sv) {
        return "Gzip";
    }
    if (input == "zstd"sv) {
        return "Zstd";
    }
    if (input == "lz4"sv) {
        return "Lz4";
    }
    if (input == "brotli"sv) {
        return "Brotli";
    }
    if (input == "bzip2"sv) {
        return "BZip2";
    }
    if (input == "xz"sv) {
        return "Xz";
    }
    THROW yexception() << "Invalid compression: " << input;
}
} // namespace

TRuntimeNode BuildParseCall(
    TRuntimeNode input,
    const std::string_view& format,
    const std::string_view& compression,
    TType* inputItemType,
    TType* outputItemType,
    NCommon::TMkqlBuildContext& ctx)
{
    if (!compression.empty()) {
        input = ctx.ProgramBuilder.Map(input, [&ctx, &compression](TRuntimeNode item) {
            return ctx.ProgramBuilder.Apply(ctx.ProgramBuilder.Udf(std::string("Decompress.") += ResolveUDFNameByCompression(compression)), {item});
        });
    }

    const auto structType = static_cast<const TStructType*>(outputItemType);
    if (format == "raw") {
        MKQL_ENSURE(1U == structType->GetMembersCount(), "Expected single column.");
        bool isOptional;
        const auto schemeType = UnpackOptionalData(structType->GetMemberType(0U), isOptional)->GetSchemeType();
        return ctx.ProgramBuilder.ExpandMap(ctx.ProgramBuilder.ToFlow(input),
            [&](TRuntimeNode item)->TRuntimeNode::TList {
                return { NUdf::TDataType<const char*>::Id == schemeType ?
                    isOptional ? ctx.ProgramBuilder.NewOptional(item) : item :
                    (ctx.ProgramBuilder.*(isOptional ? &TProgramBuilder::FromString : &TProgramBuilder::StrictFromString))
                        (item, ctx.ProgramBuilder.NewDataType(schemeType, isOptional))
                };
            }
        );
    } else if (format == "json_list") {
        input = ctx.ProgramBuilder.FlatMap(ctx.ProgramBuilder.ToFlow(input),
            [&](TRuntimeNode blob) {
                const auto json = ctx.ProgramBuilder.StrictFromString(blob, ctx.ProgramBuilder.NewDataType(NUdf::TDataType<NUdf::TJson>::Id));
                const auto dom = ctx.ProgramBuilder.Apply(ctx.ProgramBuilder.Udf("Yson2.ParseJson"), {json});
                const auto userType = ctx.ProgramBuilder.NewTupleType({ctx.ProgramBuilder.NewTupleType({dom.GetStaticType()}), ctx.ProgramBuilder.NewStructType({}), ctx.ProgramBuilder.NewListType(outputItemType)});
                return ctx.ProgramBuilder.Apply(ctx.ProgramBuilder.Udf("Yson2.ConvertTo", {}, userType), {dom});
            });
    } else {
        const auto userType = ctx.ProgramBuilder.NewTupleType({ctx.ProgramBuilder.NewTupleType({inputItemType}), ctx.ProgramBuilder.NewStructType({}), outputItemType});
        input = ctx.ProgramBuilder.ToFlow(ctx.ProgramBuilder.Apply(ctx.ProgramBuilder.Udf("ClickHouseClient.ParseFormat", {}, userType, format), {input}));
    }

    return ctx.ProgramBuilder.ExpandMap(input,
        [&](TRuntimeNode item) {
            TRuntimeNode::TList fields;
            fields.reserve(structType->GetMembersCount());
            auto j = 0U;
            std::generate_n(std::back_inserter(fields), structType->GetMembersCount(), [&](){ return ctx.ProgramBuilder.Member(item, structType->GetMemberName(j++)); });
            return fields;
        });
}

NCommon::IMkqlCallableCompiler::TCompiler BuildCompiler(const std::string_view& providerName) {
    return [providerName](const TExprNode& node, NCommon::TMkqlBuildContext& ctx) {
        if (const auto wrapper = TDqSourceWideWrap(&node); wrapper.DataSource().Category().Value() == providerName) {
            const auto input = MkqlBuildExpr(wrapper.Input().Ref(), ctx);
            const auto inputItemType = NCommon::BuildType(wrapper.Input().Ref(), *wrapper.Input().Ref().GetTypeAnn(), ctx.ProgramBuilder);
            const auto outputItemType = NCommon::BuildType(wrapper.RowType().Ref(), *wrapper.RowType().Ref().GetTypeAnn()->Cast<TTypeExprType>()->GetType(), ctx.ProgramBuilder);
            const auto& settings = GetSettings(wrapper.Settings().Cast().Ref());
            return BuildParseCall(input, GetFormat(wrapper.Settings().Cast().Ref()).Content() + settings.front(), settings.back(), inputItemType, outputItemType, ctx);
        }

        return TRuntimeNode();
    };
}

TMaybe<TRuntimeNode> TryWrapWithParser(const TDqSourceWideWrap& wrapper, NCommon::TMkqlBuildContext& ctx) {
    const auto& format = GetFormat(wrapper.Settings().Cast().Ref());
    if (!format.Content()) {
        return TMaybe<TRuntimeNode>();
    }

    const auto input = MkqlBuildExpr(wrapper.Input().Ref(), ctx);
    const auto inputItemType = NCommon::BuildType(wrapper.Input().Ref(), *wrapper.Input().Ref().GetTypeAnn(), ctx.ProgramBuilder);
    const auto outputItemType = NCommon::BuildType(wrapper.RowType().Ref(), *wrapper.RowType().Ref().GetTypeAnn()->Cast<TTypeExprType>()->GetType(), ctx.ProgramBuilder);
    const auto& settings = GetSettings(wrapper.Settings().Cast().Ref());
    return BuildParseCall(input, format.Content() + settings.front(), settings.back(), inputItemType, outputItemType, ctx);
}

}
