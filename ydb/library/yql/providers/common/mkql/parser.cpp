#include "parser.h"
#include "util/generic/maybe.h"

#include <functional>
#include <string_view>
#include <ydb/library/yql/minikql/defs.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>

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
                    if (pair.Head().IsAtom("compression"))
                        if (const auto& comp = pair.Tail().Content(); !comp.empty())
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

TRuntimeNode WrapWithDecompress(
    TRuntimeNode input,
    const TType* inputItemType,
    const std::string_view& compression,
    NCommon::TMkqlBuildContext& ctx)
{
    // If input has one field, this field is data
    if (!inputItemType->IsTuple()) {
        return ctx.ProgramBuilder.Map(input, [&ctx, &compression](TRuntimeNode item) {
            return ctx.ProgramBuilder.Apply(
                ctx.ProgramBuilder.Udf(std::string("Decompress.") += ResolveUDFNameByCompression(compression)),
                {item});
        });
    }

    // If input has multiple fields, decompress only "data" field.
    const auto* inputItemTuple = static_cast<const TTupleType*>(inputItemType);
    return ctx.ProgramBuilder.Map(input, [&](TRuntimeNode item) {
        const auto dataMember = ctx.ProgramBuilder.Nth(item, 0);
        const auto decompress = ctx.ProgramBuilder.Apply(
            ctx.ProgramBuilder.Udf(std::string("Decompress.") += ResolveUDFNameByCompression(compression)),
            {dataMember});

        std::vector<TRuntimeNode> res;
        res.emplace_back(decompress);
        for (auto i = 1U; i < inputItemTuple->GetElementsCount(); i++) {
            res.emplace_back(ctx.ProgramBuilder.Nth(item, i));
        }

        return ctx.ProgramBuilder.NewTuple(res);
    });
}
} // namespace


TRuntimeNode BuildParseCall(
    TPosition pos,
    TRuntimeNode input,
    TMaybe<TRuntimeNode> extraColumnsByPathIndex,
    std::unordered_map<TString, ui32>&& metadataColumns,
    const std::string_view& format,
    const std::string_view& compression,
    const std::vector<std::pair<std::string_view, std::string_view>>& formatSettings,
    TType* inputType,
    TType* parseItemType,
    TType* finalItemType,
    NCommon::TMkqlBuildContext& ctx,
    bool useBlocks)
{
    const auto* inputItemType = static_cast<TStreamType*>(inputType)->GetItemType();
    const auto* parseItemStructType = static_cast<TStructType*>(parseItemType);
    const auto* finalItemStructType = static_cast<TStructType*>(finalItemType);

    if (useBlocks) {
        return ctx.ProgramBuilder.BlockExpandChunked(ctx.ProgramBuilder.ExpandMap(
            ctx.ProgramBuilder.ToFlow(input), [&](TRuntimeNode item) {
                auto parsedData = (extraColumnsByPathIndex || !metadataColumns.empty())
                                      ? ctx.ProgramBuilder.Nth(item, 0)
                                      : item;

                TMaybe<TRuntimeNode> extra;
                if (extraColumnsByPathIndex) {
                    auto pathInd   = ctx.ProgramBuilder.Nth(item, 1);
                    auto extraNode = ctx.ProgramBuilder.Lookup(
                        ctx.ProgramBuilder.ToIndexDict(*extraColumnsByPathIndex), pathInd);
                    extra = ctx.ProgramBuilder.Unwrap(
                        extraNode,
                        ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::String>(
                            "Failed to lookup path index"),
                        pos.File,
                        pos.Row,
                        pos.Column);
                }

                auto blockLengthName =
                    ctx.ProgramBuilder.Member(parsedData, BlockLengthColumnName);
                TRuntimeNode::TList fields;
                fields.reserve(finalItemStructType->GetMembersCount());

                for (ui32 i = 0; i < finalItemStructType->GetMembersCount(); ++i) {
                    TStringBuf name         = finalItemStructType->GetMemberName(i);
                    const auto metadataIter = metadataColumns.find(TString(name));
                    if (metadataIter != metadataColumns.end()) {
                        fields.push_back(ctx.ProgramBuilder.ReplicateScalar(
                            ctx.ProgramBuilder.AsScalar(
                                ctx.ProgramBuilder.Nth(item, metadataIter->second)),
                            blockLengthName));
                    } else if (parseItemStructType->FindMemberIndex(name).Defined()) {
                        fields.push_back(ctx.ProgramBuilder.Member(parsedData, name));
                    } else {
                        MKQL_ENSURE(extra, "Column " << name << " wasn't found");
                        fields.push_back(ctx.ProgramBuilder.ReplicateScalar(
                            ctx.ProgramBuilder.AsScalar(
                                ctx.ProgramBuilder.Member(*extra, name)),
                            blockLengthName));
                    }
                }

                fields.push_back(blockLengthName);
                return fields;
            }));
    }

    if (!compression.empty()) {
        input = WrapWithDecompress(input, inputItemType, compression, ctx);
    }

    if (format == "raw") {
        auto parseLambda = [&](TRuntimeNode item) {
            if (parseItemStructType->GetMembersCount() == 0) {
                return ctx.ProgramBuilder.NewStruct(parseItemType, {});
            }
            MKQL_ENSURE(parseItemStructType->GetMembersCount() == 1, "Only one column in schema supported in raw format");

            bool isOptional;
            const auto schemeType = UnpackOptionalData(
                parseItemStructType->GetMemberType(0U), isOptional)->GetSchemeType();

            TRuntimeNode converted;
            if (NUdf::TDataType<const char*>::Id == schemeType) {
                converted = isOptional ? ctx.ProgramBuilder.NewOptional(item) : item;
            } else {
                auto type = ctx.ProgramBuilder.NewDataType(schemeType, isOptional);
                converted = isOptional ? ctx.ProgramBuilder.FromString(item, type) :
                                         ctx.ProgramBuilder.StrictFromString(item, type);
            }

            return ctx.ProgramBuilder.NewStruct(parseItemType, {{parseItemStructType->GetMemberName(0), converted }});
        };

        input = ctx.ProgramBuilder.Map(ctx.ProgramBuilder.ToFlow(input),
            [&](TRuntimeNode item) {
                std::vector<TRuntimeNode> res;

                if (extraColumnsByPathIndex || !metadataColumns.empty()) {
                    auto data = ctx.ProgramBuilder.Nth(item, 0);
                    res.emplace_back(parseLambda(data));
                    if (extraColumnsByPathIndex) {
                        res.emplace_back(ctx.ProgramBuilder.Nth(item, res.size()));
                    }
                    for (auto i = 0U; i < metadataColumns.size(); i++) {
                        res.emplace_back(ctx.ProgramBuilder.Nth(item, res.size()));
                    }
                    return ctx.ProgramBuilder.NewTuple(res);
                }
                return parseLambda(item);
            }
        );
    } else if (format == "json_list") {
        auto parseToListLambda = [&](TRuntimeNode blob) {
            const auto json = ctx.ProgramBuilder.StrictFromString(
                blob,
                ctx.ProgramBuilder.NewDataType(NUdf::TDataType<NUdf::TJson>::Id));
            const auto dom = ctx.ProgramBuilder.Apply(
                ctx.ProgramBuilder.Udf("Yson2.ParseJson"),
                {json});
            const auto userType = ctx.ProgramBuilder.NewTupleType({
                ctx.ProgramBuilder.NewTupleType({dom.GetStaticType()}),
                ctx.ProgramBuilder.NewStructType({}),
                ctx.ProgramBuilder.NewListType(parseItemType)});
            return ctx.ProgramBuilder.Apply(
                ctx.ProgramBuilder.Udf("Yson2.ConvertTo", {}, userType),
                {dom});
        };

        input = ctx.ProgramBuilder.FlatMap(ctx.ProgramBuilder.ToFlow(input),
            [&](TRuntimeNode blob) {
                TRuntimeNode parsedList;
                if (extraColumnsByPathIndex || !metadataColumns.empty()) {
                    auto data = ctx.ProgramBuilder.Nth(blob, 0);

                    parsedList = ctx.ProgramBuilder.Map(parseToListLambda(data),
                        [&](TRuntimeNode item) {
                            std::vector<TRuntimeNode> res;
                            res.emplace_back(item);
                            if (extraColumnsByPathIndex) {
                                res.emplace_back(ctx.ProgramBuilder.Nth(blob, res.size()));
                            }
                            for (auto i = 0U; i < metadataColumns.size(); i++) {
                                res.emplace_back(ctx.ProgramBuilder.Nth(blob, res.size()));
                            }
                            return ctx.ProgramBuilder.NewTuple(res);
                        }
                    );
                } else {
                    parsedList = parseToListLambda(blob);
                }
                return parsedList;
            });
    } else {
        TType* userOutputType = parseItemType;
        const TType* inputDataType = inputItemType;

        if (extraColumnsByPathIndex || !metadataColumns.empty()) {
            const auto* inputItemTuple = static_cast<const TTupleType*>(inputItemType);

            std::vector<TType*> tupleItems;
            tupleItems.reserve(inputItemTuple->GetElementsCount());

            tupleItems.emplace_back(userOutputType);
            for (auto i = 1U; i < inputItemTuple->GetElementsCount(); i++) {
                tupleItems.emplace_back(inputItemTuple->GetElementType(i));
            }

            userOutputType = ctx.ProgramBuilder.NewTupleType(tupleItems);
            inputDataType = inputItemTuple->GetElementType(0);
        }

        TString settingsAsJson;
        TStringOutput stream(settingsAsJson);
        NJson::TJsonWriter writer(&stream, NJson::TJsonWriterConfig());
        writer.OpenMap();

        for (const auto& v : formatSettings) {
            writer.Write(v.first, v.second);
        }

        writer.CloseMap();
        writer.Flush();
        if (settingsAsJson == "{}") {
            settingsAsJson.clear();
        }

        const auto userType = ctx.ProgramBuilder.NewTupleType({
            ctx.ProgramBuilder.NewTupleType({inputType}),
            ctx.ProgramBuilder.NewStructType({}),
            userOutputType});
        input = TType::EKind::Resource == inputDataType->GetKind() ?
            ctx.ProgramBuilder.ToFlow(ctx.ProgramBuilder.Apply(ctx.ProgramBuilder.Udf("ClickHouseClient.ParseBlocks", {}, userType), {input})):
            ctx.ProgramBuilder.ToFlow(ctx.ProgramBuilder.Apply(ctx.ProgramBuilder.Udf("ClickHouseClient.ParseFormat", {}, userType, format + settingsAsJson), {input}));
    }

    return ctx.ProgramBuilder.ExpandMap(input,
        [&](TRuntimeNode item) {
            auto parsedData = (extraColumnsByPathIndex || !metadataColumns.empty())
                ? ctx.ProgramBuilder.Nth(item, 0)
                : item;

            TMaybe<TRuntimeNode> extra;
            if (extraColumnsByPathIndex) {
                auto pathInd = ctx.ProgramBuilder.Nth(item, 1);
                auto extraNode = ctx.ProgramBuilder.Lookup(ctx.ProgramBuilder.ToIndexDict(*extraColumnsByPathIndex), pathInd);
                extra = ctx.ProgramBuilder.Unwrap(extraNode,
                    ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::String>("Failed to lookup path index"),
                    pos.File, pos.Row, pos.Column);
            }

            TRuntimeNode::TList fields;
            fields.reserve(finalItemStructType->GetMembersCount());

            for (ui32 i = 0; i < finalItemStructType->GetMembersCount(); ++i) {
                TStringBuf name = finalItemStructType->GetMemberName(i);
                const auto metadataIter = metadataColumns.find(TString(name));
                if (metadataIter != metadataColumns.end()) {
                    fields.push_back(ctx.ProgramBuilder.Nth(item, metadataIter->second));
                } else if (parseItemStructType->FindMemberIndex(name).Defined()) {
                    fields.push_back(ctx.ProgramBuilder.Member(parsedData, name));
                } else {
                    MKQL_ENSURE(extra, "Column " << name << " wasn't found");
                    fields.push_back(ctx.ProgramBuilder.Member(*extra, name));
                }
            }
            return fields;
        }
    );
}

TMaybe<TRuntimeNode> TryWrapWithParser(const TDqSourceWrapBase& wrapper, NCommon::TMkqlBuildContext& ctx, bool useBlocks) {
    const auto& format = GetFormat(wrapper.Settings().Cast().Ref());
    if (!format.Content()) {
        return TMaybe<TRuntimeNode>();
    }

    const auto input = MkqlBuildExpr(wrapper.Input().Ref(), ctx);
    const auto inputType = NCommon::BuildType(
        wrapper.Input().Ref(),
        *wrapper.Input().Ref().GetTypeAnn(),
        ctx.ProgramBuilder);

    const TStructExprType* rowType = wrapper.RowType().Ref().GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TStructExprType>();
    const TStructExprType* parsedType = rowType;

    TMaybe<TRuntimeNode> extraColumns;
    const TStructExprType* extraType = nullptr;
    if (auto extraColumnsSetting = GetSetting(wrapper.Settings().Cast().Ref(), "extraColumns")) {
        extraColumns = MkqlBuildExpr(extraColumnsSetting->Tail(), ctx);
        extraType = extraColumnsSetting->Tail().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
        MKQL_ENSURE(extraType->GetItems(), "Extra column type must not be an empty struct");
    }

    std::unordered_map<TString, ui32> metadataColumns;
    if (auto metadataSetting = GetSetting(wrapper.Settings().Cast().Ref(), "metadataColumns")) {
        for (auto i = 0U; i < metadataSetting->Tail().ChildrenSize(); i++) {
            const auto name = TCoAtom(metadataSetting->Tail().Child(i)).StringValue();
            metadataColumns.emplace(name, i + (extraColumns ? 2 : 1));
        }
    }

    std::vector<std::pair<std::string_view, std::string_view>> formatSettings;
    if (auto settings = GetSetting(wrapper.Settings().Cast().Ref(), "formatSettings")) {
        settings->Tail().ForEachChild([&](const TExprNode& v) {
            formatSettings.emplace_back(v.Child(0)->Content(), v.Child(1)->Content());
        });
    }

    auto parsedItems = rowType->GetItems();
    EraseIf(parsedItems, [extraType, &metadataColumns](const auto& item) {
        return extraType && extraType->FindItem(item->GetName()) || metadataColumns.contains(TString(item->GetName()));
    });
    parsedType = ctx.ExprCtx.MakeType<TStructExprType>(parsedItems);

    const auto parseItemType = NCommon::BuildType(
        wrapper.RowType().Ref(),
        *parsedType,
        ctx.ProgramBuilder);
    const auto finalItemType = NCommon::BuildType(
        wrapper.RowType().Ref(),
        *rowType,
        ctx.ProgramBuilder);

    const auto& settings = GetSettings(wrapper.Settings().Cast().Ref());
    TPosition pos = ctx.ExprCtx.GetPosition(wrapper.Pos());

    return BuildParseCall(
        pos,
        input,
        extraColumns,
        std::move(metadataColumns),
        format.Content() + settings.front(),
        settings.back(),
        formatSettings,
        inputType,
        parseItemType,
        finalItemType,
        ctx,
        useBlocks);
}

TMaybe<TRuntimeNode> TryWrapWithParserForArrowIPCStreaming(const TDqSourceWrapBase& wrapper, NCommon::TMkqlBuildContext& ctx) {
    const auto input = MkqlBuildExpr(wrapper.Input().Ref(), ctx);
    const TStructExprType* rowType = wrapper.RowType().Ref().GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TStructExprType>();

    const auto finalItemType = NCommon::BuildType(
        wrapper.RowType().Ref(),
        *rowType,
        ctx.ProgramBuilder);

    const auto* finalItemStructType = static_cast<TStructType*>(finalItemType);

    return ctx.ProgramBuilder.ExpandMap(ctx.ProgramBuilder.ToFlow(input), [&](TRuntimeNode item) {
        // MKQL_ENSURE(!extraColumnsByPathIndex && metadataColumns.empty(), "TODO");

        TRuntimeNode::TList fields;

        for (ui32 i = 0; i < finalItemStructType->GetMembersCount(); ++i) {
            TStringBuf name = finalItemStructType->GetMemberName(i);
            fields.push_back(ctx.ProgramBuilder.Member(item, name));
        }

        fields.push_back(ctx.ProgramBuilder.Member(item, BlockLengthColumnName));
        return fields;
    });
}

}
