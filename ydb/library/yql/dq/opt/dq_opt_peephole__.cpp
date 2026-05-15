#include "dq_opt_peephole.h"

#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>

#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_opt_utils.h>

#include <yql/essentials/providers/common/provider/yql_provider_names.h>

namespace NYql::NDq {

using namespace NYql::NNodes;

namespace {

TMaybeNode<TExprBase> FindSetting(TExprNode::TPtr settings, TStringBuf name) {
    const auto maybeSettingsList = TMaybeNode<TCoNameValueTupleList>(settings);
    if (!maybeSettingsList) {
        return nullptr;
    }
    const auto settingsList = maybeSettingsList.Cast();

    for (size_t i = 0; i < settingsList.Size(); ++i) {
        TCoNameValueTuple setting = settingsList.Item(i);
        if (setting.Name().Value() == name) {
            return setting.Value();
        }
    }
    return nullptr;
}

bool UseSharedReading(TExprNode::TPtr settings) {
    const auto maybeInnerSettings = FindSetting(settings, "settings");
    if (!maybeInnerSettings) {
        return false;
    }

    const auto maybeSharedReadingSetting = FindSetting(maybeInnerSettings.Cast().Ptr(), "SharedReading");
    if (!maybeSharedReadingSetting) {
        return false;
    }

    const TExprNode& value = maybeSharedReadingSetting.Cast().Ref();
    return value.IsAtom() && FromString<bool>(value.Content());
}

TMaybeNode<TExprBase> WrapSharedReading(const TDqSourceWrapBase& wrapper, TExprContext& ctx) {
    const auto arg = Build<TCoArgument>(ctx, wrapper.Pos())
        .Name("item")
        .Done();

    auto bodyBuilder = Build<TExprList>(ctx, wrapper.Pos());
    for (const auto& item : wrapper.RowType().Ref().ChildrenList()) {
        bodyBuilder.Add<TCoMember>()
            .Struct(arg)
            .Name(TCoAtom(item))
            .Build();
    }

    const auto lambda = Build<TCoLambda>(ctx, wrapper.Pos())
        .Args({arg})
        .Body(bodyBuilder.Done())
        .Done();

    return Build<TCoFlatMap>(ctx, wrapper.Pos())
        .Input<TCoToFlow>()
            .Input(wrapper.Input())
            .Build()
        .Lambda(lambda)
        .Done();
}

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

TMaybeNode<TExprBase> BuildParseCall(
    TPositionHandle pos,
    TExprBase input,
    TMaybeNode<TExprBase> extraColumns,
    std::unordered_map<TString, ui32>&& metadataColumns,
    std::string_view format,
    std::string_view compression,
    const std::vector<std::pair<std::string_view, std::string_view>>& formatSettings,
    TExprBase parseRowType,
    TExprBase finalRowType,
    TExprContext& ctx,
    const std::vector<TString>& csvHeaderlessColumnOrder
) {
    auto currentInput = input;

    if (!compression.empty()) {
        const auto arg = Build<TCoArgument>(ctx, pos)
            .Name("item")
            .Done();

        TExprBase payload = arg;
        if (arg.Ref().GetTypeAnn() && arg.Ref().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Tuple) {
            payload = Build<TCoNth>(ctx, pos)
                .Tuple(arg)
                .Index().Value("0").Build()
                .Done();
        }

        const auto decompress = Build<TCoApply>(ctx, pos)
            .Callable<TCoUdf>()
                .MethodName().Build(TStringBuilder() << "Decompress." << compression)
                .Build()
            .FreeArgs()
                .Add(payload)
                .Build()
            .Done();

        const auto lambda = Build<TCoLambda>(ctx, pos)
            .Args({arg})
            .Body(decompress)
            .Done();

        currentInput = Build<TCoMap>(ctx, pos)
            .Input(currentInput)
            .Lambda(lambda)
            .Done();
    }

    if (format == "raw") {
        const auto arg = Build<TCoArgument>(ctx, pos)
            .Name("item")
            .Done();

        TExprBase payload = arg;
        std::vector<TExprBase> extraTupleItems;
        if (extraColumns || !metadataColumns.empty()) {
            payload = Build<TCoNth>(ctx, pos)
                .Tuple(arg)
                .Index().Value("0").Build()
                .Done();

            const ui32 tailSize = (extraColumns ? 1U : 0U) + metadataColumns.size();
            for (ui32 i = 0; i < tailSize; ++i) {
                extraTupleItems.push_back(
                    Build<TCoNth>(ctx, pos)
                        .Tuple(arg)
                        .Index().Value(ToString(i + 1U)).Build()
                        .Done());
            }
        }

        auto body = Build<TCoAsStruct>(ctx, pos)
            .Add<TCoNameValueTuple>()
                .Name(parseRowType.Ref().Child(0))
                .Value(payload)
                .Build();

        for (const auto& item : extraTupleItems) {
            body.Add(item);
        }

        const auto lambda = Build<TCoLambda>(ctx, pos)
            .Args({arg})
            .Body(body.Done())
            .Done();

        currentInput = Build<TCoMap>(ctx, pos)
            .Input(currentInput)
            .Lambda(lambda)
            .Done();
    } else if (format == "json_list") {
        const auto arg = Build<TCoArgument>(ctx, pos)
            .Name("blob")
            .Done();

        TExprBase payload = arg;
        std::vector<TExprBase> tailItems;
        if (extraColumns || !metadataColumns.empty()) {
            payload = Build<TCoNth>(ctx, pos)
                .Tuple(arg)
                .Index().Value("0").Build()
                .Done();

            const ui32 tailSize = (extraColumns ? 1U : 0U) + metadataColumns.size();
            for (ui32 i = 0; i < tailSize; ++i) {
                tailItems.push_back(
                    Build<TCoNth>(ctx, pos)
                        .Tuple(arg)
                        .Index().Value(ToString(i + 1U)).Build()
                        .Done());
            }
        }

        const auto parseJson = Build<TCoApply>(ctx, pos)
            .Callable<TCoUdf>()
                .MethodName().Build("Yson2.ParseJson")
                .Build()
            .FreeArgs()
                .Add(payload)
                .Build()
            .Done();

        const auto convertTo = Build<TCoApply>(ctx, pos)
            .Callable<TCoUdf>()
                .MethodName().Build("Yson2.ConvertTo")
                .Build()
            .FreeArgs()
                .Add(parseJson)
                .Build()
            .Done();

        const auto listArg = Build<TCoArgument>(ctx, pos)
            .Name("parsed")
            .Done();

        auto tupleBuilder = Build<TExprList>(ctx, pos)
            .Add(listArg);
        for (const auto& item : tailItems) {
            tupleBuilder.Add(item);
        }

        const auto listMapLambda = Build<TCoLambda>(ctx, pos)
            .Args({listArg})
            .Body(tupleBuilder.Done())
            .Done();

        const auto body = tailItems.empty()
            ? TExprBase(convertTo.Ptr())
            : Build<TCoMap>(ctx, pos)
                .Input(convertTo)
                .Lambda(listMapLambda)
                .Done();

        const auto lambda = Build<TCoLambda>(ctx, pos)
            .Args({arg})
            .Body(body)
            .Done();

        currentInput = Build<TCoFlatMap>(ctx, pos)
            .Input(currentInput)
            .Lambda(lambda)
            .Done();
    } else {
        TString settingsAsJson;
        TStringOutput stream(settingsAsJson);
        NJson::TJsonWriter writer(&stream, NJson::TJsonWriterConfig());
        writer.OpenMap();
        for (const auto& v : formatSettings) {
            writer.Write(v.first, v.second);
        }
        const bool csvVirtualHeader = !csvHeaderlessColumnOrder.empty();
        if (csvVirtualHeader) {
            writer.Write("with_names_use_header", false);
            writer.Write("empty_as_default", true);
            writer.WriteKey("columns_list");
            writer.OpenArray();
            for (const auto& col : csvHeaderlessColumnOrder) {
                writer.Write(col);
            }
            writer.CloseArray();
        }
        writer.CloseMap();
        writer.Flush();
        if (settingsAsJson == "{}") {
            settingsAsJson.clear();
        }

        TString formatForCh(format);
        if (csvVirtualHeader && formatForCh == "csv") {
            formatForCh = "csv_with_names";
        }

        currentInput = Build<TCoApply>(ctx, pos)
            .Callable<TCoUdf>()
                .MethodName().Build(TStringBuilder() << "ClickHouseClient." << (format == "blocks" ? "ParseBlocks" : "ParseFormat"))
                .Build()
            .FreeArgs()
                .Add(currentInput)
                .Build()
            .Done();
    }

    const auto arg = Build<TCoArgument>(ctx, pos)
        .Name("item")
        .Done();

    TExprBase parsedData = (extraColumns || !metadataColumns.empty())
        ? Build<TCoNth>(ctx, pos)
            .Tuple(arg)
            .Index().Value("0").Build()
            .Done()
        : TExprBase(arg.Ptr());

    TMaybeNode<TExprBase> extra;
    if (extraColumns) {
        extra = Build<TCoLookup>(ctx, pos)
            .Collection(extraColumns.Cast())
            .Lookup(Build<TCoNth>(ctx, pos)
                .Tuple(arg)
                .Index().Value("1").Build()
                .Done())
            .Done();
    }

    auto bodyBuilder = Build<TExprList>(ctx, pos);
    for (const auto& item : finalRowType.Ref().ChildrenList()) {
        const auto name = TCoAtom(item).StringValue();
        const auto metadataIter = metadataColumns.find(name);
        if (metadataIter != metadataColumns.end()) {
            bodyBuilder.Add<TCoNth>()
                .Tuple(arg)
                .Index().Value(ToString(metadataIter->second)).Build()
                .Build();
        } else {
            bool foundInParsed = false;
            for (const auto& parsedItem : parseRowType.Ref().ChildrenList()) {
                if (TCoAtom(parsedItem).StringValue() == name) {
                    foundInParsed = true;
                    break;
                }
            }

            if (foundInParsed) {
                bodyBuilder.Add<TCoMember>()
                    .Struct(parsedData)
                    .Name().Value(name).Build()
                    .Build();
            } else if (extra) {
                bodyBuilder.Add<TCoMember>()
                    .Struct(extra.Cast())
                    .Name().Value(name).Build()
                    .Build();
            }
        }
    }

    const auto lambda = Build<TCoLambda>(ctx, pos)
        .Args({arg})
        .Body(bodyBuilder.Done())
        .Done();

    return Build<TCoFlatMap>(ctx, pos)
        .Input(currentInput)
        .Lambda(lambda)
        .Done();
}

TMaybeNode<TExprBase> TryWrapWithParser(const TDqSourceWrapBase& wrapper, TExprContext& ctx) {
    const auto& format = GetFormat(wrapper.Settings().Cast().Ref());
    if (!format.Content()) {
        return {};
    }

    const TStructExprType* rowType = wrapper.RowType().Ref().GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TStructExprType>();
    const TStructExprType* parsedType = rowType;

    TMaybeNode<TExprBase> extraColumns;
    const TStructExprType* extraType = nullptr;
    if (auto extraColumnsSetting = GetSetting(wrapper.Settings().Cast().Ref(), "extraColumns")) {
        extraColumns = TExprBase(extraColumnsSetting->TailPtr());
        extraType = extraColumnsSetting->Tail().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
        YQL_ENSURE(extraType->GetItems(), "Extra column type must not be an empty struct");
    }

    std::unordered_map<TString, ui32> metadataColumns;
    if (auto metadataSetting = GetSetting(wrapper.Settings().Cast().Ref(), "metadataColumns")) {
        for (auto i = 0U; i < metadataSetting->Tail().ChildrenSize(); i++) {
            const auto name = TCoAtom(metadataSetting->Tail().Child(i)).StringValue();
            metadataColumns.emplace(name, i + (extraColumns ? 2 : 1));
        }
    }

    std::vector<std::pair<std::string_view, std::string_view>> formatSettings;
    std::vector<TString> csvHeaderlessColumns;
    if (auto settings = GetSetting(wrapper.Settings().Cast().Ref(), "formatSettings")) {
        settings->Tail().ForEachChild([&](const TExprNode& v) {
            const auto key = v.Child(0)->Content();
            const auto& valNode = *v.Child(1);
            if ("UserSchemaColumns" == key && valNode.IsList()) {
                csvHeaderlessColumns.reserve(valNode.ChildrenSize());
                for (ui32 i = 0; i < valNode.ChildrenSize(); ++i) {
                    YQL_ENSURE(valNode.Child(i)->IsAtom(), "UserSchemaColumns must be a list of atoms");
                    csvHeaderlessColumns.push_back(TString(valNode.Child(i)->Content()));
                }
            } else {
                YQL_ENSURE(valNode.IsAtom(), "Expected atom format setting value");
                formatSettings.emplace_back(key, valNode.Content());
            }
        });
    }

    auto parsedItems = rowType->GetItems();
    EraseIf(parsedItems, [extraType, &metadataColumns](const auto& item) {
        return extraType && extraType->FindItem(item->GetName()) || metadataColumns.contains(TString(item->GetName()));
    });
    parsedType = ctx.MakeType<TStructExprType>(parsedItems);

    const auto& settings = GetSettings(wrapper.Settings().Cast().Ref());

    return BuildParseCall(
        wrapper.Pos(),
        wrapper.Input(),
        extraColumns,
        std::move(metadataColumns),
        format.Content() + settings.front(),
        settings.back(),
        formatSettings,
        TExprBase(ExpandType(wrapper.Pos(), *parsedType, ctx)),
        wrapper.RowType(),
        ctx,
        csvHeaderlessColumns
    );
}

[[maybe_unused]] TExprBase WrapWithWatermarkGenerator(
    TExprBase input,
    TCoLambda watermarkExpr,
    TCoNameValueTupleList watermarkSettings,
    TExprContext& ctx
) {
    const auto pos = input.Pos();

    auto watermarkSettingsBuilder = Build<TCoNameValueTupleList>(ctx, pos);
    for (const auto& nameValue : watermarkSettings) {
        if (const auto name = nameValue.Name().Value();
            "WatermarksLateArrivalDelayUs" == name) {
            watermarkSettingsBuilder.Add<TCoNameValueTuple>().InitFrom(nameValue).Build();
        } else if ("WatermarksGranularityUs" == name) {
            watermarkSettingsBuilder.Add<TCoNameValueTuple>().InitFrom(nameValue).Build();
        } else if ("WatermarksIdleTimeoutUs" == name) {
            watermarkSettingsBuilder.Add<TCoNameValueTuple>().InitFrom(nameValue).Build();
        }
    }

    return Build<TCoFromFlow>(ctx, pos)
        .Input<TDqPhyWatermarkGenerator>()
            .Input<TCoToFlow>()
                .Input(input)
                .Build()
            .WatermarkExtractor(watermarkExpr.Ptr())
            .PartitionIdExtractor<TCoLambda>()
                .Args({"arg"})
                .Body<TCoMember>()
                    .Struct("arg")
                    .Name().Build("_yql_sys_partition_id")
                    .Build()
                .Build()
            .WatermarkSettings(watermarkSettingsBuilder.Done().Ptr())
            .Build()
        .Done();
}

} // anonymous namespace

TExprBase DqPeepholeTryRewriteSourceWrapToParsing(const TExprBase& node, TExprContext& ctx) {
    const auto maybeWrapper = node.Maybe<TDqSourceWideWrap>();
    if (!maybeWrapper) {
        return node;
    }
    const auto wrapper = maybeWrapper.Cast();

    if (PqProviderName != wrapper.DataSource().Category().Value()) {
        return node;
    }

    const auto maybeSettings = wrapper.Settings();
    if (maybeSettings) {
        const auto settings = maybeSettings.Cast();
        if (UseSharedReading(settings.Ptr())) {
            if (const auto wrapped = WrapSharedReading(wrapper, ctx)) {
                return wrapped.Cast();
            }
        }
    }

    if (const auto maybeWrapped = TryWrapWithParser(wrapper, ctx)) {
        const auto wrapped = maybeWrapped.Cast();
        const auto pqSource = wrapper.Input().Raw();
        if (const auto watermarkExpr = pqSource->Child(7)) { // TDqPqTopicSource::idx_WatermarkExpr
            const auto watermarkSettings = pqSource->Child(3); // TDqPqTopicSource::idx_Settings
            return WrapWithWatermarkGenerator(wrapped, TCoLambda(watermarkExpr), TCoNameValueTupleList(watermarkSettings), ctx);
        }

        return wrapped;
    }

    const auto arg = Build<TCoArgument>(ctx, wrapper.Pos())
        .Name("item")
        .Done();

    const auto lambda = Build<TCoLambda>(ctx, wrapper.Pos())
        .Args({arg})
        .Body<TExprList>()
            .Add(arg)
            .Build()
        .Done();

    return Build<TCoFlatMap>(ctx, wrapper.Pos())
        .Input<TCoToFlow>()
            .Input(wrapper.Input())
            .Build()
        .Lambda(lambda)
        .Done();
}

} // namespace NYql::NDq
