#include "yql_yt_mkql_compiler.h"
#include "yql_yt_helpers.h"
#include "yql_yt_op_settings.h"

#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <ydb/library/yql/providers/yt/lib/row_spec/yql_row_spec.h>
#include <ydb/library/yql/providers/yt/lib/skiff/yql_skiff_schema.h>
#include <ydb/library/yql/providers/yt/lib/mkql_helpers/mkql_helpers.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/common/codec/yql_codec_type_flags.h>
#include <ydb/library/yql/providers/common/mkql/yql_type_mkql.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/defs.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/utils/log/context.h>

#include <library/cpp/yson/node/node_io.h>
#include <yt/cpp/mapreduce/common/helpers.h>

#include <util/generic/yexception.h>
#include <util/generic/xrange.h>
#include <util/string/cast.h>

namespace NYql {

using namespace NKikimr;
using namespace NKikimr::NMiniKQL;
using namespace NNodes;

TRuntimeNode BuildTableContentCall(TStringBuf callName,
    TType* outItemType,
    TStringBuf clusterName,
    const TExprNode& input,
    const TMaybe<ui64>& itemsCount,
    NCommon::TMkqlBuildContext& ctx,
    bool forceColumns,
    const THashSet<TString>& extraSysColumns,
    bool forceKeyColumns)
{
    forceColumns = forceColumns || forceKeyColumns;
    TType* const strType = ctx.ProgramBuilder.NewDataType(NUdf::TDataType<char*>::Id);
    TType* const boolType = ctx.ProgramBuilder.NewDataType(NUdf::TDataType<bool>::Id);
    TType* const ui64Type = ctx.ProgramBuilder.NewDataType(NUdf::TDataType<ui64>::Id);
    TType* const ui32Type = ctx.ProgramBuilder.NewDataType(NUdf::TDataType<ui32>::Id);
    TType* const tupleTypeTables = ctx.ProgramBuilder.NewTupleType({strType, boolType, strType, ui64Type, ui64Type, boolType, ui32Type});
    TType* const listTypeGroup = ctx.ProgramBuilder.NewListType(tupleTypeTables);

    const TExprNode* settings = nullptr;
    TMaybe<TSampleParams> sampling;
    TVector<TRuntimeNode> groups;
    if (input.IsCallable(TYtOutput::CallableName())) {
        YQL_ENSURE(!forceKeyColumns);
        auto outTableInfo = TYtOutTableInfo(GetOutTable(TExprBase(&input)));
        YQL_ENSURE(outTableInfo.Stat, "Table " << outTableInfo.Name.Quote() << " has no Stat");
        auto richYPath = NYT::TRichYPath(outTableInfo.Name);
        if (forceColumns && outTableInfo.RowSpec->HasAuxColumns()) {
            NYT::TSortColumns columns;
            for (auto& item: outTableInfo.RowSpec->GetType()->GetItems()) {
                columns.Add(TString{item->GetName()});
            }
            richYPath.Columns(columns);
        }
        TString spec;
        if (!extraSysColumns.empty()) {
            auto specNode = outTableInfo.GetCodecSpecNode();
            auto structType = AS_TYPE(TStructType, outItemType);
            NYT::TNode columns;
            for (auto& col: extraSysColumns) {
                const auto fullName = TString(YqlSysColumnPrefix).append(col);
                if (!structType->FindMemberIndex(fullName)) {
                    columns.Add(col);
                    outItemType = ctx.ProgramBuilder.NewStructType(outItemType, fullName, ctx.ProgramBuilder.NewDataType(GetSysColumnTypeId(col)));
                }
            }
            if (!columns.IsUndefined()) {
                specNode[YqlSysColumnPrefix] = std::move(columns);
            }
            spec = NYT::NodeToCanonicalYsonString(specNode);
        } else {
            spec = outTableInfo.RowSpec->ToYsonString();
        }
        groups.push_back(
            ctx.ProgramBuilder.NewList(tupleTypeTables, {ctx.ProgramBuilder.NewTuple(tupleTypeTables, {
                ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::String>(NYT::NodeToYsonString(NYT::PathToNode(richYPath))),
                ctx.ProgramBuilder.NewDataLiteral(true),
                ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::String>(spec),
                ctx.ProgramBuilder.NewDataLiteral(outTableInfo.Stat->ChunkCount),
                ctx.ProgramBuilder.NewDataLiteral(outTableInfo.Stat->RecordsCount),
                ctx.ProgramBuilder.NewDataLiteral(false),
                ctx.ProgramBuilder.NewDataLiteral(ui32(0)),
            })})
        );
    }
    else {
        auto sectionList = TYtSectionList(&input);
        TVector<TType*> sectionTypes;
        bool rebuildType = false;
        for (size_t i: xrange(sectionList.Size())) {
            auto section = sectionList.Item(i);
            TType* secType = outItemType;
            if (sectionList.Size() > 1) {
                const auto varType = AS_TYPE(TVariantType, outItemType);
                const auto tupleType = AS_TYPE(TTupleType, varType->GetUnderlyingType());
                secType = tupleType->GetElementType(i);
            }
            TVector<TRuntimeNode> tableTuples;
            TVector<TString> columns;
            if (forceColumns) {
                for (auto& colType: section.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>()->GetItems()) {
                    columns.push_back(ToString(colType->GetName()));
                }
            }

            TStructType* structType = AS_TYPE(TStructType, secType);
            if (forceKeyColumns) {
                TMap<TString, const TTypeAnnotationNode*> extraKeyColumns;
                for (auto p: section.Paths()) {
                    TYtPathInfo pathInfo(p);
                    if (!pathInfo.Ranges || !pathInfo.Table->RowSpec || !pathInfo.Table->RowSpec->IsSorted()) {
                        continue;
                    }
                    size_t usedKeyPrefix = pathInfo.Ranges->GetUsedKeyPrefixLength();
                    for (size_t i = 0; i < usedKeyPrefix; ++i) {
                        TString key = pathInfo.Table->RowSpec->SortedBy[i];
                        if (!structType->FindMemberIndex(key)) {
                            auto itemType = pathInfo.Table->RowSpec->GetType()->FindItemType(key);
                            YQL_ENSURE(itemType);

                            auto it = extraKeyColumns.find(key);
                            if (it == extraKeyColumns.end()) {
                                extraKeyColumns[key] = itemType;
                            } else {
                                YQL_ENSURE(IsSameAnnotation(*it->second, *itemType),
                                    "Extra key columns should be of same type in all paths");
                            }
                        }
                    }
                }

                for (auto& [key, keyType] : extraKeyColumns) {
                    for (auto p: section.Paths()) {
                        TYtPathInfo pathInfo(p);
                        auto currKeyType = pathInfo.Table->RowSpec->GetType()->FindItemType(key);
                        YQL_ENSURE(currKeyType,
                            "Column " << key <<
                            " is used only in key filter in one YtPath and missing in another YPath in same section");
                        YQL_ENSURE(IsSameAnnotation(*keyType, *currKeyType),
                            "Extra key columns should be of same type in all paths");
                    }

                    secType = ctx.ProgramBuilder.NewStructType(secType, key,
                        NCommon::BuildType(section.Ref(), *keyType, ctx.ProgramBuilder));
                    rebuildType = true;
                }

                for (auto& k : extraKeyColumns) {
                    columns.push_back(k.first);
                }
            }

            NYT::TNode sysColumns;
            for (auto& col: extraSysColumns) {
                const auto fullName = TString(YqlSysColumnPrefix).append(col);
                if (!structType->FindMemberIndex(fullName)) {
                    sysColumns.Add(col);
                    secType = ctx.ProgramBuilder.NewStructType(secType, fullName, ctx.ProgramBuilder.NewDataType(GetSysColumnTypeId(col)));
                    rebuildType = true;
                }
            }
            sectionTypes.push_back(secType);
            for (auto col: NYql::GetSettingAsColumnList(section.Settings().Ref(), EYtSettingType::SysColumns)) {
                sysColumns.Add(col);
            }

            for (auto p: section.Paths()) {
                TYtPathInfo pathInfo(p);
                YQL_ENSURE(pathInfo.Table->Stat, "Table " << pathInfo.Table->Name.Quote() << " has no Stat");
                // Table may have aux columns. Exclude them by specifying explicit columns from the type
                if (forceColumns && pathInfo.Table->RowSpec && (forceKeyColumns || !pathInfo.HasColumns())) {
                    pathInfo.SetColumns(columns);
                }
                TString spec;
                if (!sysColumns.IsUndefined()) {
                    auto specNode = pathInfo.GetCodecSpecNode();
                    specNode[YqlSysColumnPrefix] = sysColumns;
                    spec = NYT::NodeToCanonicalYsonString(specNode);
                } else {
                    spec = pathInfo.GetCodecSpecStr();
                }

                TVector<TRuntimeNode> tupleItems;
                NYT::TRichYPath richTPath(pathInfo.Table->Name);
                pathInfo.FillRichYPath(richTPath);
                tupleItems.push_back(ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::String>(NYT::NodeToYsonString(NYT::PathToNode(richTPath))));
                tupleItems.push_back(ctx.ProgramBuilder.NewDataLiteral(pathInfo.Table->IsTemp));
                tupleItems.push_back(ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::String>(spec));
                tupleItems.push_back(ctx.ProgramBuilder.NewDataLiteral(pathInfo.Table->Stat->ChunkCount));
                tupleItems.push_back(ctx.ProgramBuilder.NewDataLiteral(pathInfo.Table->Stat->RecordsCount));
                tupleItems.push_back(ctx.ProgramBuilder.NewDataLiteral(pathInfo.Table->IsAnonymous));
                tupleItems.push_back(ctx.ProgramBuilder.NewDataLiteral(pathInfo.Table->Epoch.GetOrElse(0)));

                tableTuples.push_back(ctx.ProgramBuilder.NewTuple(tupleTypeTables, tupleItems));
            }
            groups.push_back(ctx.ProgramBuilder.NewList(tupleTypeTables, tableTuples));
            // All sections have the same sampling settings
            sampling = GetSampleParams(section.Settings().Ref());
        }
        if (sectionList.Size() == 1) {
            settings = sectionList.Item(0).Settings().Raw();
            if (rebuildType) {
                outItemType = sectionTypes.front();
            }
        } else if (rebuildType) {
            outItemType = ctx.ProgramBuilder.NewVariantType(ctx.ProgramBuilder.NewTupleType(sectionTypes));
        }
    }

    TVector<TRuntimeNode> samplingTupleItems;
    if (sampling) {
        samplingTupleItems.push_back(ctx.ProgramBuilder.NewDataLiteral(sampling->Percentage));
        samplingTupleItems.push_back(ctx.ProgramBuilder.NewDataLiteral(sampling->Repeat));
        bool isSystemSampling = sampling->Mode == EYtSampleMode::System;
        samplingTupleItems.push_back(ctx.ProgramBuilder.NewDataLiteral(isSystemSampling));
    }

    auto outListType = ctx.ProgramBuilder.NewListType(outItemType);

    TCallableBuilder call(ctx.ProgramBuilder.GetTypeEnvironment(), callName, outListType);

    call.Add(ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::String>(clusterName)); // cluster name
    call.Add(ctx.ProgramBuilder.NewList(listTypeGroup, groups));
    call.Add(ctx.ProgramBuilder.NewTuple(samplingTupleItems));

    if (itemsCount) {
        call.Add(ctx.ProgramBuilder.NewTuple({ctx.ProgramBuilder.NewDataLiteral(*itemsCount)}));
    } else {
        call.Add(ctx.ProgramBuilder.NewEmptyTuple());
    }

    auto res = TRuntimeNode(call.Build(), false);

    if (settings) {
        for (auto child: settings->Children()) {
            switch (FromString<EYtSettingType>(child->Child(0)->Content())) {
            case EYtSettingType::Take:
                res = ctx.ProgramBuilder.Take(res, NCommon::MkqlBuildExpr(*child->Child(1), ctx));
                break;
            case EYtSettingType::Skip:
                res = ctx.ProgramBuilder.Skip(res, NCommon::MkqlBuildExpr(*child->Child(1), ctx));
                break;
            case EYtSettingType::Sample:
            case EYtSettingType::DirectRead:
            case EYtSettingType::KeyFilter:
            case EYtSettingType::KeyFilter2:
            case EYtSettingType::Unordered:
            case EYtSettingType::NonUnique:
            case EYtSettingType::SysColumns:
                break;
            default:
                YQL_LOG_CTX_THROW yexception() << "Unsupported table content setting " << TString{child->Child(0)->Content()}.Quote();
            }
        }
    }

    return res;
}

TRuntimeNode BuildTableContentCall(TType* outItemType,
    TStringBuf clusterName,
    const TExprNode& input,
    const TMaybe<ui64>& itemsCount,
    NCommon::TMkqlBuildContext& ctx,
    bool forceColumns,
    const THashSet<TString>& extraSysColumns,
    bool forceKeyColumns)
{
    return BuildTableContentCall(TYtTableContent::CallableName(), outItemType, clusterName, input, itemsCount, ctx, forceColumns, extraSysColumns, forceKeyColumns);
}

template<bool NeedPartitionRanges>
TRuntimeNode BuildDqYtInputCall(
    TType* outputType,
    TType* itemType,
    const TString& clusterName,
    const TString& tokenName,
    const TYtSectionList& sectionList,
    const TYtState::TPtr& state,
    NCommon::TMkqlBuildContext& ctx,
    size_t inflight,
    size_t timeout,
    bool enableBlockReader)
{
    NYT::TNode specNode = NYT::TNode::CreateMap();
    NYT::TNode& tablesNode = specNode[YqlIOSpecTables];
    NYT::TNode& registryNode = specNode[YqlIOSpecRegistry];
    THashMap<TString, TString> uniqSpecs;
    NYT::TNode samplingSpec;
    const ui64 nativeTypeCompat = state->Configuration->NativeYtTypeCompatibility.Get(clusterName).GetOrElse(NTCF_LEGACY);

    auto updateFlags = [nativeTypeCompat](NYT::TNode& spec) {
        if (spec.HasKey(YqlRowSpecAttribute)) {
            auto& rowSpec = spec[YqlRowSpecAttribute];
            ui64 nativeYtTypeFlags = 0;
            if (rowSpec.HasKey(RowSpecAttrNativeYtTypeFlags)) {
                nativeYtTypeFlags = rowSpec[RowSpecAttrNativeYtTypeFlags].AsUint64();
            } else {
                if (rowSpec.HasKey(RowSpecAttrUseNativeYtTypes)) {
                    nativeYtTypeFlags = rowSpec[RowSpecAttrUseNativeYtTypes].AsBool() ? NTCF_LEGACY : NTCF_NONE;
                } else if (rowSpec.HasKey(RowSpecAttrUseTypeV2)) {
                    nativeYtTypeFlags = rowSpec[RowSpecAttrUseTypeV2].AsBool() ? NTCF_LEGACY : NTCF_NONE;
                }
            }
            rowSpec[RowSpecAttrNativeYtTypeFlags] = ui64(nativeYtTypeFlags & nativeTypeCompat);
        }
    };

    TVector<TRuntimeNode> groups;
    for (size_t i: xrange(sectionList.Size())) {
        auto section = sectionList.Item(i);
        for (auto& child: section.Settings().Ref().Children()) {
            switch (FromString<EYtSettingType>(child->Child(0)->Content())) {
            case EYtSettingType::Sample:
            case EYtSettingType::SysColumns:
            case EYtSettingType::Unordered:
            case EYtSettingType::NonUnique:
                break;
            case EYtSettingType::KeyFilter:
            case EYtSettingType::KeyFilter2:
                YQL_ENSURE(child->Child(1)->ChildrenSize() == 0, "Unsupported KeyFilter setting");
                break;
            default:
                YQL_ENSURE(false, "Unsupported settings");
                break;
            }
        }

        TVector<TStringBuf> columns;
        THashMap<TString, ui32> structColumns;
        ui32 index = 0;
        for (auto& colType: section.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>()->GetItems()) {
            columns.push_back(colType->GetName());
            structColumns.emplace(colType->GetName(), index++);
        }

        NYT::TNode sysColumns;
        for (auto col: NYql::GetSettingAsColumnList(section.Settings().Ref(), EYtSettingType::SysColumns)) {
            sysColumns.Add(col);
        }

        TVector<TRuntimeNode> tableTuples;
        ui64 tableOffset = 0;
        for (auto p: section.Paths()) {
            TYtPathInfo pathInfo(p);
            // Table may have aux columns. Exclude them by specifying explicit columns from the type
            if (pathInfo.Table->RowSpec && !pathInfo.HasColumns()) {
                pathInfo.SetColumns(columns);
            }
            auto specNode = pathInfo.GetCodecSpecNode();
            if (!sysColumns.IsUndefined()) {
                specNode[YqlSysColumnPrefix] = sysColumns;
            }
            updateFlags(specNode);
            TString refName = TStringBuilder() << "$table" << uniqSpecs.size();
            auto res = uniqSpecs.emplace(NYT::NodeToCanonicalYsonString(specNode), refName);
            if (res.second) {
                registryNode[refName] = specNode;
            } else {
                refName = res.first->second;
            }
            tablesNode.Add(refName);
            // TODO() Enable range indexes
            auto skiffNode = SingleTableSpecToInputSkiff(specNode, structColumns, !enableBlockReader, !enableBlockReader, false);
            const auto tmpFolder = GetTablesTmpFolder(*state->Configuration);
            auto tableName = pathInfo.Table->Name;
            if (pathInfo.Table->IsAnonymous && !TYtTableInfo::HasSubstAnonymousLabel(pathInfo.Table->FromNode.Cast())) {
                tableName = state->AnonymousLabels.Value(std::make_pair(clusterName, tableName), TString());
                YQL_ENSURE(tableName, "Unaccounted anonymous table: " << pathInfo.Table->Name);
            }

            NYT::TRichYPath richYPath = state->Gateway->GetRealTable(state->SessionId, clusterName, tableName, pathInfo.Table->Epoch.GetOrElse(0), tmpFolder);
            pathInfo.FillRichYPath(richYPath);
            auto pathNode = NYT::PathToNode(richYPath);

            tableTuples.push_back(ctx.ProgramBuilder.NewTuple({
                ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::String>(pathInfo.Table->IsTemp ? TString() : tableName),
                ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::String>(NYT::NodeToYsonString(pathNode)),
                ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::String>(NYT::NodeToYsonString(skiffNode)),
                ctx.ProgramBuilder.NewDataLiteral(tableOffset)
            }));
            YQL_ENSURE(pathInfo.Table->Stat);
            tableOffset += pathInfo.Table->Stat->RecordsCount;
        }
        groups.push_back(ctx.ProgramBuilder.NewList(tableTuples.front().GetStaticType(), tableTuples));
        // All sections have the same sampling settings
        if (samplingSpec.IsUndefined()) {
            if (auto sampling = GetSampleParams(section.Settings().Ref())) {
                YQL_ENSURE(sampling->Mode != EYtSampleMode::System);
                samplingSpec["sampling_rate"] = sampling->Percentage / 100.;
                if (sampling->Repeat) {
                    samplingSpec["sampling_seed"] = static_cast<i64>(sampling->Repeat);
                }
            }
        }
    }

    auto server = state->Gateway->GetClusterServer(clusterName);
    YQL_ENSURE(server, "Invalid YT cluster: " << clusterName);

    TCallableBuilder call(ctx.ProgramBuilder.GetTypeEnvironment(), enableBlockReader ? "DqYtBlockRead" : "DqYtRead", outputType);

    call.Add(ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::String>(server));
    call.Add(ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::String>(tokenName));
    call.Add(ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::String>(NYT::NodeToYsonString(specNode)));
    call.Add(ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::String>(samplingSpec.IsUndefined() ? TString() : NYT::NodeToYsonString(samplingSpec)));
    call.Add(ctx.ProgramBuilder.NewList(groups.front().GetStaticType(), groups));
    call.Add(TRuntimeNode(itemType, true));

    call.Add(ctx.ProgramBuilder.NewDataLiteral(inflight));
    call.Add(ctx.ProgramBuilder.NewDataLiteral(timeout));
    if constexpr (NeedPartitionRanges)
        call.Add(ctx.ProgramBuilder.NewVoid());

    return TRuntimeNode(call.Build(), false);
}

void RegisterYtMkqlCompilers(NCommon::TMkqlCallableCompilerBase& compiler) {
    compiler.AddCallable(TYtLength::CallableName(),
        [](const TExprNode& node, NCommon::TMkqlBuildContext& ctx) {
            auto length = TYtLength(&node);
            ui64 lengthRes = 0;
            if (auto out = length.Input().Maybe<TYtOutput>()) {
                auto info = TYtOutTableInfo(GetOutTable(out.Cast()));
                YQL_ENSURE(info.Stat);
                lengthRes = info.Stat->RecordsCount;
            } else {
                auto read = length.Input().Maybe<TYtReadTable>();
                YQL_ENSURE(read, "Unknown length input");
                YQL_ENSURE(read.Cast().Input().Size() == 1, "Unsupported read with multiple sections");
                for (auto path: read.Cast().Input().Item(0).Paths()) {
                    auto info = TYtTableBaseInfo::Parse(path.Table());
                    YQL_ENSURE(info->Stat);
                    lengthRes += info->Stat->RecordsCount;
                }
            }
            return ctx.ProgramBuilder.NewDataLiteral<ui64>(lengthRes);
        });

    compiler.AddCallable(TYtTableContent::CallableName(),
        [](const TExprNode& node, NCommon::TMkqlBuildContext& ctx) {
            TYtTableContent tableContent(&node);
            TMaybe<ui64> itemsCount;
            if (auto setting = NYql::GetSetting(tableContent.Settings().Ref(), EYtSettingType::ItemsCount)) {
                itemsCount = FromString<ui64>(setting->Child(1)->Content());
            }
            if (auto maybeRead = tableContent.Input().Maybe<TYtReadTable>()) {
                auto read = maybeRead.Cast();
                return BuildTableContentCall(
                    NCommon::BuildType(node, *node.GetTypeAnn()->Cast<TListExprType>()->GetItemType(), ctx.ProgramBuilder),
                    read.DataSource().Cluster().Value(), read.Input().Ref(), itemsCount, ctx, true);
            } else {
                auto output = tableContent.Input().Cast<TYtOutput>();
                return BuildTableContentCall(
                    NCommon::BuildType(node, *node.GetTypeAnn()->Cast<TListExprType>()->GetItemType(), ctx.ProgramBuilder),
                    GetOutputOp(output).DataSink().Cluster().Value(), output.Ref(), itemsCount, ctx, true);
            }
        });

    compiler.AddCallable({TYtTablePath::CallableName(), TYtTableRecord::CallableName(), TYtTableIndex::CallableName(), TYtIsKeySwitch::CallableName(), TYtRowNumber::CallableName()},
        [](const TExprNode& node, NCommon::TMkqlBuildContext& ctx) {
            auto dataSlot = node.GetTypeAnn()->Cast<TDataExprType>()->GetSlot();

            TCallableBuilder call(ctx.ProgramBuilder.GetTypeEnvironment(), node.Content(),
                ctx.ProgramBuilder.NewDataType(dataSlot));

            call.Add(NCommon::MkqlBuildExpr(*node.Child(0), ctx));
            return TRuntimeNode(call.Build(), false);
        });
}

void RegisterDqYtMkqlCompilers(NCommon::TMkqlCallableCompilerBase& compiler, const TYtState::TPtr& state) {

    compiler.ChainCallable(TDqReadBlockWideWrap::CallableName(),
        [state](const TExprNode& node, NCommon::TMkqlBuildContext& ctx) {
            if (const auto& wrapper = TDqReadBlockWideWrap(&node); wrapper.Input().Maybe<TYtReadTable>().IsValid()) {
                const auto ytRead = wrapper.Input().Cast<TYtReadTable>();
                const auto readType = ytRead.Ref().GetTypeAnn()->Cast<TTupleExprType>()->GetItems().back();
                const auto inputItemType = NCommon::BuildType(wrapper.Input().Ref(), GetSeqItemType(*readType), ctx.ProgramBuilder);
                const auto cluster = ytRead.DataSource().Cluster().StringValue();
                const bool useRPCReaderDefault = DEFAULT_USE_RPC_READER_IN_DQ || state->Types->BlockEngineMode != EBlockEngineMode::Disable;
                size_t inflight = state->Configuration->UseRPCReaderInDQ.Get(cluster).GetOrElse(useRPCReaderDefault) ? state->Configuration->DQRPCReaderInflight.Get(cluster).GetOrElse(DEFAULT_RPC_READER_INFLIGHT) : 0;
                size_t timeout = state->Configuration->DQRPCReaderTimeout.Get(cluster).GetOrElse(DEFAULT_RPC_READER_TIMEOUT).MilliSeconds();
                const auto outputType = NCommon::BuildType(wrapper.Ref(), *wrapper.Ref().GetTypeAnn(), ctx.ProgramBuilder);
                TString tokenName;
                if (auto secureParams = wrapper.Token()) {
                    tokenName = secureParams.Cast().Name().StringValue();
                }

                bool solid = false;
                for (const auto& flag : wrapper.Flags())
                    if (solid = flag.Value() == "Solid")
                        break;
                return ctx.ProgramBuilder.BlockExpandChunked(
                    solid
                    ? BuildDqYtInputCall<false>(outputType, inputItemType, cluster, tokenName, ytRead.Input(), state, ctx, inflight, timeout, true && inflight)
                    : BuildDqYtInputCall<true>(outputType, inputItemType, cluster, tokenName, ytRead.Input(), state, ctx, inflight, timeout, true && inflight)
                );
            }

            return TRuntimeNode();
        });

    compiler.ChainCallable(TDqReadWideWrap::CallableName(),
        [state](const TExprNode& node, NCommon::TMkqlBuildContext& ctx) {
            if (const auto& wrapper = TDqReadWideWrap(&node); wrapper.Input().Maybe<TYtReadTable>().IsValid()) {
                const auto ytRead = wrapper.Input().Cast<TYtReadTable>();
                const auto readType = ytRead.Ref().GetTypeAnn()->Cast<TTupleExprType>()->GetItems().back();
                const auto inputItemType = NCommon::BuildType(wrapper.Input().Ref(), GetSeqItemType(*readType), ctx.ProgramBuilder);
                const auto cluster = ytRead.DataSource().Cluster().StringValue();
                size_t isRPC = state->Configuration->UseRPCReaderInDQ.Get(cluster).GetOrElse(DEFAULT_USE_RPC_READER_IN_DQ) ? state->Configuration->DQRPCReaderInflight.Get(cluster).GetOrElse(DEFAULT_RPC_READER_INFLIGHT) : 0;
                size_t timeout = state->Configuration->DQRPCReaderTimeout.Get(cluster).GetOrElse(DEFAULT_RPC_READER_TIMEOUT).MilliSeconds();
                const auto outputType = NCommon::BuildType(wrapper.Ref(), *wrapper.Ref().GetTypeAnn(), ctx.ProgramBuilder);
                TString tokenName;
                if (auto secureParams = wrapper.Token()) {
                    tokenName = secureParams.Cast().Name().StringValue();
                }

                bool solid = false;
                for (const auto& flag : wrapper.Flags())
                    if (solid = flag.Value() == "Solid")
                        break;

                if (solid)
                    return BuildDqYtInputCall<false>(outputType, inputItemType, cluster, tokenName, ytRead.Input(), state, ctx, isRPC, timeout, false);
                else
                    return BuildDqYtInputCall<true>(outputType, inputItemType, cluster, tokenName, ytRead.Input(), state, ctx, isRPC, timeout, false);
            }

            return TRuntimeNode();
        });

    compiler.AddCallable(TYtDqWideWrite::CallableName(),
        [](const TExprNode& node, NCommon::TMkqlBuildContext& ctx) {
            const auto write = TYtDqWideWrite(&node);
            const auto outType = NCommon::BuildType(write.Ref(), *write.Ref().GetTypeAnn(), ctx.ProgramBuilder);
            const auto arg = MkqlBuildExpr(write.Input().Ref(), ctx);

            TString server{GetSetting(write.Settings().Ref(), "server")->Child(1)->Content()};
            TString table{GetSetting(write.Settings().Ref(), "table")->Child(1)->Content()};
            TString outSpec{GetSetting(write.Settings().Ref(), "outSpec")->Child(1)->Content()};
            auto secureSetting = GetSetting(write.Settings().Ref(), "secureParams");
            TString writerOptions{GetSetting(write.Settings().Ref(), "writerOptions")->Child(1)->Content()};

            TString tokenName;
            if (secureSetting->ChildrenSize() > 1) {
                TCoSecureParam secure(secureSetting->Child(1));
                tokenName = secure.Name().StringValue();
            }

            TCallableBuilder call(ctx.ProgramBuilder.GetTypeEnvironment(), "YtDqRowsWideWrite", outType);
            call.Add(arg);
            call.Add(ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::String>(server));
            call.Add(ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::String>(tokenName));
            call.Add(ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::String>(table));
            call.Add(ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::String>(outSpec));
            call.Add(ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::String>(writerOptions));

            return TRuntimeNode(call.Build(), false);
        });
}

}
