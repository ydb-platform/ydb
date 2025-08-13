#include "exec_ctx.h"

#include <library/cpp/yson/node/node_io.h>
#include <yql/essentials/utils/log/log.h>
#include <yt/cpp/mapreduce/common/helpers.h>
#include <yt/yql/providers/yt/common/yql_names.h>
#include <yt/yql/providers/yt/gateway/lib/yt_helpers.h>
#include <yt/yql/providers/yt/lib/schema/schema.h>

#include <yql/essentials/providers/common/proto/gateways_config.pb.h>

namespace NYql {

using namespace NNodes;

TInputInfo::TInputInfo(const TString& name, const NYT::TRichYPath& path, bool temp, bool strict, const TYtTableBaseInfo& info, const NYT::TNode& spec, ui32 group)
    : Name(name)
    , Path(path)
    , Cluster(info.Cluster)
    , Temp(temp)
    , Dynamic(info.Meta->IsDynamic)
    , Strict(strict)
    , Records(info.Stat->RecordsCount)
    , DataSize(info.Stat->DataSize)
    , Spec(spec)
    , Group(group)
    , Lookup(info.Meta->Attrs.Value("optimize_for", "scan") != "scan")
    , ErasureCodec(info.Meta->Attrs.Value("erasure_codec", "none"))
    , CompressionCode(info.Meta->Attrs.Value("compression_codec", "none"))
    , PrimaryMedium(info.Meta->Attrs.Value("primary_medium", "default"))
    , Media(NYT::NodeFromYsonString(info.Meta->Attrs.Value("media", "#")))
{
}

TExecContextBaseSimple::TExecContextBaseSimple(
    const TYtBaseServices::TPtr& services,
    const TConfigClusters::TPtr& clusters,
    TIntrusivePtr<NCommon::TMkqlCommonCallableCompiler> mkqlCompiler,
    const TString& cluster,
    const TSessionBase::TPtr& session)
    : FunctionRegistry_(services->FunctionRegistry)
    , Config_(services->Config)
    , Clusters_(clusters)
    , MkqlCompiler_(mkqlCompiler)
    , Cluster_(cluster)
    , BaseSession_(session)
{
}

void TExecContextBaseSimple::FillRichPathForPullCaseInput(NYT::TRichYPath& richYPath, TYtTableBaseInfo::TPtr tableInfo) {
    if (tableInfo->Cluster != Cluster_) {
        richYPath.Cluster(Clusters_->GetYtName(tableInfo->Cluster));
    }
}

void TExecContextBaseSimple::FillRichPathForInput(NYT::TRichYPath& richYPath, const TYtPathInfo& pathInfo, const TString& newPath, bool localChainTest) {
    if (localChainTest || (pathInfo.Table->IsTemp && !pathInfo.Table->IsAnonymous)) {
        richYPath.Path(newPath);
    }
}

void TExecContextBaseSimple::SetInput(TExprBase input, bool forcePathColumns, const THashSet<TString>& extraSysColumns, const TYtSettings::TConstPtr& settings) {
    NYT::TNode extraSysColumnsNode;
    for (auto sys: extraSysColumns) {
        extraSysColumnsNode.Add(sys);
    }

    const bool allowRemoteClusters = settings->_AllowRemoteClusterInput.Get(Cluster_).GetOrElse(DEFAULT_ALLOW_REMOTE_CLUSTER_INPUT);
    if (auto out = input.Maybe<TYtOutput>()) { // Pull case
        auto tableInfo = TYtTableBaseInfo::Parse(out.Cast());
        YQL_CLOG(INFO, ProviderYt) << "Runtime cluster: " << Cluster_ << ", Input: " << tableInfo->Cluster << '.' << tableInfo->Name;
        if (tableInfo->Cluster != Cluster_ && !allowRemoteClusters) {
            YQL_LOG_CTX_THROW TErrorException(TIssuesIds::DEFAULT_ERROR) <<
                "Operation input from remote cluster " << tableInfo->Cluster.Quote() << " is not allowed on cluster " << Cluster_.Quote();
        }
        const TString tmpFolder = GetTablesTmpFolder(*settings, tableInfo->Cluster);
        NYT::TRichYPath richYPath(NYql::TransformPath(tmpFolder, tableInfo->Name, true, BaseSession_->UserName_));
        FillRichPathForPullCaseInput(richYPath, tableInfo);

        auto spec = tableInfo->GetCodecSpecNode();
        if (!extraSysColumnsNode.IsUndefined()) {
            spec[YqlSysColumnPrefix] = extraSysColumnsNode;
        }

        InputTables_.emplace_back(
            richYPath.Path_,
            richYPath,
            true,
            true,
            *tableInfo,
            spec,
            0
        );
    }
    else {
        TMaybe<bool> hasScheme;
        size_t loggedTable = 0;
        const bool localChainTest = IsLocalChainTest();
        ui32 group = 0;
        for (auto section: input.Cast<TYtSectionList>()) {
            TVector<TStringBuf> columns;
            auto sysColumnsSetting = NYql::GetSettingAsColumnList(section.Settings().Ref(), EYtSettingType::SysColumns);
            if (forcePathColumns) {
                for (auto& colType: section.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>()->GetItems()) {
                    if (auto name = colType->GetName(); !name.SkipPrefix(YqlSysColumnPrefix) || Find(sysColumnsSetting, name) == sysColumnsSetting.cend()) {
                        columns.push_back(colType->GetName());
                    }
                }
            }
            NYT::TNode sysColumns = extraSysColumnsNode;
            for (auto sys: sysColumnsSetting) {
                if (!extraSysColumns.contains(sys)) {
                    sysColumns.Add(sys);
                }
            }
            for (auto path: section.Paths()) {
                TYtPathInfo pathInfo(path);
                const TString pathCluster = pathInfo.Table->Cluster;
                if (loggedTable++ < 10) {
                    YQL_CLOG(INFO, ProviderYt) << "Runtime cluster: " << Cluster_ << ", Input: " << pathCluster << '.' << pathInfo.Table->Name << '[' << group << ']';
                }
                if (pathCluster != Cluster_ && !allowRemoteClusters) {
                    YQL_LOG_CTX_THROW TErrorException(TIssuesIds::DEFAULT_ERROR) <<
                        "Operation input from remote cluster " << pathCluster.Quote() << " is not allowed on cluster " << Cluster_.Quote();
                }
                // Table may have aux columns. Exclude them by specifying explicit columns from the type
                if (forcePathColumns && pathInfo.Table->RowSpec && !pathInfo.HasColumns()) {
                    pathInfo.SetColumns(columns);
                }
                const TString tmpFolder = GetTablesTmpFolder(*settings, pathCluster);
                auto name = NYql::TransformPath(tmpFolder, pathInfo.Table->Name, pathInfo.Table->IsTemp, BaseSession_->UserName_);
                NYT::TRichYPath richYPath;
                FillRichPathForInput(richYPath, pathInfo, name, localChainTest);
                if (pathCluster != Cluster_) {
                    richYPath.Cluster(Clusters_->GetYtName(pathCluster));
                }

                pathInfo.FillRichYPath(richYPath);

                auto spec = pathInfo.GetCodecSpecNode();
                if (!sysColumns.IsUndefined()) {
                    spec[YqlSysColumnPrefix] = sysColumns;
                }

                InputTables_.emplace_back(
                    name,
                    richYPath,
                    pathInfo.Table->IsTemp,
                    !pathInfo.Table->RowSpec || pathInfo.Table->RowSpec->StrictSchema,
                    *pathInfo.Table,
                    spec,
                    group
                );
                if (NYql::HasSetting(pathInfo.Table->Settings.Ref(), EYtSettingType::WithQB)) {
                    auto p = pathInfo.Table->Meta->Attrs.FindPtr(QB2Premapper);
                    YQL_ENSURE(p, "Expect " << QB2Premapper << " in meta attrs");
                    InputTables_.back().QB2Premapper = NYT::NodeFromYsonString(*p);
                }
                const bool tableHasScheme = InputTables_.back().Spec.HasKey(YqlRowSpecAttribute);
                if (!hasScheme) {
                    hasScheme = tableHasScheme;
                } else {
                    YQL_ENSURE(*hasScheme == tableHasScheme, "Mixed Yamr/Yson input table formats");
                }
            }
            if (0 == group) {
                Sampling = NYql::GetSampleParams(section.Settings().Ref());
            } else {
                YQL_ENSURE(NYql::GetSampleParams(section.Settings().Ref()) == Sampling, "Different sampling settings");
            }
            ++group;
        }

        if (hasScheme && !*hasScheme) {
            YamrInput = true;
        }
        if (loggedTable > 10) {
            YQL_CLOG(INFO, ProviderYt) << "...total input tables=" << loggedTable;
        }
    }
}

void TExecContextBaseSimple::SetOutput(TYtOutSection output, const TYtSettings::TConstPtr& settings, const TString& opHash) {
    const TString tmpFolder = GetTablesTmpFolder(*settings, Cluster_);
    const auto nativeYtTypeCompatibility = settings->NativeYtTypeCompatibility.Get(Cluster_).GetOrElse(NTCF_LEGACY);
    const bool rowSpecCompactForm = settings->UseYqlRowSpecCompactForm.Get().GetOrElse(DEFAULT_ROW_SPEC_COMPACT_FORM);
    const bool optimizeForScan = settings->OptimizeFor.Get(Cluster_).GetOrElse(NYT::EOptimizeForAttr::OF_LOOKUP_ATTR) != NYT::EOptimizeForAttr::OF_LOOKUP_ATTR;
    size_t loggedTable = 0;
    TVector<TString> outTablePaths;
    TVector<NYT::TNode> outTableSpecs;
    for (auto table: output) {
        TYtOutTableInfo tableInfo(table);
        YQL_ENSURE(!tableInfo.Cluster || tableInfo.Cluster == Cluster_);
        TString outTableName = tableInfo.Name;
        if (outTableName.empty()) {
            outTableName = TStringBuilder() << "tmp/" << GetGuidAsString(BaseSession_->RandomProvider_->GenGuid());
        }
        TString outTablePath = NYql::TransformPath(tmpFolder, outTableName, true, BaseSession_->UserName_);
        auto attrSpec = tableInfo.GetAttrSpecNode(nativeYtTypeCompatibility, rowSpecCompactForm);
        OutTables_.emplace_back(
            outTableName,
            outTablePath,
            tableInfo.GetCodecSpecNode(),
            attrSpec,
            ToYTSortColumns(tableInfo.RowSpec->GetForeignSort()),
            optimizeForScan ? tableInfo.GetColumnGroups() : NYT::TNode{}
        );
        outTablePaths.push_back(outTablePath);
        outTableSpecs.push_back(std::move(attrSpec));
        if (loggedTable++ < 10) {
            YQL_CLOG(INFO, ProviderYt) << "Output: " << Cluster_ << '.' << outTableName;
        }
    }
    if (loggedTable > 10) {
        YQL_CLOG(INFO, ProviderYt) << "...total output tables=" << loggedTable;
    }

    SetCache(outTablePaths, outTableSpecs, tmpFolder, settings, opHash);
}

void TExecContextBaseSimple::SetCache(const TVector<TString>&, const TVector<NYT::TNode>&, const TString&, const TYtSettings::TConstPtr&, const TString&) {
    // Cache item should only be set for native gateway, not fmr
    return;
}

void TExecContextBaseSimple::SetSingleOutput(const TYtOutTableInfo& outTable, const TYtSettings::TConstPtr& settings) {
    const TString tmpFolder = GetTablesTmpFolder(*settings, Cluster_);
    YQL_ENSURE(!outTable.Cluster || outTable.Cluster == Cluster_);
    TString outTableName = TStringBuilder() << "tmp/" << GetGuidAsString(BaseSession_->RandomProvider_->GenGuid());
    TString outTablePath = NYql::TransformPath(tmpFolder, outTableName, true, BaseSession_->UserName_);

    const auto nativeYtTypeCompatibility = settings->NativeYtTypeCompatibility.Get(Cluster_).GetOrElse(NTCF_LEGACY);
    const bool rowSpecCompactForm = settings->UseYqlRowSpecCompactForm.Get().GetOrElse(DEFAULT_ROW_SPEC_COMPACT_FORM);
    const bool optimizeForScan = settings->OptimizeFor.Get(Cluster_).GetOrElse(NYT::EOptimizeForAttr::OF_LOOKUP_ATTR) != NYT::EOptimizeForAttr::OF_LOOKUP_ATTR;

    OutTables_.emplace_back(
        outTableName,
        outTablePath,
        outTable.GetCodecSpecNode(),
        outTable.GetAttrSpecNode(nativeYtTypeCompatibility, rowSpecCompactForm),
        ToYTSortColumns(outTable.RowSpec->GetForeignSort()),
        optimizeForScan ? outTable.GetColumnGroups() : NYT::TNode{}
    );

    YQL_CLOG(INFO, ProviderYt) << "Output: " << Cluster_ << '.' << outTableName;
}

TString TExecContextBaseSimple::GetInputSpec(bool ensureOldTypesOnly, ui64 nativeTypeCompatibilityFlags, bool intermediateInput) const {
    return GetSpecImpl(InputTables_, 0, InputTables_.size(), {}, ensureOldTypesOnly, nativeTypeCompatibilityFlags, intermediateInput);
}

TString TExecContextBaseSimple::GetOutSpec(bool ensureOldTypesOnly, ui64 nativeTypeCompatibilityFlags) const {
    return GetSpecImpl(OutTables_, 0, OutTables_.size(), {}, ensureOldTypesOnly, nativeTypeCompatibilityFlags, false);
}

TString TExecContextBaseSimple::GetOutSpec(size_t beginIdx, size_t endIdx, NYT::TNode initialOutSpec, bool ensureOldTypesOnly, ui64 nativeTypeCompatibilityFlags) const {
    return GetSpecImpl(OutTables_, beginIdx, endIdx, initialOutSpec, ensureOldTypesOnly, nativeTypeCompatibilityFlags, false);
}

template <class TTableType>
TString TExecContextBaseSimple::GetSpecImpl(const TVector<TTableType>& tables, size_t beginIdx, size_t endIdx, NYT::TNode initialSpec, bool ensureOldTypesOnly, ui64 nativeTypeCompatibilityFlags, bool intermediateInput) {
    YQL_ENSURE(beginIdx <= endIdx);
    YQL_ENSURE(endIdx <= tables.size());
    NYT::TNode specNode = initialSpec;
    if (initialSpec.IsUndefined()) {
        specNode = NYT::TNode::CreateMap();
    }
    NYT::TNode& tablesNode = specNode[YqlIOSpecTables];

    auto updateFlags = [nativeTypeCompatibilityFlags](NYT::TNode& spec) {
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
            rowSpec[RowSpecAttrNativeYtTypeFlags] = (nativeYtTypeFlags & nativeTypeCompatibilityFlags);
        }
    };

    if (!intermediateInput && (endIdx - beginIdx) > 1) {
        NYT::TNode& registryNode = specNode[YqlIOSpecRegistry];
        THashMap<TString, TString> uniqSpecs;
        for (size_t i = beginIdx; i < endIdx; ++i) {
            auto& table = tables[i];
            TString refName = TStringBuilder() << "$table" << uniqSpecs.size();
            auto spec = table.Spec;
            if (ensureOldTypesOnly) {
                EnsureSpecDoesntUseNativeYtTypes(spec, table.Name, std::is_same<TTableType, TInputInfo>::value);
            } else {
                updateFlags(spec);
            }
            auto res = uniqSpecs.emplace(NYT::NodeToCanonicalYsonString(spec), refName);
            if (res.second) {
                registryNode[refName] = std::move(spec);
            }
            else {
                refName = res.first->second;
            }
            tablesNode.Add(refName);
        }
    }
    else {
        auto& table = tables[beginIdx];
        auto spec = table.Spec;
        if (ensureOldTypesOnly) {
            EnsureSpecDoesntUseNativeYtTypes(spec, table.Name, std::is_same<TTableType, TInputInfo>::value);
        } else {
            updateFlags(spec);
        }

        tablesNode.Add(std::move(spec));
    }
    return NYT::NodeToYsonString(specNode);
}

TString TExecContextBaseSimple::GetSessionId() const {
    return BaseSession_->SessionId_;
}

bool TExecContextBaseSimple::IsLocalChainTest() const {
    return false;
}

} // namespace NYql
