#include "yql_yt_exec_ctx.h"

#include "yql_yt_spec.h"

#include <ydb/library/yql/providers/yt/gateway/lib/yt_helpers.h>
#include <ydb/library/yql/providers/yt/provider/yql_yt_op_settings.h>
#include <ydb/library/yql/providers/yt/provider/yql_yt_table.h>
#include <ydb/library/yql/providers/yt/codec/yt_codec.h>
#include <ydb/library/yql/providers/yt/lib/schema/schema.h>
#include <ydb/library/yql/providers/yt/common/yql_names.h>
#include <ydb/library/yql/providers/yt/common/yql_configuration.h>
#include <ydb/library/yql/providers/common/codec/yql_codec_type_flags.h>
#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>

#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <library/cpp/yson/node/node_io.h>
#include <yt/cpp/mapreduce/interface/config.h>
#include <yt/cpp/mapreduce/common/helpers.h>

#include <util/generic/hash.h>
#include <util/generic/strbuf.h>
#include <util/generic/yexception.h>
#include <util/generic/xrange.h>
#include <util/string/builder.h>
#include <util/system/guard.h>
#include <util/system/platform.h>

#include <type_traits>

namespace NYql {

namespace NNative {

using namespace NNodes;

TExecContextBase::TExecContextBase(const TYtNativeServices& services,
    const TConfigClusters::TPtr& clusters,
    const TIntrusivePtr<NCommon::TMkqlCommonCallableCompiler>& mkqlCompiler,
    const TSession::TPtr& session,
    const TString& cluster,
    const TYtUrlMapper& urlMapper)
    : FunctionRegistry_(services.FunctionRegistry)
    , FileStorage_(services.FileStorage)
    , Config_(services.Config)
    , Clusters_(clusters)
    , MkqlCompiler_(mkqlCompiler)
    , Session_(session)
    , Cluster_(cluster)
    , UrlMapper_(urlMapper)
    , DisableAnonymousClusterAccess_(services.DisableAnonymousClusterAccess)
    , Hidden(session->SessionId_.EndsWith("_hidden"))
{
    YtServer_ = Clusters_->GetServer(Cluster_);
    LogCtx_ = NYql::NLog::CurrentLogContextPath();
}


void TExecContextBase::MakeUserFiles(const TUserDataTable& userDataBlocks) {
    const TString& activeYtCluster = Clusters_->GetYtName(Cluster_);
    UserFiles_ = MakeIntrusive<TUserFiles>(UrlMapper_, activeYtCluster);
    for (const auto& file: userDataBlocks) {
        auto block = file.second;
        if (!Config_->GetMrJobUdfsDir().empty() && block.Usage.Test(EUserDataBlockUsage::Udf) && block.Type == EUserDataType::PATH) {
            TFsPath path = block.Data;
            TString fileName = path.Basename();
#ifdef _win_
            TStringBuf changedName(fileName);
            changedName.ChopSuffix(".dll");
            fileName = TString("lib") + changedName + ".so";
#endif
            block.Data = TFsPath(Config_->GetMrJobUdfsDir()) / fileName;
            TString md5;
            if (block.FrozenFile) {
                md5 = block.FrozenFile->GetMd5();
            }
            block.FrozenFile = CreateFakeFileLink(block.Data, md5);
        }

        UserFiles_->AddFile(file.first, block);
    }
}

void TExecContextBase::SetInput(TExprBase input, bool forcePathColumns, const THashSet<TString>& extraSysColumns, const TYtSettings::TConstPtr& settings) {
    const TString tmpFolder = GetTablesTmpFolder(*settings);

    NYT::TNode extraSysColumnsNode;
    for (auto sys: extraSysColumns) {
        extraSysColumnsNode.Add(sys);
    }

    if (auto out = input.Maybe<TYtOutput>()) { // Pull case
        auto tableInfo = TYtTableBaseInfo::Parse(out.Cast());
        YQL_CLOG(INFO, ProviderYt) << "Input: " << Cluster_ << '.' << tableInfo->Name;
        NYT::TRichYPath richYPath(NYql::TransformPath(tmpFolder, tableInfo->Name, true, Session_->UserName_));

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
        const auto entry = Config_->GetLocalChainTest() ? TTransactionCache::TEntry::TPtr() : GetEntry();

        auto fillSection = [&] (TYtSection section, ui32 group) {
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
                if (loggedTable++ < 10) {
                    YQL_CLOG(INFO, ProviderYt) << "Input: " << Cluster_ << '.' << pathInfo.Table->Name << '[' << group << ']';
                }
                // Table may have aux columns. Exclude them by specifying explicit columns from the type
                if (forcePathColumns && pathInfo.Table->RowSpec && !pathInfo.HasColumns()) {
                    pathInfo.SetColumns(columns);
                }
                auto name = NYql::TransformPath(tmpFolder, pathInfo.Table->Name, pathInfo.Table->IsTemp, Session_->UserName_);
                NYT::TRichYPath richYPath;
                if ((pathInfo.Table->IsTemp && !pathInfo.Table->IsAnonymous) || !entry) {
                    richYPath.Path(name);
                } else {
                    auto p = entry->Snapshots.FindPtr(std::make_pair(name, pathInfo.Table->Epoch.GetOrElse(0)));
                    YQL_ENSURE(p, "Table " << pathInfo.Table->Name << " has no snapshot");
                    richYPath.Path(std::get<0>(*p)).TransactionId(std::get<1>(*p)).OriginalPath(NYT::AddPathPrefix(name, NYT::TConfig::Get()->Prefix));
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
        };

        if (entry) {
            with_lock(entry->Lock_) {
                ui32 group = 0;
                for (auto section: input.Cast<TYtSectionList>()) {
                    fillSection(section, group);
                    ++group;
                }
            }
        } else {
            ui32 group = 0;
            for (auto section: input.Cast<TYtSectionList>()) {
                fillSection(section, group);
                ++group;
            }
        }

        if (hasScheme && !*hasScheme) {
            YamrInput = true;
        }
        if (loggedTable > 10) {
            YQL_CLOG(INFO, ProviderYt) << "...total input tables=" << loggedTable;
        }
    }
}

void TExecContextBase::SetOutput(TYtOutSection output, const TYtSettings::TConstPtr& settings, const TString& opHash) {
    const TString tmpFolder = GetTablesTmpFolder(*settings);
    const auto nativeYtTypeCompatibility = settings->NativeYtTypeCompatibility.Get(Cluster_).GetOrElse(NTCF_LEGACY);
    const bool rowSpecCompactForm = settings->UseYqlRowSpecCompactForm.Get().GetOrElse(DEFAULT_ROW_SPEC_COMPACT_FORM);
    const bool optimizeForScan = settings->OptimizeFor.Get(Cluster_).GetOrElse(NYT::EOptimizeForAttr::OF_LOOKUP_ATTR) != NYT::EOptimizeForAttr::OF_LOOKUP_ATTR;
    size_t loggedTable = 0;
    TVector<TString> outTablePaths;
    TVector<NYT::TNode> outTableSpecs;
    for (auto table: output) {
        TYtOutTableInfo tableInfo(table);
        TString outTableName = tableInfo.Name;
        if (outTableName.empty()) {
            outTableName = TStringBuilder() << "tmp/" << GetGuidAsString(Session_->RandomProvider_->GenGuid());
        }
        TString outTablePath = NYql::TransformPath(tmpFolder, outTableName, true, Session_->UserName_);
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

    SetCacheItem(outTablePaths, outTableSpecs, tmpFolder, settings, opHash);
}

void TExecContextBase::SetSingleOutput(const TYtOutTableInfo& outTable, const TYtSettings::TConstPtr& settings) {
    const TString tmpFolder = GetTablesTmpFolder(*settings);
    TString outTableName = TStringBuilder() << "tmp/" << GetGuidAsString(Session_->RandomProvider_->GenGuid());
    TString outTablePath = NYql::TransformPath(tmpFolder, outTableName, true, Session_->UserName_);

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

void TExecContextBase::SetCacheItem(const TVector<TString>& outTablePaths, const TVector<NYT::TNode>& outTableSpecs,
    const TString& tmpFolder, const TYtSettings::TConstPtr& settings, const TString& opHash) {
    const bool testRun = Config_->GetLocalChainTest();
    if (!testRun && !Hidden) {
        NYT::TNode mergeSpec = Session_->CreateSpecWithDesc();
        NYT::TNode tableAttrs = Session_->CreateTableAttrs();

        auto entry = GetOrCreateEntry(settings);
        FillSpec(mergeSpec, *this, settings, entry, 0., Nothing());
        auto chunkLimit = settings->QueryCacheChunkLimit.Get(Cluster_).GetOrElse(0);

        QueryCacheItem.Reset(new TYtQueryCacheItem(settings->QueryCacheMode.Get().GetOrElse(EQueryCacheMode::Disable),
            entry, opHash, outTablePaths, outTableSpecs, Session_->UserName_, tmpFolder, mergeSpec, tableAttrs, chunkLimit,
            settings->QueryCacheUseExpirationTimeout.Get().GetOrElse(false),
            settings->_UseMultisetAttributes.Get().GetOrElse(DEFAULT_USE_MULTISET_ATTRS), LogCtx_));
    }
}

TString TExecContextBase::GetInputSpec(bool ensureOldTypesOnly, ui64 nativeTypeCompatibilityFlags, bool intermediateInput) const {
    return GetSpecImpl(InputTables_, 0, InputTables_.size(), {}, ensureOldTypesOnly, nativeTypeCompatibilityFlags, intermediateInput);
}

TString TExecContextBase::GetOutSpec(bool ensureOldTypesOnly, ui64 nativeTypeCompatibilityFlags) const {
    return GetSpecImpl(OutTables_, 0, OutTables_.size(), {}, ensureOldTypesOnly, nativeTypeCompatibilityFlags, false);
}

TString TExecContextBase::GetOutSpec(size_t beginIdx, size_t endIdx, NYT::TNode initialOutSpec, bool ensureOldTypesOnly, ui64 nativeTypeCompatibilityFlags) const {
    return GetSpecImpl(OutTables_, beginIdx, endIdx, initialOutSpec, ensureOldTypesOnly, nativeTypeCompatibilityFlags, false);
}

template <class TTableType>
TString TExecContextBase::GetSpecImpl(const TVector<TTableType>& tables, size_t beginIdx, size_t endIdx, NYT::TNode initialSpec, bool ensureOldTypesOnly, ui64 nativeTypeCompatibilityFlags, bool intermediateInput) {
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

TTransactionCache::TEntry::TPtr TExecContextBase::GetOrCreateEntry(const TYtSettings::TConstPtr& settings) const {
    auto token = GetAuth(settings);
    auto impersonationUser = GetImpersonationUser(settings);
    if (!token && DisableAnonymousClusterAccess_) {
        // do not use ythrow here for better error message
        throw yexception() << "Accessing YT cluster " << Cluster_ << " without OAuth token is not allowed";
    }

    return Session_->TxCache_.GetOrCreateEntry(YtServer_, token, impersonationUser, [s = Session_]() { return s->CreateSpecWithDesc(); }, settings);
}

TExpressionResorceUsage TExecContextBase::ScanExtraResourceUsageImpl(const TExprNode& node, const TYtSettings::TConstPtr& config, bool withInput) {
    auto extraUsage = ScanExtraResourceUsage(node, *config);
    if (withInput && AnyOf(InputTables_, [](const auto& input) { return input.Erasure; })) {
        if (auto codecCpu = config->ErasureCodecCpu.Get(Cluster_)) {
            extraUsage.Cpu *= *codecCpu;
        }
    }
    return extraUsage;
}

TString TExecContextBase::GetAuth(const TYtSettings::TConstPtr& config) const {
    auto auth = config->Auth.Get();
    if (!auth || auth->empty()) {
         auth = Clusters_->GetAuth(Cluster_);
    }

    return auth.GetOrElse(TString());
}

TMaybe<TString> TExecContextBase::GetImpersonationUser(const TYtSettings::TConstPtr& config) const {
    return config->_ImpersonationUser.Get();
}

ui64 TExecContextBase::EstimateLLVMMem(size_t nodes, const TString& llvmOpt, const TYtSettings::TConstPtr& config) const {
    ui64 memUsage = 0;
    if (llvmOpt != "OFF") {
        if (auto usage = config->LLVMMemSize.Get(Cluster_)) {
            memUsage += *usage;
        }
        if (auto usage = config->LLVMPerNodeMemSize.Get(Cluster_)) {
            memUsage += *usage * nodes;
        }
    }

    return memUsage;
}


} // NNative

} // NYql
