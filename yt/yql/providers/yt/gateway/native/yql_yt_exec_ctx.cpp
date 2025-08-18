#include "yql_yt_exec_ctx.h"

#include "yql_yt_spec.h"

#include <yt/yql/providers/yt/gateway/lib/yt_helpers.h>
#include <yt/yql/providers/yt/provider/yql_yt_op_settings.h>
#include <yt/yql/providers/yt/provider/yql_yt_table.h>
#include <yt/yql/providers/yt/codec/yt_codec.h>
#include <yt/yql/providers/yt/lib/schema/schema.h>
#include <yt/yql/providers/yt/common/yql_names.h>
#include <yt/yql/providers/yt/common/yql_configuration.h>
#include <yql/essentials/providers/common/codec/yql_codec_type_flags.h>
#include <yql/essentials/providers/common/proto/gateways_config.pb.h>

#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/yql_panic.h>

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

TExecContextBase::TExecContextBase(
    const TYtNativeServices::TPtr& services,
    const TConfigClusters::TPtr& clusters,
    const TIntrusivePtr<NCommon::TMkqlCommonCallableCompiler>& mkqlCompiler,
    const TSession::TPtr& session,
    const TString& cluster,
    const TYtUrlMapper& urlMapper,
    IMetricsRegistryPtr metrics)
    : TExecContextBaseSimple(services, clusters, mkqlCompiler, cluster, session)
    , FileStorage_(services->FileStorage)
    , SecretMasker(services->SecretMasker)
    , Session_(session)
    , UrlMapper_(urlMapper)
    , DisableAnonymousClusterAccess_(services->DisableAnonymousClusterAccess)
    , Hidden(session->SessionId_.EndsWith("_hidden"))
    , Metrics(std::move(metrics))
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

void TExecContextBase::SetCache(const TVector<TString>& outTablePaths, const TVector<NYT::TNode>& outTableSpecs,
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

void TExecContextBase::FillRichPathForPullCaseInput(NYT::TRichYPath& richYPath, TYtTableBaseInfo::TPtr tableInfo) {
    if (tableInfo->Cluster != Cluster_) {
        richYPath.Cluster(Clusters_->GetYtName(tableInfo->Cluster));
        if (!Config_->GetLocalChainTest()) {
            auto pathEntry = GetEntryForCluster(tableInfo->Cluster);
            richYPath.TransactionId(pathEntry->Tx->GetId());
        }
    }
}

void TExecContextBase::FillRichPathForInput(NYT::TRichYPath& richYPath, const TYtPathInfo& pathInfo, const TString& newPath, bool localChainTest) {
    const TString pathCluster = pathInfo.Table->Cluster;
    if (localChainTest || (pathInfo.Table->IsTemp && !pathInfo.Table->IsAnonymous)) {
        richYPath.Path(newPath);
        if (pathCluster != Cluster_ && !localChainTest) {
            richYPath.TransactionId(GetEntryForCluster(pathCluster)->Tx->GetId());
        }
    } else {
        auto entry = GetEntryForCluster(pathCluster);
        with_lock(entry->Lock_) {
            auto p = entry->Snapshots.FindPtr(std::make_pair(newPath, pathInfo.Table->Epoch.GetOrElse(0)));
            YQL_ENSURE(p, "Table " << Cluster_ << "." << pathInfo.Table->Name.Quote() << " has no snapshot");
            richYPath.Path(std::get<0>(*p)).TransactionId(std::get<1>(*p)).OriginalPath(NYT::AddPathPrefix(newPath, NYT::TConfig::Get()->Prefix));
        }
    }
}

bool TExecContextBase::IsLocalChainTest() const {
    return Config_->GetLocalChainTest();
}


TTransactionCache::TEntry::TPtr TExecContextBase::GetOrCreateEntry(const TYtSettings::TConstPtr& settings) const {
    auto token = GetAuth(settings);
    auto impersonationUser = GetImpersonationUser(settings);
    if (!token && DisableAnonymousClusterAccess_) {
        YQL_LOG_CTX_THROW TErrorException(TIssuesIds::YT_ACCESS_DENIED) <<
            "Accessing YT cluster " << Cluster_.Quote() << " without OAuth token is not allowed";
    }

    return Session_->TxCache_.GetOrCreateEntry(Cluster_, YtServer_, token, impersonationUser, [s = Session_]() { return s->CreateSpecWithDesc(); }, settings, Metrics);
}

TExpressionResorceUsage TExecContextBase::ScanExtraResourceUsageImpl(const TExprNode& node, const TYtSettings::TConstPtr& config, bool withInput) {
    auto extraUsage = ScanExtraResourceUsage(node, *config);
    if (withInput && AnyOf(InputTables_, [](const auto& input) { return input.ErasureCodec != "none"_sb; })) {
        if (auto codecCpu = config->ErasureCodecCpu.Get(Cluster_)) {
            extraUsage.Cpu *= *codecCpu;
            YQL_CLOG(DEBUG, ProviderYt) << "Increase cpu for erasure input by " << *codecCpu;
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
