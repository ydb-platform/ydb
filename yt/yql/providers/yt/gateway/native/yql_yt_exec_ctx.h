#pragma once

#include "yql_yt_native.h"
#include "yql_yt_session.h"

#include <yt/yql/providers/yt/gateway/lib/exec_ctx.h>
#include <yt/yql/providers/yt/gateway/lib/query_cache.h>
#include <yt/yql/providers/yt/gateway/lib/user_files.h>

#include <yt/yql/providers/yt/common/yql_yt_settings.h>
#include <yt/yql/providers/yt/lib/url_mapper/yql_yt_url_mapper.h>
#include <yt/yql/providers/yt/lib/expr_traits/yql_expr_traits.h>

#include <yql/essentials/core/yql_user_data.h>
#include <yql/essentials/core/file_storage/file_storage.h>

#include <yt/cpp/mapreduce/interface/common.h>
#include <library/cpp/yson/node/node.h>
#include <library/cpp/threading/future/future.h>
#include <library/cpp/threading/future/async.h>

#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/size_literals.h>
#include <util/generic/hash_set.h>

#include <utility>


namespace NYql {

namespace NNative {

class TExecContextBase: public TExecContextBaseSimple {
protected:
    TExecContextBase(
        const TYtNativeServices::TPtr& services,
        const TConfigClusters::TPtr& clusters,
        const TIntrusivePtr<NCommon::TMkqlCommonCallableCompiler>& mkqlCompiler,
        const TSession::TPtr& session,
        const TString& cluster,
        const TYtUrlMapper& urlMapper,
        IMetricsRegistryPtr metrics
    );

public:
    TTransactionCache::TEntry::TPtr GetEntry() const {
        return Session_->TxCache_.GetEntry(YtServer_);
    }

    TTransactionCache::TEntry::TPtr GetEntryForCluster(const TString& cluster) const {
        return Session_->TxCache_.GetEntry(Clusters_->GetServer(cluster));
    }

    TTransactionCache::TEntry::TPtr TryGetEntry() const {
        return Session_->TxCache_.TryGetEntry(YtServer_);
    }

    TTransactionCache::TEntry::TPtr GetOrCreateEntry(const TYtSettings::TConstPtr& settings) const;

protected:
    void MakeUserFiles(const TUserDataTable& userDataBlocks);

    void SetCache(const TVector<TString>& outTablePaths, const TVector<NYT::TNode>& outTableSpecs,
        const TString& tmpFolder, const TYtSettings::TConstPtr& settings, const TString& opHash) override;

    void FillRichPathForPullCaseInput(NYT::TRichYPath& path, TYtTableBaseInfo::TPtr tableInfo) override;
    void FillRichPathForInput(NYT::TRichYPath& path, const TYtPathInfo& pathInfo, const TString& newPath, bool localChainTest) override;
    bool IsLocalChainTest() const override;

    NThreading::TFuture<void> MakeOperationWaiter(const NYT::IOperationPtr& op, const TMaybe<ui32>& publicId) const {
        if (const auto& opTracker = Session_->OpTracker_) {
            return opTracker->MakeOperationWaiter(op, publicId, YtServer_, Cluster_, Session_->ProgressWriter_, Session_->StatWriter_);
        }
        return NThreading::MakeErrorFuture<void>(std::make_exception_ptr(yexception() << "Cannot run operations in session without operation tracker"));
    }

    TString GetAuth(const TYtSettings::TConstPtr& config) const;
    TMaybe<TString> GetImpersonationUser(const TYtSettings::TConstPtr& config) const;

    ui64 EstimateLLVMMem(size_t nodes, const TString& llvmOpt, const TYtSettings::TConstPtr& config) const;

    TExpressionResorceUsage ScanExtraResourceUsageImpl(const TExprNode& node, const TYtSettings::TConstPtr& config, bool withInput);

    NThreading::TFuture<NThreading::TAsyncSemaphore::TPtr> AcquireOperationLock() {
        return Session_->OperationSemaphore->AcquireAsync();
    }

public:
    TFileStoragePtr FileStorage_;
    ISecretMasker::TPtr SecretMasker;
    TSession::TPtr Session_;
    TString YtServer_;
    TUserFiles::TPtr UserFiles_;
    TVector<std::pair<TString, TString>> CodeSnippets_;
    std::pair<TString, TString> LogCtx_;
    THolder<TYtQueryCacheItem> QueryCacheItem;
    const TYtUrlMapper& UrlMapper_;
    bool DisableAnonymousClusterAccess_;
    bool Hidden = false;
    IMetricsRegistryPtr Metrics;
    TOperationProgress::EOpBlockStatus BlockStatus = TOperationProgress::EOpBlockStatus::None;
};


template <class T>
class TExecContext: public TExecContextBase {
public:
    using TPtr = ::TIntrusivePtr<TExecContext>;
    using TOptions = T;

    TExecContext(
        const TYtNativeServices::TPtr services,
        const TConfigClusters::TPtr& clusters,
        const TIntrusivePtr<NCommon::TMkqlCommonCallableCompiler>& mkqlCompiler,
        TOptions&& options,
        const TSession::TPtr& session,
        const TString& cluster,
        const TYtUrlMapper& urlMapper,
        IMetricsRegistryPtr metrics)
        : TExecContextBase(services, clusters, mkqlCompiler, session, cluster, urlMapper, std::move(metrics))
        , Options_(std::move(options))
    {
    }

    void MakeUserFiles() {
        TExecContextBase::MakeUserFiles(Options_.UserDataBlocks());
    }

    void SetInput(NNodes::TExprBase input, bool forcePathColumns, const THashSet<TString>& extraSysColumns) {
        TExecContextBase::SetInput(input, forcePathColumns, extraSysColumns, Options_.Config());
    }

    void SetOutput(NNodes::TYtOutSection output) {
        TExecContextBase::SetOutput(output, Options_.Config(), Options_.OperationHash());
    }

    void SetSingleOutput(const TYtOutTableInfo& outTable) {
        TExecContextBase::SetSingleOutput(outTable, Options_.Config());
    }

    void SetCacheItem(const TVector<TString>& outTablePaths, const TVector<NYT::TNode>& outTableSpecs, const TString& tmpFolder) {
        TExecContextBase::SetCache(outTablePaths, outTableSpecs, tmpFolder, Options_.Config(), Options_.OperationHash());
    }

    TExpressionResorceUsage ScanExtraResourceUsage(const TExprNode& node, bool withInput) {
        return TExecContextBase::ScanExtraResourceUsageImpl(node, Options_.Config(), withInput);
    }

    TTransactionCache::TEntry::TPtr GetOrCreateEntry() const {
        return TExecContextBase::GetOrCreateEntry(Options_.Config());
    }

    TString GetAuth() const {
        return TExecContextBase::GetAuth(Options_.Config());
    }

    ui64 EstimateLLVMMem(size_t nodes) const {
        return TExecContextBase::EstimateLLVMMem(nodes, Options_.OptLLVM(), Options_.Config());
    }

    void SetNodeExecProgress(const TString& stage) {
        auto publicId = Options_.PublicId();
        if (!publicId) {
            return;
        }
        auto progress = TOperationProgress(TString(YtProviderName), *publicId,
            TOperationProgress::EState::InProgress, stage);
        Session_->ProgressWriter_(progress);
    }

    void ReportNodeBlockStatus() const {
        auto publicId = Options_.PublicId();
        if (!publicId) {
            return;
        }

        YQL_CLOG(INFO, ProviderYt) << "Reporting " << BlockStatus << " block status for node #" << *publicId;
        auto progress = TOperationProgress(TString(YtProviderName), *publicId,
            TOperationProgress::EState::InProgress);
        progress.BlockStatus = BlockStatus;
        Session_->ProgressWriter_(progress);
    }

    [[nodiscard]]
    NThreading::TFuture<bool> LookupQueryCacheAsync() {
        if (QueryCacheItem) {
            SetNodeExecProgress("Awaiting cache");
            return QueryCacheItem->LookupAsync(Session_->Queue_);
        }
        return NThreading::MakeFuture(false);
    }

    void StoreQueryCache() {
        if (QueryCacheItem) {
            SetNodeExecProgress("Storing to cache");
            QueryCacheItem->Store();
        }
    }

    template <bool WithState = true>
    [[nodiscard]]
    NThreading::TFuture<void> RunOperation(std::function<NYT::IOperationPtr()>&& opFactory) {
        if constexpr (WithState) {
            SetNodeExecProgress("Waiting for concurrency limit");
        }
        Session_->EnsureInitializedSemaphore(Options_.Config());
        return TExecContextBase::AcquireOperationLock().Apply([opFactory = std::move(opFactory), self = TIntrusivePtr(this)](const auto& f) {
            YQL_LOG_CTX_ROOT_SESSION_SCOPE(self->LogCtx_);
            auto lock = f.GetValue()->MakeAutoRelease();
            auto op = opFactory();
            op->GetPreparedFuture().Subscribe([op, self](const auto& f) {
                YQL_LOG_CTX_ROOT_SESSION_SCOPE(self->LogCtx_);
                if (!f.HasException()) {
                    if constexpr (WithState) {
                        self->SetNodeExecProgress("Starting operation");
                    }
                    try {
                        op->Start();
                    } catch (...) {
                        // Promise will be initialized with exception inside of TOperation::Start()
                    }
                }
            });
            // opFactory factory may contain locked resources. Explicitly wait preparation before destroying it
            op->GetPreparedFuture().GetValueSync();

            NThreading::TFuture<void> res;
            if constexpr (WithState) {
                res = self->TExecContextBase::MakeOperationWaiter(op, self->Options_.PublicId());
            } else {
                res = self->TExecContextBase::MakeOperationWaiter(op, Nothing());
            }
            return res.Apply([queue = self->Session_->Queue_, unlock = lock.DeferRelease()](const auto& f) {
                if (f.HasException()) {
                    unlock(f);
                    f.TryRethrow();
                }
                return queue->Async([unlock = std::move(unlock), f]() {
                    return unlock(f);
                });
            });
        });
    }

    TOptions Options_;
};

} // NNative

} // NYql
