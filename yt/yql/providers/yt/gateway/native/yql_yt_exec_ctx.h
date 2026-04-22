#pragma once

#include "yql_yt_native.h"
#include "yql_yt_session.h"

#include <yt/yql/providers/yt/gateway/lib/exec_ctx.h>
#include <yt/yql/providers/yt/gateway/lib/query_cache.h>
#include <yt/yql/providers/yt/gateway/lib/user_files.h>

#include <yt/yql/providers/yt/common/yql_yt_settings.h>
#include <yt/yql/providers/yt/gateway/lib/yt_helpers.h>
#include <yt/yql/providers/yt/lib/url_mapper/yql_yt_url_mapper.h>
#include <yt/yql/providers/yt/lib/expr_traits/yql_expr_traits.h>

#include <yql/essentials/core/yql_user_data.h>
#include <yql/essentials/core/file_storage/file_storage.h>

#include <yt/cpp/mapreduce/interface/common.h>
#include <yt/cpp/mapreduce/common/helpers.h>
#include <library/cpp/yson/node/node.h>
#include <library/cpp/retry/retry.h>
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
        const IYtGateway::TPtr& gateway,
        const TYtNativeServices::TPtr& services,
        const TConfigClusters::TPtr& clusters,
        const TIntrusivePtr<NCommon::TMkqlCommonCallableCompiler>& mkqlCompiler,
        const TSession::TPtr& session,
        const TString& cluster,
        std::shared_ptr<TYtUrlMapper> urlMapper,
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
    void SetCache(const TVector<TString>& outTablePaths, const TVector<NYT::TNode>& outTableSpecs,
        const TString& tmpFolder, const TYtSettings::TConstPtr& settings, const TString& opHash, const TMaybe<TString>& OutputHash) override;

    void FillRichPathForPullCaseInput(NYT::TRichYPath& path, TYtTableBaseInfo::TPtr tableInfo) override;
    void FillRichPathForInput(NYT::TRichYPath& path, const TYtPathInfo& pathInfo, const TString& newPath, bool localChainTest) override;
    bool IsLocalChainTest() const override;

    NThreading::TFuture<void> MakeOperationWaiter(const NYT::IOperationPtr& op, const TMaybe<ui32>& publicId) const {
        if (const auto& opTracker = Session_->OpTracker_) {
            return opTracker->MakeOperationWaiter(op, publicId, YtServer_, Cluster_, Session_->ProgressWriter_, Session_->StatWriter_);
        }
        return NThreading::MakeErrorFuture<void>(std::make_exception_ptr(yexception() << "Cannot run operations in session without operation tracker"));
    }

    ui64 EstimateLLVMMem(size_t nodes, const TString& llvmOpt, const TYtSettings::TConstPtr& config) const;

    TExpressionResorceUsage ScanExtraResourceUsageImpl(const TExprNode& node, const TYtSettings::TConstPtr& config, bool withInput);

    void DumpFilesFromJob(const NYT::TNode& opSpec, const TYtSettings::TConstPtr& config) const;

    NThreading::TFuture<NThreading::TAsyncSemaphore::TPtr> AcquireOperationLock() {
        return Session_->OperationSemaphore->AcquireAsync();
    }

public:
    ISecretMasker::TPtr SecretMasker;
    TSession::TPtr Session_;
    TVector<std::pair<TString, TString>> CodeSnippets_;
    THolder<TYtQueryCacheItem> QueryCacheItem;
    bool DisableAnonymousClusterAccess_;
    bool Hidden = false;
    IMetricsRegistryPtr Metrics;
    IYtAccessProvider::TPtr YtAccessProvider;
    TOperationProgress::EOpBlockStatus BlockStatus = TOperationProgress::EOpBlockStatus::None;
    THashMap<TString, TString> JobFilesDumpPaths;  // yt job basename -> dump path
};


template <class T>
class TExecContext: public TExecContextBase {
public:
    using TPtr = ::TIntrusivePtr<TExecContext>;
    using TOptions = T;

    TExecContext(
        const IYtGateway::TPtr& gateway,
        const TYtNativeServices::TPtr services,
        const TConfigClusters::TPtr& clusters,
        const TIntrusivePtr<NCommon::TMkqlCommonCallableCompiler>& mkqlCompiler,
        TOptions&& options,
        const TSession::TPtr& session,
        const TString& cluster,
        std::shared_ptr<TYtUrlMapper> urlMapper,
        IMetricsRegistryPtr metrics)
        : TExecContextBase(gateway, services, clusters, mkqlCompiler, session, cluster, urlMapper, std::move(metrics))
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
        TExecContextBase::SetOutput(output, Options_.Config(), Options_.OperationHash(),
            Options_.OutputHash());
    }

    void SetSingleOutput(const TYtOutTableInfo& outTable) {
        TExecContextBase::SetSingleOutput(outTable, Options_.Config());
    }

    void SetCacheItem(const TVector<TString>& outTablePaths, const TVector<NYT::TNode>& outTableSpecs, const TString& tmpFolder) {
        TExecContextBase::SetCache(outTablePaths, outTableSpecs, tmpFolder, Options_.Config(), Options_.OperationHash(),
            Options_.OutputHash());
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
            YQL_ENSURE(Session_->UseSecureTmp_);

            auto future = NThreading::MakeFuture();
            if (Session_->UseSecureTmp_->load() && Options_.Config()->_SecureTmpRoot.Get(Cluster_)) {
                auto logCtx = NYql::NLog::CurrentLogContextPath();
                future = Session_->Async([this, logCtx] {
                    YQL_LOG_CTX_ROOT_SESSION_SCOPE(logCtx);
                    try {
                        PrepareSecureTmpFolder();
                        return NThreading::MakeFuture();
                    } catch (...) {
                        YQL_CLOG(ERROR, ProviderYt) << CurrentExceptionMessage();
                        return NThreading::MakeErrorFuture<void>(std::current_exception());
                    }
                });
            }

            return future.Apply([this](const NThreading::TFuture<void>& f) {
                f.GetValue();

                SetNodeExecProgress("Awaiting cache");
                return QueryCacheItem->LookupAsync(Session_->Queue_);
            });
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
                    if (self->Session_->FullCapture_) {
                        try {
                            auto attrs = op->GetAttributes(NYT::TGetOperationOptions().AttributeFilter(
                                NYT::TOperationAttributeFilter().Add(NYT::EOperationAttribute::Spec)
                            ));
                            YQL_ENSURE(attrs.Spec.Defined());
                            self->DumpFilesFromJob(*attrs.Spec, self->Options_.Config());
                        } catch (const std::exception& e) {
                            self->Session_->FullCapture_->ReportError(e);
                        }
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
                return TAsyncQueue::Async(queue, [unlock = std::move(unlock), f]() {
                    return unlock(f);
                });
            });
        });
    }

    void PrepareSecureTmpFolder() {
        YQL_ENSURE(Session_->UseSecureTmp_);
        if (!Session_->UseSecureTmp_->load() || !Options_.Config()->_SecureTmpRoot.Get(Cluster_)) {
            return;
        }

        NThreading::TFuture<void> future;
        with_lock(Session_->SecureTmpFolderPreparationsMutex_) {
            auto& futureRef = Session_->SecureTmpFolderPreparationsByCluster_[Cluster_];
            // Check for ongoing request for cluster
            if (!futureRef.Initialized()) {
                auto logCtx = NYql::NLog::CurrentLogContextPath();
                futureRef = Session_->Async([this, logCtx] {
                    YQL_LOG_CTX_ROOT_SESSION_SCOPE(logCtx);
                    try {
                        ExecPrepareSecureTmpFolder();
                        return NThreading::MakeFuture();
                    } catch (...) {
                        YQL_CLOG(ERROR, ProviderYt) << CurrentExceptionMessage();
                        return NThreading::MakeErrorFuture<void>(std::current_exception());
                    }
                });
            }

            future = futureRef;
        }

        future.GetValueSync();
    }

    TOptions Options_;

private:
    void ExecPrepareSecureTmpFolder() {
        YQL_ENSURE(!Session_->OperationOptions_.ProjectSlug, "Secure tmp for projects is not supported yet");

        auto secureTmpPath = NYT::AddPathPrefix(
            GetTablesTmpFolder(*Options_.Config(), Cluster_, Session_->UseSecureTmp_, Session_->OperationOptions_),
            NYT::TConfig::Get()->Prefix
        );

        const auto waitForAclDelay = Options_.Config()->_SecureTmpWaitForAclDelay.Get();
        const auto waitForAclMaxAttempts = Options_.Config()->_SecureTmpWaitForAclMaxAttempts.Get();
        YQL_ENSURE(waitForAclDelay && waitForAclMaxAttempts);
        const auto attributes = Options_.Config()->_SecureTmpAttributes.Get().GetOrElse(NYT::TNode::CreateMap());

        auto entry = GetEntry();
        entry->Client->Create(secureTmpPath, NYT::NT_MAP, NYT::TCreateOptions().IgnoreExisting(true).Attributes(attributes));

        auto retryPolicy = IRetryPolicy<bool>::GetFixedIntervalPolicy(
            /*retryClassFunction=*/ [](bool hasAccess) {
                return hasAccess ? ERetryErrorClass::NoRetry : ERetryErrorClass::ShortRetry;
            },
            /*delay=*/ *waitForAclDelay,
            /*longRetryDelay=*/ *waitForAclDelay,
            /*maxRetries=*/ *waitForAclMaxAttempts,
            /*maxTime=*/ TDuration::Max()
        );

        bool accessRequested = false;
        bool hasAccess = DoWithRetryOnRetCode<bool>([&]() {
            auto checkReadWrite = [&](const TString& user) {
                auto read = entry->Client->CheckPermission(user, NYT::EPermission::Read, secureTmpPath);
                auto write = entry->Client->CheckPermission(user, NYT::EPermission::Write, secureTmpPath);
                return read.Action == NYT::ESecurityAction::Allow && write.Action == NYT::ESecurityAction::Allow;
            };

            YQL_ENSURE(Session_->OperationOptions_.AuthenticatedUser);
            bool hasReadWrite = checkReadWrite(*Session_->OperationOptions_.AuthenticatedUser);
            if (hasReadWrite) {
                YQL_CLOG(INFO, ProviderYt) << "Using secure tmp folder " << secureTmpPath;
                return true;
            }

            // User doesn't have permissions on secure tmp
            if (!accessRequested) {
                if constexpr (NPrivate::THasPublicId<TOptions>::value) {
                    SetNodeExecProgress("Waiting for secure temporary folder");
                }

                RequestSecureTmpAccess(EIdentityType::User, secureTmpPath).GetValueSync();
                YQL_CLOG(INFO, ProviderYt) << "Requested permissions for secure tmp folder";
                accessRequested = true;
            }

            YQL_CLOG(INFO, ProviderYt) << "Waiting for secure tmp folder ACL";
            return false;
        }, retryPolicy);

        if (!hasAccess) {
            YQL_LOG_CTX_THROW TErrorException(TIssuesIds::YT_SECURE_DATA_IN_COMMON_TMP)
                << "Failed to create secure tmp folder on cluster " << Cluster_ << ". Aborting query to prevent sensitive data leak";
        }
    }

    NThreading::TFuture<void> RequestSecureTmpAccess(EIdentityType type, const TString& path) {
        auto logCtx = NYql::NLog::CurrentLogContextPath();
        return Session_->Async([this, logCtx, type, &path] {
            YQL_LOG_CTX_ROOT_SESSION_SCOPE(logCtx);
            try {
                YQL_CLOG(INFO, ProviderYt) << "Requesting permissions for secure tmp " << path << " for " << type << " for cluster " << Cluster_;
                YtAccessProvider->RequestAccess(Clusters_->GetYtName(Cluster_), type, path, Session_->UserName_, Session_->OperationOptions_);
                return NThreading::MakeFuture();
            } catch (...) {
                YQL_CLOG(ERROR, ProviderYt) << CurrentExceptionMessage();
                return NThreading::MakeErrorFuture<void>(std::current_exception());
            }
        });
    }
};

} // NNative

} // NYql
