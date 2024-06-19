#pragma once

#include "yql_yt_native.h"
#include "yql_yt_session.h"

#include <ydb/library/yql/providers/yt/lib/config_clusters/config_clusters.h>
#include <ydb/library/yql/providers/yt/gateway/lib/query_cache.h>
#include <ydb/library/yql/providers/yt/gateway/lib/user_files.h>

#include <ydb/library/yql/providers/yt/common/yql_yt_settings.h>
#include <ydb/library/yql/providers/yt/lib/url_mapper/yql_yt_url_mapper.h>
#include <ydb/library/yql/providers/yt/lib/expr_traits/yql_expr_traits.h>
#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <ydb/library/yql/providers/yt/provider/yql_yt_table.h>

#include <ydb/library/yql/providers/common/mkql/yql_provider_mkql.h>

#include <ydb/library/yql/core/yql_user_data.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/file_storage/file_storage.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/utils/log/context.h>

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

struct TInputInfo {
    TInputInfo() = default;
    TInputInfo(const TString& name, const NYT::TRichYPath& path, bool temp, bool strict, const TYtTableBaseInfo& info, const NYT::TNode& spec, ui32 group = 0)
        : Name(name)
        , Path(path)
        , Temp(temp)
        , Dynamic(info.Meta->IsDynamic)
        , Strict(strict)
        , Records(info.Stat->RecordsCount)
        , DataSize(info.Stat->DataSize)
        , Spec(spec)
        , Group(group)
        , Lookup(info.Meta->Attrs.Value("optimize_for", "scan") != "scan")
        , Erasure(info.Meta->Attrs.Value("erasure_codec", "none") != "none")
    {
    }

    TString Name;
    NYT::TRichYPath Path;
    bool Temp = false;
    bool Dynamic = false;
    bool Strict = true;
    ui64 Records = 0;
    ui64 DataSize = 0;
    NYT::TNode Spec;
    NYT::TNode QB2Premapper;
    ui32 Group = 0;
    bool Lookup = false;
    bool Erasure = false;
};

struct TOutputInfo {
    TOutputInfo() = default;
    TOutputInfo(const TString& name, const TString& path, const NYT::TNode& codecSpec, const NYT::TNode& attrSpec,
        const NYT::TSortColumns& sortedBy, NYT::TNode columnGroups)
        : Name(name)
        , Path(path)
        , Spec(codecSpec)
        , AttrSpec(attrSpec)
        , SortedBy(sortedBy)
        , ColumnGroups(std::move(columnGroups))
    {
    }
    TString Name;
    TString Path;
    NYT::TNode Spec;
    NYT::TNode AttrSpec;
    NYT::TSortColumns SortedBy;
    NYT::TNode ColumnGroups;
};

class TExecContextBase: public TThrRefBase {
protected:
    TExecContextBase(const TYtNativeServices& services,
        const TConfigClusters::TPtr& clusters,
        const TIntrusivePtr<NCommon::TMkqlCommonCallableCompiler>& mkqlCompiler,
        const TSession::TPtr& session,
        const TString& cluster,
        const TYtUrlMapper& urlMapper);

public:
    TString GetInputSpec(bool ensureOldTypesOnly, ui64 nativeTypeCompatibilityFlags, bool intermediateInput) const;
    TString GetOutSpec(bool ensureOldTypesOnly, ui64 nativeTypeCompatibilityFlags) const;
    TString GetOutSpec(size_t beginIdx, size_t endIdx, NYT::TNode initialOutSpec, bool ensureOldTypesOnly, ui64 nativeTypeCompatibilityFlags) const;

    TTransactionCache::TEntry::TPtr GetEntry() const {
        return Session_->TxCache_.GetEntry(YtServer_);
    }

    TTransactionCache::TEntry::TPtr TryGetEntry() const {
        return Session_->TxCache_.TryGetEntry(YtServer_);
    }

    TTransactionCache::TEntry::TPtr GetOrCreateEntry(const TYtSettings::TConstPtr& settings) const;

protected:
    void MakeUserFiles(const TUserDataTable& userDataBlocks);

    void SetInput(NNodes::TExprBase input, bool forcePathColumns, const THashSet<TString>& extraSysColumns, const TYtSettings::TConstPtr& settings);
    void SetOutput(NNodes::TYtOutSection output, const TYtSettings::TConstPtr& settings, const TString& opHash);
    void SetSingleOutput(const TYtOutTableInfo& outTable, const TYtSettings::TConstPtr& settings);
    void SetCacheItem(const TVector<TString>& outTablePaths, const TVector<NYT::TNode>& outTableSpecs,
        const TString& tmpFolder, const TYtSettings::TConstPtr& settings, const TString& opHash);

    template <class TTableType>
    static TString GetSpecImpl(const TVector<TTableType>& tables, size_t beginIdx, size_t endIdx, NYT::TNode initialOutSpec, bool ensureOldTypesOnly, ui64 nativeTypeCompatibilityFlags, bool intermediateInput);

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
    const NKikimr::NMiniKQL::IFunctionRegistry* FunctionRegistry_ = nullptr;
    TFileStoragePtr FileStorage_;
    TYtGatewayConfigPtr Config_;
    TConfigClusters::TPtr Clusters_;
    TIntrusivePtr<NCommon::TMkqlCommonCallableCompiler> MkqlCompiler_;
    TSession::TPtr Session_;
    TString Cluster_;
    TString YtServer_;
    TUserFiles::TPtr UserFiles_;
    TVector<std::pair<TString, TString>> CodeSnippets_;
    std::pair<TString, TString> LogCtx_;
    TVector<TInputInfo> InputTables_;
    bool YamrInput = false;
    TMaybe<TSampleParams> Sampling;
    TVector<TOutputInfo> OutTables_;
    THolder<TYtQueryCacheItem> QueryCacheItem;
    const TYtUrlMapper& UrlMapper_;
    bool DisableAnonymousClusterAccess_;
    bool Hidden = false;
};


template <class T>
class TExecContext: public TExecContextBase {
public:
    using TPtr = ::TIntrusivePtr<TExecContext>;
    using TOptions = T;

    TExecContext(const TYtNativeServices& services,
        const TConfigClusters::TPtr& clusters,
        const TIntrusivePtr<NCommon::TMkqlCommonCallableCompiler>& mkqlCompiler,
        TOptions&& options,
        const TSession::TPtr& session,
        const TString& cluster,
        const TYtUrlMapper& urlMapper)
        : TExecContextBase(services, clusters, mkqlCompiler, session, cluster, urlMapper)
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
        TExecContextBase::SetCacheItem(outTablePaths, outTableSpecs, tmpFolder, Options_.Config(), Options_.OperationHash());
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
