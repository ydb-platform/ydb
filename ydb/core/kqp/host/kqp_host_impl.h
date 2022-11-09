#pragma once

#include "kqp_host.h"

#include <ydb/core/kqp/common/kqp_transform.h>
#include <ydb/core/kqp/prepare/kqp_prepare.h>

namespace NKikimr {
namespace NKqp {

Ydb::Table::QueryStatsCollection::Mode GetStatsMode(NYql::EKikimrStatsMode statsMode);

template<typename TResult, bool copyIssues = true>
class TKqpAsyncResultBase : public NYql::IKikimrAsyncResult<TResult> {
public:
    using TTransformStatusCallback = std::function<void(const NYql::IGraphTransformer::TStatus&)>;

public:
    TKqpAsyncResultBase(const NYql::TExprNode::TPtr& exprRoot, NYql::TExprContext& exprCtx,
        NYql::IGraphTransformer& transformer, TTransformStatusCallback transformCallback = {})
        : ExprRoot(exprRoot)
        , ExprCtx(exprCtx)
        , Transformer(transformer)
        , TransformCallback(transformCallback) {}

    bool HasResult() const override {
        if (TransformFinished()) {
            return TransformCallbackDone || !TransformCallback;
        }

        return false;
    }

    TResult GetResult() override {
        YQL_ENSURE(HasResult());

        if (Status.GetValue() == NYql::IGraphTransformer::TStatus::Error) {
            return NYql::NCommon::ResultFromErrors<TResult>(ExprCtx.IssueManager.GetIssues());
        }

        YQL_ENSURE(Status.GetValue() == NYql::IGraphTransformer::TStatus::Ok);

        TResult result;
        result.ProtobufArenaPtr.reset(new google::protobuf::Arena());

        result.SetSuccess();

        if (copyIssues) {
            result.AddIssues(ExprCtx.IssueManager.GetIssues());
        }
        FillResult(result);
        return std::move(result);
    }

    NThreading::TFuture<bool> Continue() override {
        if (TransformFinished()) {
            if (TransformCallback && !TransformCallbackDone) {
                TransformCallback(Status.GetValue());
                TransformCallbackDone = true;
            }

            return NThreading::MakeFuture<bool>(true);
        }

        Status = NThreading::NewPromise<NYql::IGraphTransformer::TStatus>();

        auto resultPromise = NThreading::NewPromise<bool>();
        auto statusPromise = Status;
        bool hasCallback = !!TransformCallback;

        AsyncTransform(Transformer, ExprRoot, ExprCtx, Started,
            [resultPromise, statusPromise, hasCallback](const NYql::IGraphTransformer::TStatus& status) mutable {
                bool transformFinished = status != NYql::IGraphTransformer::TStatus::Async;
                bool finished = transformFinished && !hasCallback;

                statusPromise.SetValue(status);
                resultPromise.SetValue(finished);
            });

        Started = true;
        return resultPromise.GetFuture();
    }

protected:
    virtual void FillResult(TResult& result) const = 0;

    NYql::TExprNode::TPtr GetExprRoot() const { return ExprRoot; }
    NYql::TExprContext& GetExprContext() const { return ExprCtx; }
    NYql::IGraphTransformer& GetTransformer() const { return Transformer; }

private:
    bool TransformFinished() const {
        return Status.HasValue() && Status.GetValue() != NYql::IGraphTransformer::TStatus::Async;
    }

private:
    NYql::TExprNode::TPtr ExprRoot;
    NYql::TExprContext& ExprCtx;
    NYql::IGraphTransformer& Transformer;
    bool Started = false;
    TTransformStatusCallback TransformCallback;
    bool TransformCallbackDone = false;
    NThreading::TPromise<NYql::IGraphTransformer::TStatus> Status;
};


template<typename TResult, typename TApplyResult>
class TKqpAsyncApplyResult : public NYql::IKikimrAsyncResult<TApplyResult> {
public:
    using TCallback = std::function<TIntrusivePtr<NYql::IKikimrAsyncResult<TApplyResult>>(TResult&&)>;

    TKqpAsyncApplyResult(TIntrusivePtr<NYql::IKikimrAsyncResult<TResult>> result, const TCallback& callback)
        : Result(result)
        , Callback(callback)
    {
        YQL_ENSURE(Result);
        YQL_ENSURE(Callback);
    }

    bool HasResult() const override {
        if (!ApplyResult) {
            return false;
        }

        return ApplyResult->HasResult();
    }

    TApplyResult GetResult() override {
        YQL_ENSURE(HasResult());

        return std::move(ApplyResult->GetResult());
    }

    NThreading::TFuture<bool> Continue() override {
        if (!ApplyResult) {
            if (Result->HasResult()) {
                ApplyResult = Callback(Result->GetResult());
            } else {
                return Result->Continue().Apply([](const NThreading::TFuture<bool>& future) {
                    YQL_ENSURE(future.HasValue());
                    return NThreading::MakeFuture<bool>(false);
                });
            }
        }

        return ApplyResult->Continue();
    }
private:
    TIntrusivePtr<NYql::IKikimrAsyncResult<TResult>> Result;
    TIntrusivePtr<NYql::IKikimrAsyncResult<TApplyResult>> ApplyResult;
    TCallback Callback;
};

template<typename TItem, typename TExecResult>
class TKqpAsyncExecAllResult : public NYql::IKikimrAsyncResult<TExecResult> {
public:
    using TCallback = std::function<TIntrusivePtr<NYql::IKikimrAsyncResult<TExecResult>>(TItem&&)>;

    TKqpAsyncExecAllResult(const TVector<TItem>& items, const TCallback& callback)
        : Items(items)
        , Callback(callback)
    {
        YQL_ENSURE(Callback);
    }

    bool HasResult() const override {
        return CurrentIndex >= Items.size();
    }

    TExecResult GetResult() override {
        YQL_ENSURE(HasResult());

        TExecResult execResult;
        if (Success) {
            execResult.SetSuccess();
        }

        execResult.AddIssues(Issues);

        return std::move(execResult);
    }

    NThreading::TFuture<bool> Continue() override {
        if (HasResult()) {
            return NThreading::MakeFuture<bool>(true);
        }

        if (!CurrentResult) {
            CurrentResult.Reset(Callback(std::move(Items[CurrentIndex])));
        }

        if (CurrentResult->HasResult()) {
            auto result = CurrentResult->GetResult();
            Success = Success && result.Success();
            Issues.AddIssues(result.Issues());

            ++CurrentIndex;
            CurrentResult.Reset();

            return NThreading::MakeFuture<bool>(false);
        }

        return CurrentResult->Continue().Apply(
            [](const NThreading::TFuture<bool>& future) {
                YQL_ENSURE(future.HasValue());
                return NThreading::MakeFuture<bool>(false);
            });;
    }

private:
    TVector<TItem> Items;
    TCallback Callback;

    ui32 CurrentIndex= 0;
    TIntrusivePtr<NYql::IKikimrAsyncResult<TExecResult>> CurrentResult;
    bool Success = true;
    NYql::TIssues Issues;
};

template<typename TResult, typename TApplyResult>
TIntrusivePtr<NYql::IKikimrAsyncResult<TApplyResult>> AsyncApplyResult(
    TIntrusivePtr<NYql::IKikimrAsyncResult<TResult>> result,
    const typename TKqpAsyncApplyResult<TResult, TApplyResult>::TCallback& callback)
{
    return MakeIntrusive<TKqpAsyncApplyResult<TResult, TApplyResult>>(result, callback);
}

template<typename TResult, bool copyIssues = true>
class TKqpAsyncExecuteResultBase : public TKqpAsyncResultBase<TResult, copyIssues> {
public:
    TKqpAsyncExecuteResultBase(const NYql::TExprNode::TPtr& exprRoot, NYql::TExprContext& exprCtx,
        NYql::IGraphTransformer& transformer, TIntrusivePtr<NYql::TKikimrTransactionContextBase> txCtx)
        : TKqpAsyncResultBase<TResult, copyIssues>(exprRoot, exprCtx, transformer,
            [txCtx](const auto& status) {
                if (txCtx) {
                    auto& kqpTxCtx = static_cast<TKqpTransactionContext&>(*txCtx);
                    if (status == NYql::IGraphTransformer::TStatus::Ok) {
                        kqpTxCtx.OnEndQuery();
                    } else {
                        kqpTxCtx.Invalidate();
                    }
                }
            }) {}
};

inline bool NeedSnapshot(const TKqpTransactionContext& txCtx, const NYql::TKikimrConfiguration& config, bool rollbackTx,
    bool commitTx, const NKqpProto::TKqpPhyQuery& physicalQuery)
{
    if (*txCtx.EffectiveIsolationLevel != NKikimrKqp::ISOLATION_LEVEL_SERIALIZABLE)
        return false;

    if (!config.FeatureFlags.GetEnableMvccSnapshotReads())
        return false;

    if (txCtx.GetSnapshot().IsValid())
        return false;

    if (rollbackTx)
        return false;
    if (!commitTx)
        return true;

    size_t readPhases = 0;
    bool hasEffects = false;

    for (const auto &tx : physicalQuery.GetTransactions()) {
        switch (tx.GetType()) {
            case NKqpProto::TKqpPhyTx::TYPE_COMPUTE:
                // ignore pure computations
                break;

            default:
                ++readPhases;
                break;
        }

        if (tx.GetHasEffects()) {
            hasEffects = true;
        }
    }

    // We don't want snapshot when there are effects at the moment,
    // because it hurts performance when there are multiple single-shard
    // reads and a single distributed commit. Taking snapshot costs
    // similar to an additional distributed transaction, and it's very
    // hard to predict when that happens, causing performance
    // degradation.
    if (hasEffects) {
        return false;
    }

    // We need snapshot when there are multiple table read phases, most
    // likely it involves multiple tables and we would have to use a
    // distributed commit otherwise. Taking snapshot helps as avoid TLI
    // for read-only transactions, and costs less than a final distributed
    // commit.
    return readPhases > 1;
}


class IKqpRunner : public TThrRefBase {
public:
    using TQueryResult = NYql::IKikimrGateway::TQueryResult;
    using TAsyncQueryResult = NYql::IKikimrAsyncResult<TQueryResult>;

    /* Data queries */
    virtual TIntrusivePtr<TAsyncQueryResult> PrepareDataQuery(const TString& cluster,
        const NYql::TExprNode::TPtr& query, NYql::TExprContext& ctx,
        const NYql::IKikimrQueryExecutor::TExecuteSettings& settings) = 0;

    virtual TIntrusivePtr<TAsyncQueryResult> PrepareScanQuery(const TString& cluster,
        const NYql::TExprNode::TPtr& query, NYql::TExprContext& ctx,
        const NYql::IKikimrQueryExecutor::TExecuteSettings& settings) = 0;

    virtual TIntrusivePtr<TAsyncQueryResult> ExecutePreparedQueryNewEngine(const TString& cluster,
        const NYql::TExprNode::TPtr& world, std::shared_ptr<const NKqpProto::TKqpPhyQuery>&& phyQuery,
        NYql::TExprContext& ctx, const NYql::IKikimrQueryExecutor::TExecuteSettings& settings) = 0;

    virtual TIntrusivePtr<TAsyncQueryResult> ExecutePreparedScanQuery(const TString& cluster,
        const NYql::TExprNode::TPtr& world, std::shared_ptr<const NKqpProto::TKqpPhyQuery>&& phyQuery,
        NYql::TExprContext& ctx, const NActors::TActorId& target) = 0;
};

TIntrusivePtr<IKqpRunner> CreateKqpRunner(TIntrusivePtr<IKqpGateway> gateway, const TString& cluster,
    TIntrusivePtr<NYql::TTypeAnnotationContext> typesCtx, TIntrusivePtr<NYql::TKikimrSessionContext> sessionCtx,
    const NMiniKQL::IFunctionRegistry& funcRegistry);

TAutoPtr<NYql::IGraphTransformer> CreateKqpAcquireMvccSnapshotTransformer(TIntrusivePtr<IKqpGateway> gateway,
    TIntrusivePtr<TKqpTransactionState> txState, TIntrusivePtr<TKqlTransformContext> transformCtx);

TAutoPtr<NYql::IGraphTransformer> CreateKqpExecutePhysicalDataTransformer(TIntrusivePtr<IKqpGateway> gateway,
    const TString& cluster, TIntrusivePtr<TKqpTransactionState> txState,
    TIntrusivePtr<TKqlTransformContext> transformCtx);

TAutoPtr<NYql::IGraphTransformer> CreateKqpExplainPreparedTransformer(TIntrusivePtr<IKqpGateway> gateway,
    const TString& cluster, TIntrusivePtr<TKqlTransformContext> transformCtx);

TAutoPtr<NYql::IGraphTransformer> CreateKqpExecuteScanTransformer(TIntrusivePtr<IKqpGateway> gateway,
    const TString& cluster, TIntrusivePtr<TKqpTransactionState> txState,
    TIntrusivePtr<TKqlTransformContext> transformCtx);

TAutoPtr<NYql::IGraphTransformer> CreateKqpCreateSnapshotTransformer(TIntrusivePtr<IKqpGateway> gateway,
    TIntrusivePtr<TKqlTransformContext> transformCtx, TIntrusivePtr<TKqpTransactionState> txState);

TAutoPtr<NYql::IGraphTransformer> CreateKqpReleaseSnapshotTransformer(TIntrusivePtr<IKqpGateway> gateway,
    TIntrusivePtr<TKqpTransactionState> txState);

} // namespace NKqp
} // namespace NKikimr
