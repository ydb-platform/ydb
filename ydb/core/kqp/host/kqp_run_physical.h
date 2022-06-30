#pragma once

#include <ydb/core/kqp/common/kqp_gateway.h>
#include <ydb/core/kqp/expr_nodes/kqp_expr_nodes.h>
#include <ydb/core/kqp/prepare/kqp_prepare.h>
#include <ydb/core/tx/long_tx_service/public/lock_handle.h>

#include <ydb/library/yql/core/yql_graph_transformer.h>

namespace NKikimr {
namespace NKqp {

enum class TKqpExecuteFlag : ui32 {
    Results  = 1 << 0,
    Commit   = 1 << 1,
    Rollback = 1 << 2,
};

Y_DECLARE_FLAGS(TKqpExecuteFlags, TKqpExecuteFlag);
Y_DECLARE_OPERATORS_FOR_FLAGS(TKqpExecuteFlags);

class TKqpExecutePhysicalTransformerBase : public NYql::TGraphTransformerBase {
public:
    TKqpExecutePhysicalTransformerBase(TIntrusivePtr<IKqpGateway> gateway, const TString& cluster,
        TIntrusivePtr<TKqpTransactionState> txState, TIntrusivePtr<TKqlTransformContext> transformCtx)
        : Gateway(gateway)
        , Cluster(cluster)
        , TxState(txState)
        , TransformCtx(transformCtx)
        , CurrentTxIndex(0)
        , TransformState(std::make_shared<TTransformState>(txState)) {}


    TStatus DoTransform(NYql::TExprNode::TPtr input, NYql::TExprNode::TPtr& output, NYql::TExprContext& ctx) override;

    NThreading::TFuture<void> DoGetAsyncFuture(const NYql::TExprNode& input) override;

    TStatus DoApplyAsyncChanges(NYql::TExprNode::TPtr input, NYql::TExprNode::TPtr& output,
        NYql::TExprContext& ctx) override;

    void Rewind() override;

protected:
    virtual TStatus DoExecute(std::shared_ptr<const NKqpProto::TKqpPhyTx> tx, bool commit, NYql::TExprContext& ctx) = 0;
    virtual TStatus DoRollback() = 0;

    virtual bool OnExecuterResult(NKikimrKqp::TExecuterTxResult&& execResult,
        NLongTxService::TLockHandle&& lockHandle, NYql::TExprContext& ctx, bool commit) = 0;

protected:
    TKqpParamsMap PrepareParameters(const NKqpProto::TKqpPhyTx& tx);

    void ClearTx();

private:
    TStatus Execute(std::shared_ptr<const NKqpProto::TKqpPhyTx> tx, bool commit, NYql::TExprContext& ctx);
    TStatus Rollback();

    bool AddDeferredEffect(std::shared_ptr<const NKqpProto::TKqpPhyTx> tx);

    void PreserveParams(const NKqpProto::TKqpPhyTx& tx, TParamValueMap& paramsMap);

protected:
    struct TTransformState {
        TTransformState(TIntrusivePtr<TKqpTransactionState> txState)
            : TxState(txState) {}

        TIntrusivePtr<TKqpTransactionState> TxState;
        TVector<TVector<NKikimrMiniKQL::TResult>> TxResults;
    };

protected:
    TIntrusivePtr<IKqpGateway> Gateway;
    TString Cluster;

    TIntrusivePtr<TKqpTransactionState> TxState;
    TIntrusivePtr<TKqlTransformContext> TransformCtx;

    ui32 CurrentTxIndex;
    TKqpExecuteFlags ExecuteFlags;
    NThreading::TFuture<IKqpGateway::TExecPhysicalResult> ExecuteFuture;
    NThreading::TPromise<void> Promise;

    std::shared_ptr<TTransformState> TransformState;
};

} // namespace NKqp
} // namespace NKikimr
