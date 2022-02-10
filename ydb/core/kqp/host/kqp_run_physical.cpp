#include "kqp_run_physical.h"

namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NYql::NNodes;
using namespace NThreading;

IGraphTransformer::TStatus TKqpExecutePhysicalTransformerBase::DoTransform(TExprNode::TPtr input,
    TExprNode::TPtr& output, TExprContext& ctx)
{
    output = input;

    auto settings = TransformCtx->Settings;

    YQL_ENSURE(TMaybeNode<TCoWorld>(input));
    YQL_ENSURE(TransformCtx->PhysicalQuery);

    const auto& query = *TransformCtx->PhysicalQuery; 

    TStatus status = TStatus::Ok;

    auto& txState = TxState->Tx();
    const ui64 txsCount = query.TransactionsSize(); 

    if (settings.GetRollbackTx()) { 
        if (ExecuteFlags.HasFlags(TKqpExecuteFlag::Rollback)) { 
            ClearTx(); 
        } else { 
            status = Rollback(); 
        } 
    } else if (ExecuteFlags.HasFlags(TKqpExecuteFlag::Commit)) { 
        YQL_ENSURE(CurrentTxIndex >= txsCount); 
    } else {
        if (CurrentTxIndex >= txsCount) { 
            if (!txState.DeferredEffects.Empty() && TxState->Tx().Locks.Broken()) { 
                TxState->Tx().Locks.ReportIssues(ctx);
                return TStatus::Error;
            }
 
            if (settings.GetCommitTx()) { 
                status = Execute(nullptr, /* commit */ true, ctx); 
            } 
        } else { 
            const auto& tx = query.GetTransactions(CurrentTxIndex); 
 
            if (tx.GetHasEffects()) { 
                if (!AddDeferredEffect(tx)) { 
                    ctx.AddError(YqlIssue({}, TIssuesIds::KIKIMR_BAD_REQUEST, 
                        "Failed to mix queries with old- and new- engines")); 
                    return TStatus::Error; 
                } 
 
                ++CurrentTxIndex; 
                return TStatus::Repeat; 
            }

            // tx without effects 
 
            bool commit = false;
            if (CurrentTxIndex == txsCount - 1 && settings.GetCommitTx() && txState.DeferredEffects.Empty()) { 
                // last transaction and no effects (RO-query) 
                commit = true; 
            }
            status = Execute(&tx, commit, ctx); 
        }
    }

    if (status == TStatus::Ok) {
        for (const auto& resultBinding : query.GetResultBindings()) {
            YQL_ENSURE(resultBinding.GetTypeCase() == NKqpProto::TKqpPhyResultBinding::kTxResultBinding);
            auto& txResultBinding = resultBinding.GetTxResultBinding();
            auto txIndex = txResultBinding.GetTxIndex();
            auto resultIndex = txResultBinding.GetResultIndex();


            auto& txResults = TransformState->TxResults;
            YQL_ENSURE(txIndex < txResults.size());
            YQL_ENSURE(resultIndex < txResults[txIndex].size());
            TransformCtx->PhysicalQueryResults.emplace_back(std::move(txResults[txIndex][resultIndex]));
        }

        return status;
    }

    if (status == TStatus::Error) {
        return status;
    }

    YQL_ENSURE(status == TStatus::Async);

    Promise = NewPromise();
    auto promise = Promise;
    ExecuteFuture.Apply([promise](const TFuture<IKqpGateway::TExecPhysicalResult> future) mutable {
        YQL_ENSURE(future.HasValue());
        promise.SetValue();
    });

    return status;
}

TFuture<void> TKqpExecutePhysicalTransformerBase::DoGetAsyncFuture(const TExprNode& input) {
    Y_UNUSED(input);
    return Promise.GetFuture();
}

IGraphTransformer::TStatus TKqpExecutePhysicalTransformerBase::DoApplyAsyncChanges(TExprNode::TPtr input,
    TExprNode::TPtr& output, TExprContext& ctx)
{
    output = input;

    auto result = ExecuteFuture.ExtractValue();
    result.ReportIssues(ctx.IssueManager);
    if (!result.Success()) {
        TxState->Tx().Invalidate();
        return TStatus::Error;
    }

    if (ExecuteFlags.HasFlags(TKqpExecuteFlag::Rollback)) { 
        return TStatus::Repeat; 
    } 
 
    auto& execResult = result.ExecuterResult;
    if (ExecuteFlags.HasFlags(TKqpExecuteFlag::Results)) {
        TVector<NKikimrMiniKQL::TResult> txResults;
        txResults.resize(execResult.ResultsSize());
        for (ui32 i = 0; i < execResult.ResultsSize(); ++i) {
            txResults[i].Swap(execResult.MutableResults(i));
        }

        TransformState->TxResults.emplace_back(std::move(txResults));
    }

    if (!OnExecuterResult(std::move(execResult), ctx, ExecuteFlags.HasFlags(TKqpExecuteFlag::Commit))) {
        return TStatus::Error;
    }

    ++CurrentTxIndex;
    return TStatus::Repeat;
}

void TKqpExecutePhysicalTransformerBase::Rewind() {
    ExecuteFlags = TKqpExecuteFlags();
    CurrentTxIndex = 0;
    TransformState->TxResults.clear();
}

IGraphTransformer::TStatus TKqpExecutePhysicalTransformerBase::Execute(const NKqpProto::TKqpPhyTx* tx, bool commit, 
    NYql::TExprContext& ctx) 
{ 
    ExecuteFlags = TKqpExecuteFlags();

    if (tx) {
        ExecuteFlags |= TKqpExecuteFlag::Results;
    } else {
        YQL_ENSURE(commit);
    }

    if (commit) {
        ExecuteFlags |= TKqpExecuteFlag::Commit;
    }

    return DoExecute(tx, commit, ctx); 
}

IGraphTransformer::TStatus TKqpExecutePhysicalTransformerBase::Rollback() { 
    ExecuteFlags = TKqpExecuteFlags(); 
    ExecuteFlags |= TKqpExecuteFlag::Rollback; 
 
    return DoRollback(); 
} 
 
bool TKqpExecutePhysicalTransformerBase::AddDeferredEffect(const NKqpProto::TKqpPhyTx& tx) { 
    TParamValueMap params; 
    PreserveParams(tx, params); 

    return TxState->Tx().AddDeferredEffect(tx, std::move(params)); 
}

NDq::TMkqlValueRef TKqpExecutePhysicalTransformerBase::GetParamValue(
    const NKqpProto::TKqpPhyParamBinding& paramBinding)
{
    switch (paramBinding.GetTypeCase()) {
        case NKqpProto::TKqpPhyParamBinding::kExternalBinding: {
            auto clientParam = TransformCtx->QueryCtx->Parameters.FindPtr(paramBinding.GetName());
            YQL_ENSURE(clientParam, "Parameter not found: " << paramBinding.GetName());
            return NDq::TMkqlValueRef(*clientParam);
        }
        case NKqpProto::TKqpPhyParamBinding::kTxResultBinding: {
            auto& txResultBinding = paramBinding.GetTxResultBinding();
            auto txIndex = txResultBinding.GetTxIndex();
            auto resultIndex = txResultBinding.GetResultIndex();

            auto& txResults = TransformState->TxResults;
            YQL_ENSURE(txIndex < txResults.size());
            YQL_ENSURE(resultIndex < txResults[txIndex].size());
            return NDq::TMkqlValueRef(txResults[txIndex][resultIndex]);
        }
        case NKqpProto::TKqpPhyParamBinding::kInternalBinding: {
            auto& internalBinding = paramBinding.GetInternalBinding();
            auto& param = TransformCtx->QueryCtx->Parameters[paramBinding.GetName()];

            switch (internalBinding.GetType()) {
                case NKqpProto::TKqpPhyInternalBinding::PARAM_NOW:
                    param.MutableType()->SetKind(NKikimrMiniKQL::ETypeKind::Data);
                    param.MutableType()->MutableData()->SetScheme(NKikimr::NUdf::TDataType<ui64>::Id);
                    param.MutableValue()->SetUint64(TransformCtx->QueryCtx->GetCachedNow());
                    break;
                case NKqpProto::TKqpPhyInternalBinding::PARAM_CURRENT_DATE: {
                    param.MutableType()->SetKind(NKikimrMiniKQL::ETypeKind::Data);
                    param.MutableType()->MutableData()->SetScheme(NKikimr::NUdf::TDataType<NUdf::TDate>::Id);
                    ui64 date = TransformCtx->QueryCtx->GetCachedDate();
                    YQL_ENSURE(date <= Max<ui32>());
                    param.MutableValue()->SetUint32(static_cast<ui32>(date));
                    break;
                }
                case NKqpProto::TKqpPhyInternalBinding::PARAM_CURRENT_DATETIME: {
                    param.MutableType()->SetKind(NKikimrMiniKQL::ETypeKind::Data);
                    param.MutableType()->MutableData()->SetScheme(NKikimr::NUdf::TDataType<NUdf::TDatetime>::Id);
                    ui64 datetime = TransformCtx->QueryCtx->GetCachedDatetime();
                    YQL_ENSURE(datetime <= Max<ui32>());
                    param.MutableValue()->SetUint32(static_cast<ui32>(datetime));
                    break;
                }
                case NKqpProto::TKqpPhyInternalBinding::PARAM_CURRENT_TIMESTAMP:
                    param.MutableType()->SetKind(NKikimrMiniKQL::ETypeKind::Data);
                    param.MutableType()->MutableData()->SetScheme(NKikimr::NUdf::TDataType<NUdf::TTimestamp>::Id);
                    param.MutableValue()->SetUint64(TransformCtx->QueryCtx->GetCachedTimestamp());
                    break;
                case NKqpProto::TKqpPhyInternalBinding::PARAM_RANDOM_NUMBER:
                    param.MutableType()->SetKind(NKikimrMiniKQL::ETypeKind::Data);
                    param.MutableType()->MutableData()->SetScheme(NKikimr::NUdf::TDataType<ui64>::Id);
                    param.MutableValue()->SetUint64(TransformCtx->QueryCtx->GetCachedRandom<ui64>());
                    break;
                case NKqpProto::TKqpPhyInternalBinding::PARAM_RANDOM:
                    param.MutableType()->SetKind(NKikimrMiniKQL::ETypeKind::Data);
                    param.MutableType()->MutableData()->SetScheme(NKikimr::NUdf::TDataType<double>::Id);
                    param.MutableValue()->SetDouble(TransformCtx->QueryCtx->GetCachedRandom<double>());
                    break;
                case NKqpProto::TKqpPhyInternalBinding::PARAM_RANDOM_UUID: {
                    param.MutableType()->SetKind(NKikimrMiniKQL::ETypeKind::Data);
                    param.MutableType()->MutableData()->SetScheme(NKikimr::NUdf::TDataType<NUdf::TUuid>::Id);
                    auto uuid = TransformCtx->QueryCtx->GetCachedRandom<TGUID>();
                    param.MutableValue()->SetBytes(uuid.dw, sizeof(TGUID));
                    break;
                }
                default:
                    YQL_ENSURE(false, "Unexpected internal parameter type: " << (ui32)internalBinding.GetType());
            }

            return NDq::TMkqlValueRef(param);
        }
        default:
            YQL_ENSURE(false, "Unexpected parameter binding type: " << (ui32)paramBinding.GetTypeCase());
    }
}

TKqpParamsMap TKqpExecutePhysicalTransformerBase::PrepareParameters(const NKqpProto::TKqpPhyTx& tx) {
    TKqpParamsMap paramsMap(TransformState);
    for (const auto& paramBinding : tx.GetParamBindings()) {
        auto it = paramsMap.Values.emplace(paramBinding.GetName(), GetParamValue(paramBinding));
        YQL_ENSURE(it.second); 
    }

    return paramsMap;
}

void TKqpExecutePhysicalTransformerBase::PreserveParams(const NKqpProto::TKqpPhyTx& tx, TParamValueMap& paramsMap) {
    for (const auto& paramBinding : tx.GetParamBindings()) {
        auto paramValueRef = GetParamValue(paramBinding);

        NKikimrMiniKQL::TParams param;
        param.MutableType()->CopyFrom(paramValueRef.GetType());
        param.MutableValue()->CopyFrom(paramValueRef.GetValue());

        YQL_ENSURE(paramsMap.emplace(paramBinding.GetName(), std::move(param)).second); 
    }
}

void TKqpExecutePhysicalTransformerBase::ClearTx() { 
    TxState->Tx().ClearDeferredEffects(); 
    TxState->Tx().Locks.Clear(); 
    TxState->Tx().Finish(); 
} 
 
} // namespace NKqp 
} // namespace NKikimr
