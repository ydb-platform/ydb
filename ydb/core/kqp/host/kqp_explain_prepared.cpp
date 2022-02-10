#include "kqp_host_impl.h" 
 
#include <ydb/core/kqp/common/kqp_gateway.h> 
#include <ydb/core/kqp/common/kqp_yql.h> 
#include <ydb/core/kqp/prepare/kqp_query_plan.h> 
 
namespace NKikimr { 
namespace NKqp { 
 
using namespace NYql; 
using namespace NYql::NNodes; 
using namespace NThreading; 
 
class TKqpExplainPreparedTransformer : public NYql::TGraphTransformerBase { 
public: 
    TKqpExplainPreparedTransformer(TIntrusivePtr<IKqpGateway> gateway, const TString& cluster, 
        TIntrusivePtr<TKqlTransformContext> transformCtx) 
        : Gateway(gateway) 
        , Cluster(cluster) 
        , TransformCtx(transformCtx) 
        , CurrentTxIndex(0) {} 
 
    TStatus DoTransform(NYql::TExprNode::TPtr input, NYql::TExprNode::TPtr& output, NYql::TExprContext& ctx) override { 
        output = input; 
 
        auto pure = [](const auto& tx) { 
            if (tx.GetType() != NKqpProto::TKqpPhyTx::TYPE_COMPUTE) { 
                return false; 
            } 
 
            for (const auto& stage : tx.GetStages()) { 
                if (stage.InputsSize() != 0) { 
                    return false; 
                } 
            } 
 
            return true; 
        }; 
 
        auto& query = *TransformCtx->QueryCtx->PreparingQuery->MutablePhysicalQuery(); 
        while (CurrentTxIndex < query.TransactionsSize()) { 
            const auto& tx = query.GetTransactions(CurrentTxIndex); 
            auto params = PrepareParameters(tx); 
 
            if (pure(tx) && params) { 
                IKqpGateway::TExecPhysicalRequest request; 
                request.Transactions.emplace_back(tx, std::move(*params)); 
 
                ExecuteFuture = Gateway->ExecutePure(std::move(request), {}); 
 
                Promise = NewPromise(); 
                ExecuteFuture.Apply([promise = Promise](const TFuture<IKqpGateway::TExecPhysicalResult> future) mutable { 
                    YQL_ENSURE(future.HasValue()); 
                    promise.SetValue(); 
                }); 
 
                return TStatus::Async; 
            } 
 
            ++CurrentTxIndex; 
        } 
 
        PhyQuerySetTxPlans(query, TKqpPhysicalQuery(input), std::move(TxResults), 
            ctx, Cluster, TransformCtx->Tables, TransformCtx->Config); 
        query.SetQueryAst(KqpExprToPrettyString(*input, ctx)); 
 
        return TStatus::Ok; 
    } 
 
    NThreading::TFuture<void> DoGetAsyncFuture(const NYql::TExprNode& /*input*/) override { 
        return Promise.GetFuture(); 
    } 
 
    TStatus DoApplyAsyncChanges(NYql::TExprNode::TPtr input, NYql::TExprNode::TPtr& output, 
        NYql::TExprContext& ctx) override 
    { 
        output = input; 
 
        auto result = ExecuteFuture.ExtractValue(); 
        result.ReportIssues(ctx.IssueManager); 
        if (!result.Success()) { 
            return TStatus::Error; 
        } 
 
        auto& txResults = result.ExecuterResult.GetResults(); 
        TxResults[CurrentTxIndex] = {txResults.begin(), txResults.end()}; 
 
        ++CurrentTxIndex; 
        return TStatus::Repeat; 
    } 
 
    void Rewind() override { 
        CurrentTxIndex = 0; 
        TxResults.clear(); 
    } 
 
private: 
 
    TMaybe<NDq::TMkqlValueRef> GetParamValue(const NKqpProto::TKqpPhyParamBinding& paramBinding) 
    { 
        switch (paramBinding.GetTypeCase()) { 
            case NKqpProto::TKqpPhyParamBinding::kExternalBinding: { 
                auto clientParam = TransformCtx->QueryCtx->Parameters.FindPtr(paramBinding.GetName()); 
                if (clientParam) { 
                    return TMaybe<NDq::TMkqlValueRef>(NDq::TMkqlValueRef(*clientParam)); 
                } 
                return {}; 
            } 
            case NKqpProto::TKqpPhyParamBinding::kTxResultBinding: { 
                auto& txResultBinding = paramBinding.GetTxResultBinding(); 
                auto txIndex = txResultBinding.GetTxIndex(); 
                auto resultIndex = txResultBinding.GetResultIndex(); 
 
                if (TxResults.contains(txIndex) && resultIndex < TxResults[txIndex].size()) { 
                    return TMaybe<NDq::TMkqlValueRef>(NDq::TMkqlValueRef(TxResults[txIndex][resultIndex])); 
                } 
                return {}; 
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
 
    TMaybe<TKqpParamsMap> PrepareParameters(const NKqpProto::TKqpPhyTx& tx) { 
        TKqpParamsMap params; 
        for (const auto& paramBinding : tx.GetParamBindings()) { 
            if (auto paramValue = GetParamValue(paramBinding)) { 
                params.Values.emplace(std::make_pair(paramBinding.GetName(), *paramValue)); 
            } else { 
                return {}; 
            } 
        } 
 
        return TMaybe<TKqpParamsMap>(params); 
    } 
 
    TIntrusivePtr<IKqpGateway> Gateway; 
    TString Cluster; 
    THashMap<ui32, TVector<NKikimrMiniKQL::TResult>> TxResults; 
    TIntrusivePtr<TKqlTransformContext> TransformCtx; 
    ui32 CurrentTxIndex; 
    NThreading::TFuture<IKqpGateway::TExecPhysicalResult> ExecuteFuture; 
    NThreading::TPromise<void> Promise; 
}; 
 
 
TAutoPtr<IGraphTransformer> CreateKqpExplainPreparedTransformer(TIntrusivePtr<IKqpGateway> gateway, 
    const TString& cluster, TIntrusivePtr<TKqlTransformContext> transformCtx) 
{ 
    return new TKqpExplainPreparedTransformer(gateway, cluster, transformCtx); 
} 
 
} // namespace NKqp 
} // namespace NKikimr 
