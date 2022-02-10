#include "kqp_prepare_impl.h"

#include <ydb/core/engine/mkql_engine_flat.h> 
#include <ydb/core/kqp/common/kqp_yql.h> 
#include <ydb/core/kqp/provider/yql_kikimr_provider_impl.h> 
#include <ydb/core/tx/datashard/sys_tables.h> 

#include <ydb/library/yql/utils/log/log.h> 

#include <google/protobuf/text_format.h>

namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NYql::NNodes;
using namespace NYql::NCommon;
using namespace NThreading;

namespace {

struct TTransformState {
    TTransformState(TIntrusivePtr<TKqpTransactionState> txState, TIntrusivePtr<TKqlTransformContext> transformCtx)
        : ParamsState(txState->Tx().ParamsState)
        , TransformCtx(transformCtx) {}

    TIntrusivePtr<TKqpTransactionContext::TParamsState> ParamsState;
    TIntrusivePtr<TKqlTransformContext> TransformCtx;
};

struct TParamBinding {
    TString Name;
    TMaybe<ui32> ResultIndex;

    TParamBinding(const TString& name, TMaybe<ui32> resultIndex = {})
        : Name(name)
        , ResultIndex(resultIndex) {}
};

NKikimrKqp::TParameterBinding GetParameterBinding(TCoParameter param) {
    auto name = TString(param.Name().Value());

    NKikimrKqp::TParameterBinding binding;
    binding.SetName(name);
    if (param.Ref().HasResult()) {
        auto indexTuple = TCoAtomList(TExprNode::GetResult(param.Ptr()));
        ui32 mkqlIndex = FromString<ui32>(indexTuple.Item(0).Value());
        ui32 resultIndex = FromString<ui32>(indexTuple.Item(1).Value());
        binding.SetMkqlIndex(mkqlIndex);
        binding.SetResultIndex(resultIndex);
    }

    return binding;
}

NDq::TMkqlValueRef GetParamFromResult(const NKikimrKqp::TParameterBinding& binding,
    const TKqlTransformContext& transformCtx)
{
    YQL_ENSURE(binding.HasMkqlIndex());
    YQL_ENSURE(binding.HasResultIndex());
    YQL_ENSURE(binding.GetMkqlIndex() < transformCtx.MkqlResults.size());

    const NKikimrMiniKQL::TType* type;
    const NKikimrMiniKQL::TValue* value;
    GetKikimrUnpackedRunResult(*transformCtx.MkqlResults[binding.GetMkqlIndex()], binding.GetResultIndex(),
        type, value);
    YQL_ENSURE(type);
    YQL_ENSURE(value);

    return NDq::TMkqlValueRef(*type, *value);
}

NKikimrMiniKQL::TParams BuildParamFromResult(const NKikimrKqp::TParameterBinding& binding,
    const TKqlTransformContext& transformCtx)
{
    auto valueRef = GetParamFromResult(binding, transformCtx);

    NKikimrMiniKQL::TParams param;
    param.MutableType()->CopyFrom(valueRef.GetType());
    param.MutableValue()->CopyFrom(valueRef.GetValue());
    return param;
}

bool GetPredicateValue(const TKiConditionalEffect& effect, const TKqlTransformContext& transformCtx,
    const THashMap<TString, NKikimrKqp::TParameterBinding>& bindingsMap)
{
    YQL_ENSURE(effect.Predicate().Maybe<TCoParameter>());
    auto paramName = effect.Predicate().Cast<TCoParameter>().Name().Value();

    auto binding = bindingsMap.FindPtr(paramName);
    YQL_ENSURE(binding);
    YQL_ENSURE(binding->HasMkqlIndex());

    auto paramValue = GetParamFromResult(*binding, transformCtx);
    YQL_ENSURE(paramValue.GetType().GetKind() == NKikimrMiniKQL::ETypeKind::Data);
    YQL_ENSURE(paramValue.GetType().GetData().GetScheme() == NKikimr::NScheme::NTypeIds::Bool);
    return paramValue.GetValue().GetBool();
}

bool ProcessEffect(TExprBase& effect, const THashMap<TString, NKikimrKqp::TParameterBinding>& bindingsMap,
    const TKqlTransformContext& transformCtx, TExprContext& ctx)
{
    TOptimizeExprSettings optSettings(nullptr);
    optSettings.VisitChanges = true;
    TExprNode::TPtr output;
    auto status = OptimizeExpr(effect.Ptr(), output,
        [&bindingsMap, &transformCtx](const TExprNode::TPtr& input, TExprContext& ctx) {
            TExprBase node(input);

            if (auto maybeRevert = node.Maybe<TKiRevertIf>()) {
                auto revert = maybeRevert.Cast();
                auto predicateValue = GetPredicateValue(revert, transformCtx, bindingsMap);

                if (predicateValue) {
                    ctx.AddWarning(YqlIssue(ctx.GetPosition(revert.Pos()), TIssuesIds::KIKIMR_OPERATION_REVERTED, TStringBuilder()
                        << "Operation reverted due to constraint violation: " << revert.Constraint().Value()));

                    return GetEmptyEffectsList(revert.Pos(), ctx).Ptr();
                }

                return revert.Effect().Ptr();
            }

            if (auto maybeAbort = node.Maybe<TKiAbortIf>()) {
                auto abort = maybeAbort.Cast();
                auto predicateValue = GetPredicateValue(abort, transformCtx, bindingsMap);

                if (predicateValue) {
                    ctx.AddError(YqlIssue(ctx.GetPosition(abort.Pos()), TIssuesIds::KIKIMR_CONSTRAINT_VIOLATION, TStringBuilder()
                        << "Operation aborted due to constraint violation: " << abort.Constraint().Value()));

                    return TExprNode::TPtr();
                }

                return abort.Effect().Ptr();
            }

            return input;
        }, ctx, optSettings);

    if (status == IGraphTransformer::TStatus::Ok) {
        effect = TExprBase(output);
        return true;
    }

    YQL_ENSURE(status == IGraphTransformer::TStatus::Error);
    return false;
}

NNodes::TExprBase PreserveParams(NNodes::TExprBase node,
    const THashMap<TString, NKikimrKqp::TParameterBinding>& bindingsMap, TExprContext& ctx,
    TKqpTransactionState& txState, const TKqlTransformContext& transformCtx, bool preserveValues)
{
    TNodeOnNodeOwnedMap replaceMap;
    THashMap<TString, TExprBase> resultsMap;
    VisitExpr(node.Ptr(), [&replaceMap, &resultsMap, &ctx, &bindingsMap, &txState, &transformCtx, preserveValues] (const TExprNode::TPtr& node) {
        if (auto maybeParam = TMaybeNode<TCoParameter>(node)) {
            auto param = maybeParam.Cast();
            auto name = TString(param.Name().Value());

            auto bindingPtr = bindingsMap.FindPtr(name);
            YQL_ENSURE(bindingPtr);

            auto& binding = *bindingPtr;

            TMaybe<NKikimrMiniKQL::TParams> paramValue;
            TMaybeNode<TExprBase> resultNode;
            if (preserveValues) {
                if (binding.HasMkqlIndex()) {
                    paramValue = BuildParamFromResult(binding, transformCtx);
                } else {
                    if (!txState.Tx().ParamsState->Values.contains(binding.GetName())) {
                        auto clientParam = transformCtx.QueryCtx->Parameters.FindPtr(binding.GetName());
                        YQL_ENSURE(clientParam);
                        paramValue = *clientParam;
                    }
                }
            } else {
                if (!param.Ref().HasResult() && binding.HasMkqlIndex()) {
                    resultNode = Build<TCoAtomList>(ctx, param.Pos())
                        .Add().Build(ToString(binding.GetMkqlIndex()))
                        .Add().Build(ToString(binding.GetResultIndex()))
                        .Done();
                }
            }

            if (!paramValue && !resultNode) {
                return true;
            }

            auto newParamName = txState.Tx().NewParamName();

            auto newParamNode = Build<TCoParameter>(ctx, node->Pos())
                .Name().Build(newParamName)
                .Type(param.Type())
                .Done();

            replaceMap.emplace(node.Get(), newParamNode.Ptr());

            if (paramValue) {
                YQL_ENSURE(txState.Tx().ParamsState->Values.emplace(std::make_pair(newParamName, *paramValue)).second);
            }

            if (resultNode) {
                YQL_ENSURE(resultsMap.emplace(std::make_pair(newParamName, resultNode.Cast())).second);
            }
        }

        return true;
    });

    auto newNode = TExprBase(ctx.ReplaceNodes(node.Ptr(), replaceMap));

    if (!resultsMap.empty()) {
        VisitExpr(newNode.Ptr(), [&resultsMap] (const TExprNode::TPtr& node) {
            if (auto maybeParam = TMaybeNode<TCoParameter>(node)) {
                auto param = maybeParam.Cast();
                auto name = TString(param.Name().Value());

                auto result = resultsMap.FindPtr(name);
                if (result) {
                    param.Ptr()->SetResult(result->Ptr());
                }
            }

            return true;
        });
    }

    return newNode;
}

class TKqpExecTransformer : public TGraphTransformerBase {
public:
    TKqpExecTransformer(TIntrusivePtr<IKqpGateway> gateway, const TString& cluster,
        TIntrusivePtr<TKqpTransactionState> txState, TIntrusivePtr<TKqlTransformContext> transformCtx)
        : Gateway(gateway)
        , Cluster(cluster)
        , TxState(txState)
        , TransformCtx(transformCtx) {}

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        output = input;

        const auto& analyzeResults = TransformCtx->AnalyzeResults;

        TVector<TExprBase> results;
        for (auto& root : analyzeResults.ExecutionRoots) {
            if (root.Scope) {
                ctx.AddError(TIssue(ctx.GetPosition(root.Node.Pos()), TStringBuilder()
                    << "Unexpected nested execution roots in rewritten program."));
                return TStatus::Error;
            }

            results.push_back(TExprBase(root.Node));
        }

        TMaybeNode<TExprList> resultsNode;
        if (analyzeResults.CanExecute) {
            auto kqpProgram = TKiProgram(input);

            if (HasEffects(kqpProgram)) {
                bool preserveParams = !TransformCtx->Settings.GetCommitTx();
                if (!AddDeferredEffect(kqpProgram.Effects(), ctx, *TxState, *TransformCtx, preserveParams)) {
                    return TStatus::Error;
                }
            }

            if (!HasResults(kqpProgram)) {
                TransformCtx->MkqlResults.push_back(nullptr);
                return TStatus::Ok;
            }

            if (TransformCtx->Settings.GetCommitTx() && !TransformCtx->QueryCtx->PrepareOnly) {
                if (TxState->Tx().DeferredEffects.Empty()) {
                    // Merge read-only query with commit tx
                    return TStatus::Ok;
                }
            }

            resultsNode = kqpProgram.Results();
        } else {
            YQL_ENSURE(!results.empty());
            resultsNode = Build<TExprList>(ctx, input->Pos())
                .Add(results)
                .Done();
        }

        auto effectsNode = Build<TCoAsList>(ctx, input->Pos())
            .Add<TCoIf>()
                .Predicate<TCoParameter>()
                    .Name().Build(LocksAcquireParamName)
                    .Type<TCoDataType>()
                        .Type().Build("Bool")
                        .Build()
                    .Build()
                .ThenValue<TKiAcquireLocks>()
                    .LockTxId<TCoParameter>()
                        .Name().Build(LocksTxIdParamName)
                        .Type<TCoDataType>()
                            .Type().Build("Uint64")
                            .Build()
                        .Build()
                    .Build()
                .ElseValue<TCoVoid>()
                    .Build()
                .Build()
            .Done();

        YQL_ENSURE(resultsNode);
        auto program = Build<TKiProgram>(ctx, input->Pos())
            .Results(resultsNode.Cast())
            .Effects(effectsNode)
            .Done();

        Promise = NewPromise();
        MkqlExecuteResult = ExecuteMkql(program, Gateway, Cluster, ctx, TxState, TransformCtx, false);

        auto promise = Promise;
        MkqlExecuteResult.Future.Apply([promise](const TFuture<IKqpGateway::TMkqlResult> future) mutable {
            YQL_ENSURE(future.HasValue());
            promise.SetValue();
        });

        return TStatus::Async;
    }

    TFuture<void> DoGetAsyncFuture(const TExprNode& input) final {
        Y_UNUSED(input);
        return Promise.GetFuture();
    }

    TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        output = input;

        NKikimr::NKqp::IKqpGateway::TMkqlResult result(MkqlExecuteResult.Future.ExtractValue());

        result.ReportIssues(ctx.IssueManager);
        if (!result.Success()) {
            return TStatus::Error;
        }

        auto mkqlResult = MakeSimpleShared<NKikimrMiniKQL::TResult>();
        mkqlResult->Swap(&result.Result);

        TransformCtx->MkqlResults.push_back(mkqlResult);

        if (TransformCtx->QueryCtx->PrepareOnly) {
            YQL_ENSURE(!TransformCtx->GetPreparingKql().GetMkqls().empty());
            auto& mkql = *TransformCtx->GetPreparingKql().MutableMkqls()->rbegin();
            mkql.SetProgram(result.CompiledProgram);
            mkql.SetProgramText(MkqlExecuteResult.Program);
        } else {
            LogMkqlResult(*mkqlResult, ctx);
            TransformCtx->AddMkqlStats(MkqlExecuteResult.Program, std::move(result.TxStats));

            if (TxState->Tx().EffectiveIsolationLevel == NKikimrKqp::ISOLATION_LEVEL_SERIALIZABLE) {
                if (!UnpackMergeLocks(*mkqlResult, TxState->Tx(), ctx)) {
                    return TStatus::Error;
                }
            }
        }

        if (TransformCtx->AnalyzeResults.CanExecute) {
            output = Build<TKiProgram>(ctx, input->Pos())
                .Results()
                    .Build()
                .Effects(TKiProgram(input).Effects())
                .Done()
                .Ptr();
        }

        return TStatus::Ok;
    }

    void Rewind() override {}

private:
    TIntrusivePtr<IKqpGateway> Gateway;
    TString Cluster;
    TIntrusivePtr<TKqpTransactionState> TxState;
    TIntrusivePtr<TKqlTransformContext> TransformCtx;
    TMkqlExecuteResult MkqlExecuteResult;
    TPromise<void> Promise;
};

void ExtractQueryStats(NKqpProto::TKqpStatsQuery& dst, const NKikimrQueryStats::TTxStats& txStats) {
    auto& dstExec = *dst.AddExecutions();
    NKqpProto::TKqpExecutionExtraStats executionExtraStats;

    dstExec.SetDurationUs(txStats.GetDurationUs());
    dstExec.SetCpuTimeUs(txStats.GetComputeCpuTimeUsec());

    auto& dstComputeTime = *executionExtraStats.MutableComputeCpuTimeUs();
    dstComputeTime.SetMin(txStats.GetComputeCpuTimeUsec());
    dstComputeTime.SetMax(txStats.GetComputeCpuTimeUsec());
    dstComputeTime.SetSum(txStats.GetComputeCpuTimeUsec());
    dstComputeTime.SetCnt(1);

    {
        i64 cnt = 0;
        ui64 minCpu = Max<ui64>();
        ui64 maxCpu = 0;
        ui64 sumCpu = 0;
        ui64 sumReadSets = 0;
        ui64 maxProgramSize = 0;
        ui64 maxReplySize = 0;
        for (const auto& perShard : txStats.GetPerShardStats()) {
            ui64 cpu = perShard.GetCpuTimeUsec();
            minCpu = Min(minCpu, cpu);
            maxCpu = Max(maxCpu, cpu);
            sumCpu += cpu;
            sumReadSets += perShard.GetOutgoingReadSetsCount();
            maxProgramSize = Max(maxProgramSize, perShard.GetProgramSize());
            maxReplySize = Max(maxReplySize, perShard.GetReplySize());
            ++cnt;
        }
        if (cnt) {
            auto& dstShardTime = *executionExtraStats.MutableShardsCpuTimeUs();
            dstShardTime.SetMin(minCpu);
            dstShardTime.SetMax(maxCpu);
            dstShardTime.SetSum(sumCpu);
            dstShardTime.SetCnt(cnt);

            dst.SetReadSetsCount(dst.GetReadSetsCount() + sumReadSets);
            dst.SetMaxShardProgramSize(Max(dst.GetMaxShardProgramSize(), maxProgramSize));
            dst.SetMaxShardReplySize(Max(dst.GetMaxShardReplySize(), maxReplySize));

            dstExec.SetCpuTimeUs(dstExec.GetCpuTimeUs() + sumCpu);
        }
    }

    ui32 affectedShards = 0;
    for (auto& table : txStats.GetTableAccessStats()) {
        auto& dstTable = *dstExec.AddTables();
        dstTable.SetTablePath(table.GetTableInfo().GetName());
        dstTable.SetReadRows(table.GetSelectRow().GetRows() + table.GetSelectRange().GetRows());
        dstTable.SetReadBytes(table.GetSelectRow().GetBytes() + table.GetSelectRange().GetBytes());
        dstTable.SetWriteRows(table.GetUpdateRow().GetRows());
        dstTable.SetWriteBytes(table.GetUpdateRow().GetBytes());
        dstTable.SetEraseRows(table.GetEraseRow().GetRows());
        dstTable.SetAffectedPartitions(table.GetShardCount());

        // NOTE: This might be incorrect in case when single shard has several
        // tables, i.e. collocated tables.
        affectedShards += table.GetShardCount();
    }

    executionExtraStats.SetAffectedShards(affectedShards);

    dstExec.MutableExtra()->PackFrom(executionExtraStats);
}

} // namespace

void TKqlTransformContext::AddMkqlStats(const TString& program, NKikimrQueryStats::TTxStats&& txStats) {
    Y_UNUSED(program);

    ExtractQueryStats(QueryStats, txStats);
}

IKqpGateway::TMkqlSettings TKqlTransformContext::GetMkqlSettings(bool hasDataEffects, TInstant now) const {
    IKqpGateway::TMkqlSettings mkqlSettings;
    mkqlSettings.LlvmRuntime = false;
    mkqlSettings.CollectStats = QueryCtx->StatsMode >= EKikimrStatsMode::Basic;

    if (hasDataEffects) {
        mkqlSettings.PerShardKeysSizeLimitBytes = Config->_CommitPerShardKeysSizeLimitBytes.Get().GetRef();
    }

    auto& deadlines = QueryCtx->Deadlines;
    if (deadlines.CancelAt) {
        mkqlSettings.CancelAfterMs = now < deadlines.CancelAt ? (deadlines.CancelAt - now).MilliSeconds() : 1;
    }
    if (deadlines.TimeoutAt) {
        mkqlSettings.TimeoutMs = now < deadlines.TimeoutAt ? (deadlines.TimeoutAt - now).MilliSeconds() : 1;
    }

    mkqlSettings.Limits = QueryCtx->Limits.PhaseLimits;

    return mkqlSettings;
}

TMkqlExecuteResult ExecuteMkql(TKiProgram program, TIntrusivePtr<IKqpGateway> gateway, const TString& cluster,
    TExprContext& ctx, TIntrusivePtr<TKqpTransactionState> txState, TIntrusivePtr<TKqlTransformContext> transformCtx,
    bool hasDataEffects)
{
    auto mkqlProgram = TranslateToMkql(program, ctx, TString(ReadTargetParamName));
    if (!mkqlProgram) {
        return TMkqlExecuteResult(MakeFuture(ResultFromError<IKqpGateway::TMkqlResult>(
            "Mkql translation failed.", ctx.GetPosition(program.Pos()))));
    }

    auto mkqlProgramText = NCommon::SerializeExpr(ctx, mkqlProgram.Cast().Ref());
    TFuture<IKqpGateway::TMkqlResult> future;

    auto paramBindings = CollectParams(mkqlProgram.Cast());

    if (transformCtx->QueryCtx->PrepareOnly) {
        YQL_CLOG(INFO, ProviderKqp) << "Preparing MiniKQL program:" << Endl << mkqlProgramText;

        auto& mkql = *transformCtx->GetPreparingKql().AddMkqls();
        for (auto& binding : paramBindings) {
            mkql.AddBindings()->CopyFrom(binding);
        }

        mkql.SetIsPure(!hasDataEffects && IsKqlPureExpr(program));

        future = gateway->PrepareMkql(cluster, mkqlProgramText);
    } else {
        YQL_CLOG(INFO, ProviderKqp) << "Executing MiniKQL program:" << Endl << mkqlProgramText;

        bool acquireLocks = *txState->Tx().EffectiveIsolationLevel == NKikimrKqp::ISOLATION_LEVEL_SERIALIZABLE
            && !txState->Tx().Locks.Broken();
        auto execParams = BuildParamsMap(paramBindings, txState, transformCtx, acquireLocks);

        if (YQL_CLOG_ACTIVE(TRACE, ProviderKqp)) {
            TStringBuilder paramsTextBuilder;
            for (auto& pair : execParams.Values) {
                TString paramText;
                NProtoBuf::TextFormat::PrintToString(pair.second.GetValue(), &paramText);
                paramsTextBuilder << pair.first << ": " << paramText << Endl;
            }

            YQL_CLOG(TRACE, ProviderKqp) << "MiniKQL parameters:" << Endl << paramsTextBuilder;
        }

        future = gateway->ExecuteMkql(cluster, mkqlProgramText, std::move(execParams),
            transformCtx->GetMkqlSettings(hasDataEffects, gateway->GetCurrentTime()), txState->Tx().GetSnapshot());
    }

    return TMkqlExecuteResult(mkqlProgramText, future);
}

bool AddDeferredEffect(NNodes::TExprBase effect, const TVector<NKikimrKqp::TParameterBinding>& bindings,
    TExprContext& ctx, TKqpTransactionState& txState, TKqlTransformContext& transformCtx, bool preserveParamValues)
{
    if (transformCtx.QueryCtx->PrepareOnly) {
        auto& newEffect = *transformCtx.GetPreparingKql().AddEffects();
        newEffect.SetNodeAst(NCommon::SerializeExpr(ctx, effect.Ref()));
        for (auto& binding : bindings) {
            newEffect.AddBindings()->CopyFrom(binding);
        }
    } else {
        if (txState.Tx().Locks.Broken()) {
            txState.Tx().Locks.ReportIssues(ctx);
            return false;
        }
        THashMap<TString, NKikimrKqp::TParameterBinding> bindingsMap;
        for (auto& binding : bindings) {
            bindingsMap.emplace(binding.GetName(), binding);
        }

        if (!ProcessEffect(effect, bindingsMap, transformCtx, ctx)) {
            return false;
        }

        effect = PreserveParams(effect, bindingsMap, ctx, txState, transformCtx, preserveParamValues);
    }

    bool added = txState.Tx().AddDeferredEffect(effect);
    YQL_ENSURE(added, "Cannot execute new- and old- execution engine queries in the same transaction");

    YQL_CLOG(INFO, ProviderKqp) << "Adding deferred effect, total " << txState.Tx().DeferredEffects.Size() << ": "
        << Endl << KqpExprToPrettyString(effect, ctx);
    return true;
}

bool AddDeferredEffect(NNodes::TExprBase effect, TExprContext& ctx, TKqpTransactionState& txState,
    TKqlTransformContext& transformCtx, bool preserveParamValues)
{
    return AddDeferredEffect(effect, CollectParams(effect), ctx, txState, transformCtx, preserveParamValues);
}

TKqpParamsMap BuildParamsMap(const TVector<NKikimrKqp::TParameterBinding>& bindings,
    TIntrusivePtr<TKqpTransactionState> txState, TIntrusivePtr<TKqlTransformContext> transformCtx, bool acquireLocks)
{
    TKqpParamsMap paramsMap(std::make_shared<TTransformState>(txState, transformCtx));

    for (auto& binding : bindings) {
        auto name = binding.GetName();

        TMaybe<NDq::TMkqlValueRef> paramRef;
        if (binding.GetName() == LocksAcquireParamName) {
            auto& param = txState->Tx().ParamsState->Values[LocksAcquireParamName];
            param.MutableType()->SetKind(NKikimrMiniKQL::ETypeKind::Data);
            param.MutableType()->MutableData()->SetScheme(NKikimr::NUdf::TDataType<bool>::Id);
            param.MutableValue()->SetBool(acquireLocks);
            paramRef = NDq::TMkqlValueRef(param);
        } else if (binding.GetName() == LocksTxIdParamName) {
            auto& param = txState->Tx().ParamsState->Values[LocksTxIdParamName];
            param.MutableType()->SetKind(NKikimrMiniKQL::ETypeKind::Data);
            param.MutableType()->MutableData()->SetScheme(NKikimr::NUdf::TDataType<ui64>::Id);
            param.MutableValue()->SetUint64(txState->Tx().Locks.GetLockTxId());
            paramRef = NDq::TMkqlValueRef(param);
        } else if (binding.GetName() == ReadTargetParamName) {
            YQL_ENSURE(txState->Tx().EffectiveIsolationLevel);

            ui32 readTarget = (ui32)NKikimr::TReadTarget::EMode::Online;
            switch (*txState->Tx().EffectiveIsolationLevel) {
                case NKikimrKqp::ISOLATION_LEVEL_READ_UNCOMMITTED:
                    readTarget = (ui32)NKikimr::TReadTarget::EMode::Head;
                    break;
                case NKikimrKqp::ISOLATION_LEVEL_READ_STALE:
                    readTarget = (ui32)NKikimr::TReadTarget::EMode::Follower;
                    break;
                default:
                    break;
            }

            auto& param = txState->Tx().ParamsState->Values[ReadTargetParamName];
            param.MutableType()->SetKind(NKikimrMiniKQL::ETypeKind::Data);
            param.MutableType()->MutableData()->SetScheme(NKikimr::NUdf::TDataType<ui32>::Id);
            param.MutableValue()->SetUint32(readTarget);
            paramRef = NDq::TMkqlValueRef(param);
        } else if (binding.GetName() == NowParamName) {
            auto& param = txState->Tx().ParamsState->Values[binding.GetName()];
            param.MutableType()->SetKind(NKikimrMiniKQL::ETypeKind::Data);
            param.MutableType()->MutableData()->SetScheme(NKikimr::NUdf::TDataType<ui64>::Id);
            param.MutableValue()->SetUint64(transformCtx->QueryCtx->GetCachedNow());
            paramRef = NDq::TMkqlValueRef(param);
        } else if (binding.GetName() == CurrentDateParamName) {
            auto& param = txState->Tx().ParamsState->Values[binding.GetName()];
            param.MutableType()->SetKind(NKikimrMiniKQL::ETypeKind::Data);
            param.MutableType()->MutableData()->SetScheme(NKikimr::NUdf::TDataType<NUdf::TDate>::Id);
            ui64 date = transformCtx->QueryCtx->GetCachedDate();
            YQL_ENSURE(date <= Max<ui32>());
            param.MutableValue()->SetUint32(static_cast<ui32>(date));
            paramRef = NDq::TMkqlValueRef(param);
        } else if (binding.GetName() == CurrentDatetimeParamName) {
            auto& param = txState->Tx().ParamsState->Values[binding.GetName()];
            param.MutableType()->SetKind(NKikimrMiniKQL::ETypeKind::Data);
            param.MutableType()->MutableData()->SetScheme(NKikimr::NUdf::TDataType<NUdf::TDatetime>::Id);
            ui64 datetime = transformCtx->QueryCtx->GetCachedDatetime();
            YQL_ENSURE(datetime <= Max<ui32>());
            param.MutableValue()->SetUint32(static_cast<ui32>(datetime));
            paramRef = NDq::TMkqlValueRef(param);
        } else if (binding.GetName() == CurrentTimestampParamName) {
            auto& param = txState->Tx().ParamsState->Values[binding.GetName()];
            param.MutableType()->SetKind(NKikimrMiniKQL::ETypeKind::Data);
            param.MutableType()->MutableData()->SetScheme(NKikimr::NUdf::TDataType<NUdf::TTimestamp>::Id);
            param.MutableValue()->SetUint64(transformCtx->QueryCtx->GetCachedTimestamp());
            paramRef = NDq::TMkqlValueRef(param);
        } else if (binding.GetName() == RandomNumberParamName) {
            auto& param = txState->Tx().ParamsState->Values[binding.GetName()];
            param.MutableType()->SetKind(NKikimrMiniKQL::ETypeKind::Data);
            param.MutableType()->MutableData()->SetScheme(NKikimr::NUdf::TDataType<ui64>::Id);
            param.MutableValue()->SetUint64(transformCtx->QueryCtx->GetCachedRandom<ui64>());
            paramRef = NDq::TMkqlValueRef(param);
        } else if (binding.GetName() == RandomParamName) {
            auto& param = txState->Tx().ParamsState->Values[binding.GetName()];
            param.MutableType()->SetKind(NKikimrMiniKQL::ETypeKind::Data);
            param.MutableType()->MutableData()->SetScheme(NKikimr::NUdf::TDataType<double>::Id);
            param.MutableValue()->SetDouble(transformCtx->QueryCtx->GetCachedRandom<double>());
            paramRef = NDq::TMkqlValueRef(param);
        } else if (binding.GetName() == RandomUuidParamName) {
            auto& param = txState->Tx().ParamsState->Values[binding.GetName()];
            param.MutableType()->SetKind(NKikimrMiniKQL::ETypeKind::Data);
            param.MutableType()->MutableData()->SetScheme(NKikimr::NUdf::TDataType<NUdf::TUuid>::Id);
            auto uuid = transformCtx->QueryCtx->GetCachedRandom<TGUID>();
            param.MutableValue()->SetBytes(uuid.dw, sizeof(TGUID));
            paramRef = NDq::TMkqlValueRef(param);
        } else if (binding.HasMkqlIndex()) {
            paramRef = GetParamFromResult(binding, *transformCtx);
        } else {
            auto clientParam = transformCtx->QueryCtx->Parameters.FindPtr(binding.GetName());
            if (clientParam) {
                paramRef = NDq::TMkqlValueRef(*clientParam);
            } else {
                auto paramValue = txState->Tx().ParamsState->Values.FindPtr(binding.GetName());
                YQL_ENSURE(paramValue, "Parameter not found: " << binding.GetName());

                paramRef = NDq::TMkqlValueRef(*paramValue);
            }
        }

        YQL_ENSURE(paramRef);
        auto result = paramsMap.Values.emplace(std::make_pair(name, *paramRef));
        YQL_ENSURE(result.second);
    }

    return paramsMap;
}

TIssue GetLocksInvalidatedIssue(const TKqpTransactionContext& txCtx, const TMaybe<TKqpTxLock>& invalidatedLock) {
    TStringBuilder message;
    message << "Transaction locks invalidated.";

    TMaybe<TString> tableName;
    if (invalidatedLock) {
        TKikimrPathId id(invalidatedLock->GetSchemeShard(), invalidatedLock->GetPathId());
        auto table = txCtx.TableByIdMap.FindPtr(id);
        if (table) {
            tableName = *table;
        }
    }

    if (tableName) {
        message << " Table: " << *tableName;
    }

    return YqlIssue(TPosition(), TIssuesIds::KIKIMR_LOCKS_INVALIDATED, message);
}

std::pair<bool, std::vector<TIssue>> MergeLocks(const NKikimrMiniKQL::TType& type, const NKikimrMiniKQL::TValue& value,
        TKqpTransactionContext& txCtx) {

    std::pair<bool, std::vector<TIssue>> res;
    auto& locks = txCtx.Locks;

    YQL_ENSURE(type.GetKind() == NKikimrMiniKQL::ETypeKind::List);
    auto locksListType = type.GetList();

    if (!locks.HasLocks()) {
        locks.LockType = locksListType.GetItem();
        locks.LocksListType = locksListType;
    }

    YQL_ENSURE(locksListType.GetItem().GetKind() == NKikimrMiniKQL::ETypeKind::Struct);
    auto lockType = locksListType.GetItem().GetStruct();
    YQL_ENSURE(lockType.MemberSize() == 6);
    YQL_ENSURE(lockType.GetMember(0).GetName() == "Counter");
    YQL_ENSURE(lockType.GetMember(1).GetName() == "DataShard");
    YQL_ENSURE(lockType.GetMember(2).GetName() == "Generation");
    YQL_ENSURE(lockType.GetMember(3).GetName() == "LockId");
    YQL_ENSURE(lockType.GetMember(4).GetName() == "PathId");
    YQL_ENSURE(lockType.GetMember(5).GetName() == "SchemeShard");

    res.first = true;
    for (auto& lockValue : value.GetList()) {
        TKqpTxLock txLock(lockValue);
        if (auto counter = txLock.GetCounter(); counter >= NKikimr::TSysTables::TLocksTable::TLock::ErrorMin) {
            switch (counter) {
                case NKikimr::TSysTables::TLocksTable::TLock::ErrorAlreadyBroken:
                case NKikimr::TSysTables::TLocksTable::TLock::ErrorBroken:
                    res.second.emplace_back(GetLocksInvalidatedIssue(txCtx, txLock));
                    break;
                default:
                    res.second.emplace_back(YqlIssue(TPosition(), TIssuesIds::KIKIMR_LOCKS_ACQUIRE_FAILURE));
                    break;
            }
            res.first = false;

        } else if (auto curTxLock = locks.LocksMap.FindPtr(txLock.GetKey())) {
            if (curTxLock->Invalidated(txLock)) {
                res.second.emplace_back(GetLocksInvalidatedIssue(txCtx, txLock));
                res.first = false;
            }
        } else {
            // despite there were some errors we need to proceed merge to erase remaining locks properly
            locks.LocksMap.insert(std::make_pair(txLock.GetKey(), txLock));
        }
    }

    return res;
}

bool MergeLocks(const NKikimrMiniKQL::TType& type, const NKikimrMiniKQL::TValue& value, TKqpTransactionContext& txCtx,
        TExprContext& ctx) {
    auto [success, issues] = MergeLocks(type, value, txCtx);
    if (!success) {
        if (!txCtx.GetSnapshot().IsValid()) {
            for (auto& issue : issues) {
                ctx.AddError(std::move(issue));
            }
            return false;
        } else {
            txCtx.Locks.MarkBroken(issues.back());
            if (!txCtx.DeferredEffects.Empty()) {
                txCtx.Locks.ReportIssues(ctx);
                return false;
            }
        }
    }
    return true;
}

bool UnpackMergeLocks(const NKikimrMiniKQL::TResult& result, TKqpTransactionContext& txCtx, TExprContext& ctx) {
    auto structType = result.GetType().GetStruct();
    ui32 locksIndex;
    bool found = GetRunResultIndex(structType, TString(NKikimr::NMiniKQL::TxLocksResultLabel2), locksIndex);
    YQL_ENSURE(found ^ txCtx.Locks.Broken());

    if (found) {
        auto locksType = structType.GetMember(locksIndex).GetType().GetOptional().GetItem();
        auto locksValue = result.GetValue().GetStruct(locksIndex).GetOptional();

        return MergeLocks(locksType, locksValue, txCtx, ctx);
    }

    return false;
}

void LogMkqlResult(const NKikimrMiniKQL::TResult& result, TExprContext& ctx) {
    Y_UNUSED(ctx);

    if (YQL_CLOG_ACTIVE(TRACE, ProviderKqp)) {
        TString resultType;
        TString resultValue;

        NProtoBuf::TextFormat::PrintToString(result.GetType(), &resultType);
        NProtoBuf::TextFormat::PrintToString(result.GetValue(), &resultValue);

        YQL_CLOG(TRACE, ProviderKqp) << "MiniKQL results\n"
                << "Type:\n" << resultType
                << "Value:\n" << resultValue;
    }
}

bool HasEffects(const TKiProgram& program) {
    return !program.Effects().Maybe<TCoList>();
}

bool HasResults(const TKiProgram& program) {
    return !program.Results().Empty();
}

TVector<NKikimrKqp::TParameterBinding> CollectParams(TExprBase query) {
    TSet<TStringBuf> parametersSet;
    TVector<NKikimrKqp::TParameterBinding> bindings;

    VisitExpr(query.Ptr(), [&bindings, &parametersSet] (const TExprNode::TPtr& node) {
        if (auto maybeParam = TMaybeNode<TCoParameter>(node)) {
            auto param = maybeParam.Cast();
            auto result = parametersSet.insert(param.Name());
            if (result.second) {
                bindings.push_back(GetParameterBinding(param));
            }
        }

        return true;
    });

    return bindings;
}

TAutoPtr<IGraphTransformer> CreateKqpExecTransformer(TIntrusivePtr<IKqpGateway> gateway, const TString& cluster,
    TIntrusivePtr<TKqpTransactionState> txState, TIntrusivePtr<TKqlTransformContext> transformCtx)
{
    return new TKqpExecTransformer(gateway, cluster, txState, transformCtx);
}

} // namespace NKqp
} // namespace NKikimr
