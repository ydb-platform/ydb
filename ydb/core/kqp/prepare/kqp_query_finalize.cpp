#include "kqp_prepare_impl.h"

#include <ydb/core/kqp/provider/yql_kikimr_provider_impl.h>
#include <ydb/core/tx/datashard/sys_tables.h>

#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/core/issue/yql_issue.h>

namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NYql::NCommon;
using namespace NYql::NNodes;
using namespace NThreading;

namespace {

const TStringBuf LocksInvalidatedResultName = "tx_locks_invalidated";
const TStringBuf LocksInvalidatedListName = "tx_locks_invalidated_list";
const TStringBuf LocksTableName = "/sys/locks2";
const TStringBuf LocksTableVersion = "0";
const TString LocksTablePathId = TKikimrPathId(TSysTables::SysSchemeShard, TSysTables::SysTableLocks2).ToString();
const ui64 LocksInvalidatedCount = 1;

TExprBase GetDeferredEffectsList(const TDeferredEffects& effects, TPositionHandle pos, TExprContext& ctx) {
    if (effects.Empty()) {
        return GetEmptyEffectsList(pos, ctx);
    }

    TVector<TExprBase> effectNodes;
    effectNodes.reserve(effects.Size());
    for (const auto& effect : effects) {
        YQL_ENSURE(effect.Params.empty());
        YQL_ENSURE(effect.Node);
        effectNodes.push_back(effect.Node.Cast());
    }

    return Build<TCoExtend>(ctx, pos) 
        .Add(effectNodes)
        .Done();
}

TExprBase GetEraseLocksEffects(const TString& cluster, TPositionHandle pos, TCoParameter locksList, TExprContext& ctx) {
    return Build<TKiMapParameter>(ctx, pos)
        .Input(locksList)
        .Lambda()
            .Args({"lockItem"})
            .Body<TKiEraseRow>()
                .Cluster().Build(cluster)
                .Table<TKiVersionedTable>()
                    .Path<TCoAtom>().Build(LocksTableName)
                    .SchemaVersion<TCoAtom>().Build(LocksTableVersion)
                    .PathId<TCoAtom>().Build(LocksTablePathId)
                .Build()
                .Key()
                    .Add()
                        .Name().Build("LockId")
                        .Value<TCoMember>() 
                            .Struct("lockItem")
                            .Name().Build("LockId")
                            .Build()
                        .Build()
                    .Add()
                        .Name().Build("DataShard")
                        .Value<TCoMember>() 
                            .Struct("lockItem")
                            .Name().Build("DataShard")
                            .Build()
                        .Build()
                    .Add()
                        .Name().Build("SchemeShard")
                        .Value<TCoMember>()
                            .Struct("lockItem")
                            .Name().Build("SchemeShard")
                            .Build()
                        .Build()
                    .Add()
                        .Name().Build("PathId")
                        .Value<TCoMember>()
                            .Struct("lockItem")
                            .Name().Build("PathId")
                            .Build()
                        .Build()
                    .Build()
                .Build()
            .Build()
        .Done();
}

const TTypeAnnotationNode* GetTxLockListType(TExprContext& ctx) {
    auto ui32Type = ctx.MakeType<TDataExprType>(EDataSlot::Uint32);
    auto ui64Type = ctx.MakeType<TDataExprType>(EDataSlot::Uint64);
    TVector<const TItemExprType*> lockItems;
    lockItems.reserve(6);
    lockItems.push_back(ctx.MakeType<TItemExprType>("LockId", ui64Type));
    lockItems.push_back(ctx.MakeType<TItemExprType>("DataShard", ui64Type));
    lockItems.push_back(ctx.MakeType<TItemExprType>("SchemeShard", ui64Type));
    lockItems.push_back(ctx.MakeType<TItemExprType>("PathId", ui64Type));
    lockItems.push_back(ctx.MakeType<TItemExprType>("Generation", ui32Type));
    lockItems.push_back(ctx.MakeType<TItemExprType>("Counter", ui64Type));
    auto lockType = ctx.MakeType<TStructExprType>(lockItems);
    auto lockListType = ctx.MakeType<TListExprType>(lockType);
    return lockListType;
}

class TKqpFinalizeTransformer : public TGraphTransformerBase {
public:
    TKqpFinalizeTransformer(TIntrusivePtr<IKqpGateway> gateway, const TString& cluster,
        TIntrusivePtr<TKqpTransactionState> txState, TIntrusivePtr<TKqlTransformContext> transformCtx)
        : Gateway(gateway)
        , Cluster(cluster)
        , TxState(txState)
        , TransformCtx(transformCtx)
        , HasProgramResults(false) {}

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        output = input;

        YQL_ENSURE(State == EFinalizeState::Initial);

        auto resultsNode = GetResultsNode(TExprBase(input), ctx);
        TExprBase effectsNode = GetEmptyEffectsList(input->Pos(), ctx);

        auto settings = TransformCtx->Settings;

        if (settings.GetRollbackTx()) {
            YQL_ENSURE(!settings.GetCommitTx());

            YQL_CLOG(INFO, ProviderKqp) << "Rollback Tx"
                << ", deferred effects count: " << TxState->Tx().DeferredEffects.Size()
                << ", locks count: " << TxState->Tx().Locks.Size();

            effectsNode = GetRollbackEffects(input->Pos(), ctx);
            State = EFinalizeState::RollbackInProgress;
        }

        bool hasDataEffects = false;
        if (settings.GetCommitTx()) {
            YQL_ENSURE(!settings.GetRollbackTx());

            effectsNode = GetCommitEffects(input->Pos(), ctx, hasDataEffects);

            {
                if (!CheckCommitEffects(effectsNode, ctx)) {
                    return RollbackOnError(ctx);
                }

                if (TxState->Tx().IsInvalidated()) {
                    ctx.AddError(YqlIssue(ctx.GetPosition(input->Pos()), TIssuesIds::KIKIMR_OPERATION_ABORTED, TStringBuilder()
                        << "Failed to commit transaction due to previous errors."));
                    return RollbackOnError(ctx);
                }
            }

            YQL_CLOG(INFO, ProviderKqp) << "Commit Tx"
                << ", deferred effects count: " << TxState->Tx().DeferredEffects.Size()
                << ", locks count: " << TxState->Tx().Locks.Size();

            State = EFinalizeState::CommitInProgress;
        }

        auto program = Build<TKiProgram>(ctx, input->Pos())
            .Results(resultsNode)
            .Effects(effectsNode)
            .Done();

        HasProgramResults = HasResults(program);
        if (!HasProgramResults && !HasEffects(program)) {
            if (State != EFinalizeState::Initial) {
                ResetTxState(State == EFinalizeState::CommitInProgress);
            }

            State = EFinalizeState::Initial;
            return TStatus::Ok;
        }

        if (TransformCtx->QueryCtx->PrepareOnly) {
            YQL_ENSURE(!HasProgramResults);
            State = EFinalizeState::Initial;
            return TStatus::Ok;
        }

        return ExecuteProgram(program, ctx, hasDataEffects, ShouldWaitForResults(hasDataEffects));
    }

    TFuture<void> DoGetAsyncFuture(const TExprNode& input) final {
        Y_UNUSED(input);
        return Promise.GetFuture();
    }

    TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        output = input;

        NKikimr::NKqp::IKqpGateway::TMkqlResult result(MkqlExecuteResult.Future.ExtractValue());
        bool success = result.Success();

        if (success) {
            LogMkqlResult(result.Result, ctx);
            TransformCtx->AddMkqlStats(MkqlExecuteResult.Program, std::move(result.TxStats));
        }

        switch (State) {
            case EFinalizeState::CommitInProgress: {
                TMaybe<TKqpTxLock> invalidatedLock;
                bool locksOk = success
                    ? CheckInvalidatedLocks(result.Result, invalidatedLock)
                    : true;

                success = success && locksOk;

                if (!success) {
                    result.ReportIssues(ctx.IssueManager);

                    if (!locksOk) {
                        ctx.AddError(GetLocksInvalidatedIssue(TxState->Tx(), invalidatedLock));
                    }
                } else {
                    result.ReportIssues(ctx.IssueManager);

                    auto mkqlResult = MakeSimpleShared<NKikimrMiniKQL::TResult>();
                    mkqlResult->Swap(&result.Result);

                    if (HasProgramResults) {
                        TransformCtx->MkqlResults.push_back(mkqlResult);
                    }
                }

                break;
            }

            case EFinalizeState::RollbackInProgress: {
                success = true;
                break;
            }

            case EFinalizeState::RollbackOnErrorInProgress: {
                success = false;
                break;
            }

            default:
                YQL_ENSURE(false, "Unexpected state in finalize transformer.");
                break;
        }

        ResetTxState(State == EFinalizeState::CommitInProgress);
        State = EFinalizeState::Initial;

        return success ? TStatus::Ok : TStatus::Error;
    }

    void Rewind() override {
        State = EFinalizeState::Initial;
    }

private:
    enum class EFinalizeState {
        Initial,
        CommitInProgress,
        RollbackInProgress,
        RollbackOnErrorInProgress
    };

    TStatus ExecuteProgram(TKiProgram program, TExprContext& ctx, bool hasDataEffects, bool waitForResults) {
        if (waitForResults) {
            Promise = NewPromise();
            MkqlExecuteResult = ExecuteMkql(program, Gateway, Cluster, ctx, TxState, TransformCtx, hasDataEffects);

            auto promise = Promise;
            MkqlExecuteResult.Future.Apply([promise](const TFuture<IKqpGateway::TMkqlResult> future) mutable {
                YQL_ENSURE(future.HasValue());
                promise.SetValue();
            });

            return TStatus::Async;
        }

        YQL_ENSURE(!hasDataEffects);

        ExecuteMkql(program, Gateway, Cluster, ctx, TxState, TransformCtx, false);

        ResetTxState(true);
        State = EFinalizeState::Initial;

        return TStatus::Ok;
    }

    TStatus RollbackOnError(TExprContext& ctx) {
        YQL_ENSURE(State == EFinalizeState::Initial);

        YQL_CLOG(INFO, ProviderKqp) << "Rollback Tx On Error"
            << ", deferred effects count: " << TxState->Tx().DeferredEffects.Size()
            << ", locks count: " << TxState->Tx().Locks.Size();

        auto program = Build<TKiProgram>(ctx, TPositionHandle())
            .Results()
                .Build()
            .Effects(GetRollbackEffects(TPositionHandle(), ctx))
            .Done();

        if (!HasEffects(program)) {
            ResetTxState(false);

            return TStatus::Error;
        }

        State = EFinalizeState::RollbackOnErrorInProgress;
        return ExecuteProgram(program, ctx, false, true);
    }

    TExprList GetResultsNode(TExprBase program, TExprContext& ctx) {
        TMaybeNode<TExprList> resultsNode;

        if (program.Maybe<TCoWorld>()) {
            resultsNode = Build<TExprList>(ctx, program.Pos()).Done();
        } else {
            const auto& analyzeResults = TransformCtx->AnalyzeResults;
            YQL_ENSURE(analyzeResults.CanExecute);

            resultsNode = program.Cast<TKiProgram>().Results();
        }

        return resultsNode.Cast();
    }

    bool CheckCommitEffects(TExprBase effects, TExprContext& ctx) const {
        TIssueScopeGuard issueScope(ctx.IssueManager, [effects, &ctx]() {
            return MakeIntrusive<TIssue>(YqlIssue(ctx.GetPosition(effects.Pos()), TIssuesIds::DEFAULT_ERROR,
                "Failed to commit transaction"));
        });

        TMaybeNode<TExprBase> blackistedNode;
        ui32 readsCount = 0;
        VisitExpr(effects.Ptr(), [&blackistedNode, &readsCount](const TExprNode::TPtr& exprNode) {
            if (blackistedNode) {
                return false;
            }

            if (auto maybeCallable = TMaybeNode<TCallable>(exprNode)) {
                auto callable = maybeCallable.Cast();

                if (callable.CallableName() == "Udf" ||
                    callable.Maybe<TKiSelectRange>())
                {
                    blackistedNode = callable;
                    return false;
                }

                if (callable.Maybe<TKiSelectRow>()) {
                    ++readsCount;
                }
            }

            return true;
        });

        if (blackistedNode) {
            ctx.AddError(TIssue(ctx.GetPosition(blackistedNode.Cast().Pos()), TStringBuilder()
                << "Callable not expected in commit tx: " << blackistedNode.Cast<TCallable>().CallableName()));
            return false;
        }

        ui32 maxReadsCount = TransformCtx->Config->_CommitReadsLimit.Get().GetRef();
        if (readsCount > maxReadsCount) {
            ctx.AddError(TIssue(ctx.GetPosition(effects.Pos()), TStringBuilder()
                << "Reads limit exceeded in commit tx: " << readsCount << " > " << maxReadsCount));
            return false;
        }

        return true;
    }

    static bool CheckInvalidatedLocks(const NKikimrMiniKQL::TResult& result, TMaybe<TKqpTxLock>& invalidatedLock) {
        auto structType = result.GetType().GetStruct();

        ui32 resultIndex;
        if (!GetRunResultIndex(structType, TString(LocksInvalidatedResultName), resultIndex)) {
            return true;
        }

        auto locksResult = result.GetValue().GetStruct(resultIndex);
        if (locksResult.HasOptional()) {
            bool invalidated = locksResult.GetOptional().GetBool();
            YQL_ENSURE(invalidated);

            ui32 listIndex;
            if (GetRunResultIndex(structType, TString(LocksInvalidatedListName), listIndex)) {
                auto& locksResult = result.GetValue().GetStruct(listIndex);
                auto& list = locksResult.GetOptional().GetList();
                if (!list.empty()) {
                    invalidatedLock = TKqpTxLock(list.Get(0));
                }
            }

            return false;
        }

        return true;
    }

    void ResetTxState(bool committed) {
        if (!committed) {
            TxState->Tx().Invalidate();
        }

        TxState->Tx().ClearDeferredEffects();
        TxState->Tx().Locks.Clear();
        TxState->Tx().Finish();
    }

    TExprBase GetRollbackEffects(TPositionHandle pos, TExprContext& ctx) {
        if (!TxState->Tx().Locks.HasLocks()) {
            return GetEmptyEffectsList(pos, ctx);
        }

        auto locksParamName = TxState->Tx().NewParamName();
        YQL_ENSURE(TxState->Tx().ParamsState->Values.emplace(std::make_pair(locksParamName,
            GetLocksParamValue(TxState->Tx().Locks))).second);

        auto locksParamNode = Build<TCoParameter>(ctx, pos) 
            .Name().Build(locksParamName)
            .Type(ExpandType(pos, *GetTxLockListType(ctx), ctx))
            .Done();

        return GetEraseLocksEffects(Cluster, pos, locksParamNode, ctx);
    }

    TExprBase GetCommitEffects(TPositionHandle pos, TExprContext& ctx, bool& hasDataEffects) {
        hasDataEffects = !TxState->Tx().DeferredEffects.Empty();

        Y_VERIFY_DEBUG(!hasDataEffects || !TxState->Tx().Locks.Broken());

        auto deferredEffects = GetDeferredEffectsList(TxState->Tx().DeferredEffects, pos, ctx);

        if (!TxState->Tx().Locks.HasLocks()) {
            return deferredEffects;
        }

        auto locksParamName = TxState->Tx().NewParamName();
        YQL_ENSURE(TxState->Tx().ParamsState->Values.emplace(std::make_pair(locksParamName,
            GetLocksParamValue(TxState->Tx().Locks))).second);

        auto locksParamNode = Build<TCoParameter>(ctx, pos) 
            .Name().Build(locksParamName)
            .Type(ExpandType(pos, *GetTxLockListType(ctx), ctx))
            .Done();

        if (!hasDataEffects && TxState->Tx().GetSnapshot().IsValid())
            return GetEraseLocksEffects(Cluster, pos, locksParamNode, ctx);

        auto lockArg = Build<TCoArgument>(ctx, pos) 
            .Name("lockArg")
            .Done();

        auto selectLock = Build<TKiSelectRow>(ctx, pos)
            .Cluster().Build(Cluster)
            .Table<TKiVersionedTable>()
                .Path<TCoAtom>().Build(LocksTableName)
                .SchemaVersion<TCoAtom>().Build(LocksTableVersion)
                .PathId<TCoAtom>().Build(LocksTablePathId)
            .Build()
            .Key()
                .Add()
                    .Name().Build("LockId")
                    .Value<TCoMember>() 
                        .Struct(lockArg)
                        .Name().Build("LockId")
                        .Build()
                    .Build()
                .Add()
                    .Name().Build("DataShard")
                    .Value<TCoMember>() 
                        .Struct(lockArg)
                        .Name().Build("DataShard")
                        .Build()
                    .Build()
                .Add()
                    .Name().Build("SchemeShard")
                    .Value<TCoMember>()
                        .Struct(lockArg)
                        .Name().Build("SchemeShard")
                        .Build()
                    .Build()
                .Add()
                    .Name().Build("PathId")
                    .Value<TCoMember>()
                        .Struct(lockArg)
                        .Name().Build("PathId")
                        .Build()
                    .Build()
                .Build()
            .Select()
                .Add().Build("Generation")
                .Add().Build("Counter")
                .Build()
            .Done();

        TVector<TExprBase> args = {
            Build<TCoCmpEqual>(ctx, pos)
                .Left<TCoMember>()
                    .Struct(selectLock)
                    .Name().Build("Generation")
                    .Build()
                .Right<TCoMember>()
                    .Struct(lockArg)
                    .Name().Build("Generation")
                    .Build()
                .Done(),
            Build<TCoCmpEqual>(ctx, pos)
                .Left<TCoMember>()
                    .Struct(selectLock)
                    .Name().Build("Counter")
                    .Build()
                .Right<TCoMember>()
                    .Struct(lockArg)
                    .Name().Build("Counter")
                    .Build()
                .Done()
        };
        auto lockPredicate = Build<TCoNot>(ctx, pos) 
            .Value<TCoCoalesce>() 
                .Predicate<TCoAnd>() 
                    .Add(args)
                    .Build()
                .Value<TCoBool>() 
                    .Literal().Build("false")
                    .Build()
                .Build()
            .Done();

        auto locksInvalidatedList = Build<TKiFlatMapParameter>(ctx, pos)
            .Input(locksParamNode)
            .Lambda()
                .Args(lockArg)
                .Body<TCoListIf>()
                    .Predicate(lockPredicate)
                    .Value(lockArg)
                    .Build()
                .Build()
            .Done();

        auto locksInvalidatedPredicate = Build<TCoHasItems>(ctx, pos)
            .List(locksInvalidatedList)
            .Done();

        auto effects = Build<TCoExtend>(ctx, pos) 
            .Add<TCoIf>() 
                .Predicate(locksInvalidatedPredicate)
                .ThenValue<TCoAsList>() 
                    .Add<TKiSetResult>()
                        .Name().Build(LocksInvalidatedResultName)
                        .Data(locksInvalidatedPredicate)
                        .Build()
                    .Add<TKiSetResult>()
                        .Name().Build(LocksInvalidatedListName)
                        .Data<TCoTake>()
                            .Input(locksInvalidatedList)
                            .Count<TCoUint64>()
                                .Literal().Build(ToString(LocksInvalidatedCount))
                                .Build()
                            .Build()
                        .Build()
                    .Build()
                .ElseValue(deferredEffects)
                .Build()
            .Add(GetEraseLocksEffects(Cluster, pos, locksParamNode, ctx))
            .Done();

        return effects;
    }

    bool ShouldWaitForResults(bool hasDataEffects) {
        if (State != EFinalizeState::CommitInProgress) {
            return true;
        }

        if (hasDataEffects) {
            return true;
        }

        if (!TxState->Tx().GetSnapshot().IsValid()) {
            return true;
        }

        if (HasProgramResults) {
            return true;
        }

        return false;
    }

private:
    TIntrusivePtr<IKqpGateway> Gateway;
    TString Cluster;
    TIntrusivePtr<TKqpTransactionState> TxState;
    TIntrusivePtr<TKqlTransformContext> TransformCtx;
    EFinalizeState State;
    bool HasProgramResults;
    TMkqlExecuteResult MkqlExecuteResult;
    TPromise<void> Promise;
};

} // namespace

TCoList GetEmptyEffectsList(const TPositionHandle pos, TExprContext& ctx) {
    return Build<TCoList>(ctx, pos) 
        .ListType<TCoListType>() 
            .ItemType<TCoVoidType>() 
                .Build()
            .Build()
        .Done();
}

NKikimrMiniKQL::TParams GetLocksParamValue(const TKqpTxLocks& locks) {
    YQL_ENSURE(locks.HasLocks());

    NKikimrMiniKQL::TParams locksResult;
    auto type = locksResult.MutableType();
    type->SetKind(NKikimrMiniKQL::ETypeKind::List);
    type->MutableList()->CopyFrom(locks.LocksListType);
    auto value = locksResult.MutableValue();
    for (auto& pair : locks.LocksMap) {
        auto lockValue = value->AddList();
        lockValue->CopyFrom(pair.second.GetValue());
    }

    return locksResult;
}

TAutoPtr<IGraphTransformer> CreateKqpFinalizeTransformer(TIntrusivePtr<IKqpGateway> gateway, const TString& cluster,
    TIntrusivePtr<TKqpTransactionState> txState, TIntrusivePtr<TKqlTransformContext> transformCtx)
{
    return new TKqpFinalizeTransformer(gateway, cluster, txState, transformCtx);
}

} // namespace NKqp
} // namespace NKikimr
