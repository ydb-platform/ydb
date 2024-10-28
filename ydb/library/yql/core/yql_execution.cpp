#include "yql_execution.h"
#include "yql_expr_optimize.h"
#include "yql_opt_proposed_by_data.h"

#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <util/string/builder.h>
#include <util/string/join.h>
#include <util/system/env.h>
#include <util/generic/queue.h>


namespace NYql {

namespace {

const bool RewriteSanityCheck = false;

class TExecutionTransformer : public TGraphTransformerBase {
public:
    struct TState : public TThrRefBase {
        TAdaptiveLock Lock;

        struct TItem : public TIntrusiveListItem<TItem> {
            TExprNode* Node = nullptr;
            IDataProvider* DataProvider = nullptr;
            NThreading::TFuture<void> Future;
        };

        using TQueueType = TIntrusiveListWithAutoDelete<TState::TItem, TDelete>;
        TQueueType Completed;
        TQueueType Inflight;
        NThreading::TPromise<void> Promise;
        bool HasResult = false;
    };

    using TStatePtr = TIntrusivePtr<TState>;

    TExecutionTransformer(TTypeAnnotationContext& types,
        TOperationProgressWriter writer,
        bool withFinalize)
        : Types(types)
        , Writer(writer)
        , WithFinalize(withFinalize)
        , DeterministicMode(GetEnv("YQL_DETERMINISTIC_MODE"))
    {
        Rewind();
    }

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        if (FinalizingTransformer) {
            YQL_CLOG(INFO, CoreExecution) << "FinalizingTransformer, root #" << input->UniqueId();
            auto status = FinalizingTransformer->Transform(input, output, ctx);
            YQL_CLOG(INFO, CoreExecution) << "FinalizingTransformer done, output #" << output->UniqueId() << ", status: " << status;
            return status;
        }

        YQL_CLOG(INFO, CoreExecution) << "Begin, root #" << input->UniqueId();
        output = input;
        if (RewriteSanityCheck) {
            VisitExpr(input, [&](const TExprNode::TPtr& localInput) {
                if (NewNodes.cend() != NewNodes.find(localInput.Get())) {
                    Cerr << "found old node: #" << localInput->UniqueId() << "\n" << input->Dump();
                    YQL_ENSURE(false);
                }
                return true;
            });
        }

        auto status = CollectUnusedNodes(*input, ctx);
        YQL_CLOG(INFO, CoreExecution) << "Collect unused nodes for root #" << input->UniqueId() << ", status: " << status;
        if (status != TStatus::Ok) {
            return status;
        }

        status = status.Combine(ExecuteNode(input, output, ctx, 0));
        for (auto node: FreshPendingNodes) {
            if (TExprNode::EState::ExecutionPending == node->GetState()) {
                node->SetState(TExprNode::EState::ConstrComplete);
            }
        }
        FreshPendingNodes.clear();
        if (!ReplaceNewNodes(output, ctx)) {
            return TStatus::Error;
        }
        YQL_CLOG(INFO, CoreExecution) << "Finish, output #" << output->UniqueId() << ", status: " << status;

        if (status != TStatus::Ok || !WithFinalize) {
            return status;
        }

        YQL_CLOG(INFO, CoreExecution) << "Creating finalizing transformer, output #" << output->UniqueId();
        FinalizingTransformer = CreateCompositeFinalizingTransformer(Types);
        return FinalizingTransformer->Transform(input, output, ctx);
    }

    NThreading::TFuture<void> DoGetAsyncFuture(const TExprNode& input) final {
        return FinalizingTransformer ?
            FinalizingTransformer->GetAsyncFuture(input) :
            State->Promise.GetFuture();
    }

    TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        if (FinalizingTransformer) {
            return FinalizingTransformer->ApplyAsyncChanges(input, output, ctx);
        }

        output = input;

        TStatus combinedStatus = TStatus::Ok;

        TState::TQueueType completed;
        auto newPromise = NThreading::NewPromise();
        {
            TGuard<TAdaptiveLock> guard(State->Lock);
            completed.Swap(State->Completed);
            State->Promise.Swap(newPromise);
            State->HasResult = false;
        }

        for (auto& item : completed) {
            auto collectedIt = CollectingNodes.find(item.Node);
            if (collectedIt != CollectingNodes.end()) {
                YQL_CLOG(INFO, CoreExecution) << "Completed async cleanup for node #" << item.Node->UniqueId();
                TExprNode::TPtr callableOutput;
                auto status = item.DataProvider->GetTrackableNodeProcessor().GetCleanupTransformer().ApplyAsyncChanges(item.Node, callableOutput, ctx);
                combinedStatus = combinedStatus.Combine(status);
                CollectingNodes.erase(collectedIt);
                continue;
            }

            YQL_CLOG(INFO, CoreExecution) << "Completed async execution for node #" << item.Node->UniqueId();
            auto asyncIt = AsyncNodes.find(item.Node);
            YQL_ENSURE(asyncIt != AsyncNodes.end());
            TExprNode::TPtr callableOutput;
            auto status = item.DataProvider->GetCallableExecutionTransformer().ApplyAsyncChanges(item.Node, callableOutput, ctx);
            Y_ABORT_UNLESS(callableOutput);
            YQL_ENSURE(status != TStatus::Async);
            combinedStatus = combinedStatus.Combine(status);
            if (status.Level == TStatus::Error) {
                item.Node->SetState(TExprNode::EState::Error);
            } else if (status.Level == TStatus::Repeat) {
                if (callableOutput != item.Node) {
                    YQL_CLOG(INFO, CoreExecution) << "Rewrite node #" << item.Node->UniqueId() << " to #" << callableOutput->UniqueId()
                        << " in ApplyAsyncChanges()";
                    NewNodes[item.Node] = callableOutput;
                    combinedStatus = combinedStatus.Combine(TStatus(TStatus::Repeat, true));
                    FinishNode(item.DataProvider->GetName(), *item.Node, *callableOutput);
                }
            }
            if (callableOutput == item.Node) {
                YQL_CLOG(INFO, CoreExecution) << "State is " << item.Node->GetState()
                        << " after apply async changes for node #" << item.Node->UniqueId();
            }

            if (item.Node->GetState() == TExprNode::EState::ExecutionComplete ||
                item.Node->GetState() == TExprNode::EState::Error)
            {
                FinishNode(item.DataProvider->GetName(), *item.Node, *callableOutput);
            }

            AsyncNodes.erase(asyncIt);
        }

        if (!ReplaceNewNodes(output, ctx)) {
            return TStatus::Error;
        }

        if (!completed.Empty() && combinedStatus.Level == TStatus::Ok) {
            combinedStatus = TStatus::Repeat;
        }

        return combinedStatus;
    }

    bool ReplaceNewNodes(TExprNode::TPtr& output, TExprContext& ctx) {
        if (!NewNodes.empty()) {
            TOptimizeExprSettings settings(&Types);
            settings.VisitChanges = true;
            settings.VisitStarted = true;
            auto replaceStatus = OptimizeExpr(output, output, [&](const TExprNode::TPtr& input, TExprContext& ctx) -> TExprNode::TPtr {
                Y_UNUSED(ctx);
                const auto replace = NewNodes.find(input.Get());
                if (NewNodes.cend() != replace) {
                    return replace->second;
                }

                return input;
            }, ctx, settings);

            if (!RewriteSanityCheck) {
                NewNodes.clear();
            }

            if (replaceStatus.Level == TStatus::Error) {
                return false;
            }
        }

        if (RewriteSanityCheck) {
            VisitExpr(output, [&](const TExprNode::TPtr& localInput) {
                if (NewNodes.cend() != NewNodes.find(localInput.Get())) {
                    Cerr << "found old node: #" << localInput->UniqueId() << "\n" << output->Dump();
                    YQL_ENSURE(false);
                }
                return true;
            });
        }
        return true;
    }

    void Rewind() override {
        State = MakeIntrusive<TState>();
        State->Promise = NThreading::NewPromise();
        State->HasResult = false;
        NewNodes.clear();
        FinalizingTransformer.Reset();

        TrackableNodes.clear();
        CollectingNodes.clear();
        ProvidersCache.clear();
        AsyncNodes.clear();
    }

    TStatus ExecuteNode(const TExprNode::TPtr& node, TExprNode::TPtr& output, TExprContext& ctx, ui32 depth) {
        output = node;
        bool changed = false;
        const auto knownNode = NewNodes.find(node.Get());
        if (NewNodes.cend() != knownNode) {
            output = knownNode->second;
            changed = true;
        }

        switch (output->GetState()) {
        case TExprNode::EState::Initial:
        case TExprNode::EState::TypeInProgress:
        case TExprNode::EState::TypePending:
        case TExprNode::EState::TypeComplete:
        case TExprNode::EState::ConstrInProgress:
        case TExprNode::EState::ConstrPending:
            return TStatus(TStatus::Repeat, true);
        case TExprNode::EState::ExecutionInProgress:
            return TStatus::Async;
        case TExprNode::EState::ExecutionPending:
            return ExecuteChildren(output, output, ctx, depth + 1);
        case TExprNode::EState::ConstrComplete:
        case TExprNode::EState::ExecutionRequired:
            break;
        case TExprNode::EState::ExecutionComplete:
            YQL_ENSURE(output->HasResult());
            OnNodeExecutionComplete(output, ctx);
            return changed ? TStatus(TStatus::Repeat, true) : TStatus(TStatus::Ok);
        case TExprNode::EState::Error:
            return TStatus::Error;
        default:
            YQL_ENSURE(false, "Unknown state");
        }

        switch (output->Type()) {
        case TExprNode::Atom:
        case TExprNode::Argument:
        case TExprNode::Arguments:
        case TExprNode::Lambda:
            ctx.AddError(TIssue(ctx.GetPosition(output->Pos()), TStringBuilder() << "Failed to execute node with type: " << output->Type()));
            output->SetState(TExprNode::EState::Error);
            return TStatus::Error;

        case TExprNode::List:
        case TExprNode::Callable:
        {
            auto prevOutput = output;
            auto status = output->Type() == TExprNode::Callable
                ? ExecuteCallable(output, output, ctx, depth)
                : ExecuteList(output, ctx);
            if (status.Level == TStatus::Error) {
                output->SetState(TExprNode::EState::Error);
            } else if (status.Level == TStatus::Ok) {
                output->SetState(TExprNode::EState::ExecutionComplete);
                OnNodeExecutionComplete(output, ctx);
                YQL_ENSURE(output->HasResult());
            } else if (status.Level == TStatus::Repeat) {
                if (!status.HasRestart) {
                    output->SetState(TExprNode::EState::ExecutionPending);
                    status = ExecuteChildren(output, output, ctx, depth + 1);
                    if (TExprNode::EState::ExecutionPending == output->GetState()) {
                        FreshPendingNodes.push_back(output.Get());
                    }
                    if (status.Level != TStatus::Repeat) {
                        return status;
                    }
                }
                if (output != prevOutput) {
                    YQL_CLOG(INFO, CoreExecution) << "Rewrite node #" << node->UniqueId() << " to #" << output->UniqueId();
                    NewNodes[node.Get()] = output;
                }
                return TStatus(TStatus::Repeat, output != prevOutput);
            } else if (status.Level == TStatus::Async) {
                output->SetState(TExprNode::EState::ExecutionInProgress);
            }

            return status;
        }

        case TExprNode::World:
            output->SetState(TExprNode::EState::ExecutionComplete);
            return TStatus::Ok;

        default:
            YQL_ENSURE(false, "Unknown type");
        }
    }

    TStatus ExecuteChildren(const TExprNode::TPtr& node, TExprNode::TPtr& output, TExprContext& ctx, ui32 depth) {
        TStatus combinedStatus = TStatus::Ok;
        TExprNode::TListType newChildren;
        bool newNode = false;
        for (auto& child : node->Children()) {
            auto newChild = child;
            if (child->GetState() == TExprNode::EState::ExecutionRequired) {
                auto childStatus = ExecuteNode(child, newChild, ctx, depth);
                if (childStatus.Level == TStatus::Error)
                    return childStatus;

                combinedStatus = combinedStatus.Combine(childStatus);
            } else if (child->GetState() == TExprNode::EState::ExecutionInProgress) {
                combinedStatus = combinedStatus.Combine(TStatus::Async);
            } else if (child->GetState() == TExprNode::EState::ExecutionPending) {
                combinedStatus = combinedStatus.Combine(TStatus::Repeat);
            }
            newChildren.push_back(newChild);
            if (newChild != child) {
                newNode = true;
            }
        }

        if (combinedStatus.Level == TStatus::Ok) {
            Y_DEBUG_ABORT_UNLESS(!newNode);
            node->SetState(TExprNode::EState::ConstrComplete);
            return ExecuteNode(node, output, ctx, depth - 1);
        } else {
            if (combinedStatus.Level == TStatus::Error) {
                node->SetState(TExprNode::EState::Error);
            }
            if (newNode) {
                output = ctx.ChangeChildren(*node, std::move(newChildren));
            }

            return combinedStatus;
        }
    }

    TStatus ExecuteList(const TExprNode::TPtr& node, TExprContext& ctx) {
        IGraphTransformer::TStatus combinedStatus = IGraphTransformer::TStatus::Ok;
        for (ui32 i = 0; i < node->ChildrenSize(); ++i) {
            combinedStatus = combinedStatus.Combine(RequireChild(*node, i));
        }

        if (combinedStatus.Level != IGraphTransformer::TStatus::Ok) {
            return combinedStatus;
        }

        node->SetResult(ctx.NewWorld(node->Pos()));
        return TStatus::Ok;
    }

    IDataProvider* GetDataProvider(const TExprNode& node) const {
        IDataProvider* dataProvider = nullptr;
        for (const auto& x : Types.DataSources) {
            if (x->CanExecute(node)) {
                dataProvider = x.Get();
                break;
            }
        }

        if (!dataProvider) {
            for (const auto& x : Types.DataSinks) {
                if (x->CanExecute(node)) {
                    dataProvider = x.Get();
                }
            }
        }
        return dataProvider;
    }

    TStatus ExecuteCallable(const TExprNode::TPtr& node, TExprNode::TPtr& output, TExprContext& ctx, ui32 depth) {
        YQL_CLOG(TRACE, CoreExecution) << '{' << depth << "}, callable #"
                << node->UniqueId() << " <" << node->Content() << '>';
        if (node->Content() == CommitName) {
            auto requireStatus = RequireChild(*node, 0);
            if (requireStatus.Level != TStatus::Ok) {
                return requireStatus;
            }

            auto category = node->Child(1)->Child(0)->Content();
            auto datasink = Types.DataSinkMap.FindPtr(category);
            YQL_ENSURE(datasink);
            output = node;
            auto status = (*datasink)->GetCallableExecutionTransformer().Transform(node, output, ctx);
            if (status.Level == TStatus::Async) {
                Y_DEBUG_ABORT_UNLESS(output == node);
                StartNode(category, *node);
                AddCallable(node, (*datasink).Get(), ctx);
            } else {
                if (output->GetState() == TExprNode::EState::ExecutionComplete ||
                    output->GetState() == TExprNode::EState::Error)
                {
                    StartNode(category, *node);
                    FinishNode(category, *node, *output);
                }
            }

            return status;
        }

        if (node->Content() == SyncName) {
            return ExecuteList(node, ctx);
        }

        if (node->Content() == LeftName) {
            auto requireStatus = RequireChild(*node, 0);
            if (requireStatus.Level != TStatus::Ok) {
                return requireStatus;
            }

            node->SetResult(ctx.NewWorld(node->Pos()));
            return TStatus::Ok;
        }

        if (node->Content() == RightName) {
            auto requireStatus = RequireChild(*node, 0);
            if (requireStatus.Level != TStatus::Ok) {
                return requireStatus;
            }

            node->SetResult(ctx.NewWorld(node->Pos()));
            return TStatus::Ok;
        }

        IDataProvider* dataProvider = GetDataProvider(*node);
        if (!dataProvider) {
            ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), TStringBuilder() << "Failed to execute callable with name: " << node->Content()));
            return TStatus::Error;
        }

        output = node;
        TStatus status = dataProvider->GetCallableExecutionTransformer().Transform(node, output, ctx);
        if (status.Level == TStatus::Async) {
            Y_DEBUG_ABORT_UNLESS(output == node);
            StartNode(dataProvider->GetName(), *node);
            AddCallable(node, dataProvider, ctx);
        } else {
            if (output->GetState() == TExprNode::EState::ExecutionComplete ||
                output->GetState() == TExprNode::EState::Error)
            {
                StartNode(dataProvider->GetName(), *node);
                FinishNode(dataProvider->GetName(), *node, *output);
            }
        }

        return status;
    }

    void AddCallable(const TExprNode::TPtr& node, IDataProvider* dataProvider, TExprContext& ctx) {
        Y_UNUSED(ctx);
        YQL_CLOG(INFO, CoreExecution) << "Register async execution for node #" << node->UniqueId();
        auto future = dataProvider->GetCallableExecutionTransformer().GetAsyncFuture(*node);
        AsyncNodes[node.Get()] = node;
        SubscribeAsyncFuture(node, dataProvider, future);
    }

    static void ProcessFutureResultQueue(TStatePtr state) {
        NThreading::TPromise<void> promiseToSet;
        bool hasResult = false;

        TGuard<TAdaptiveLock> guard(state->Lock);
        while (!state->Inflight.Empty()) {
            auto* first = state->Inflight.Front();
            if (first->Future.HasValue()) {
                state->Inflight.PopFront();
                state->Completed.PushBack(first);
                hasResult = true;
            } else {
                break;
            }
        }
        guard.Release();

        if (hasResult && !state->HasResult) {
            state->HasResult = true;
            promiseToSet = state->Promise;
        }

        if (promiseToSet.Initialized()) {
            promiseToSet.SetValue();
        }
    }

    static void ProcessAsyncFutureResult(TStatePtr state, TAutoPtr<TState::TItem> item) {
        NThreading::TPromise<void> promiseToSet;
        {
            TGuard<TAdaptiveLock> guard(state->Lock);
            state->Completed.PushBack(item.Release());
            if (!state->HasResult) {
                state->HasResult = true;
                promiseToSet = state->Promise;
            }
        }

        if (promiseToSet.Initialized()) {
            promiseToSet.SetValue();
        }
    }

    void SubscribeAsyncFuture(const TExprNode::TPtr& node, IDataProvider* dataProvider, const NThreading::TFuture<void>& future)
    {
        auto state = State;
        if (DeterministicMode) {
            TAutoPtr<TState::TItem> item = new TState::TItem;
            item->Node = node.Get(); item->DataProvider = dataProvider; item->Future = future;

            TGuard<TAdaptiveLock> guard(state->Lock);
            state->Inflight.PushBack(item.Release());
        }

        if (DeterministicMode) {
            future.Subscribe([state](const NThreading::TFuture<void>& future) {
                YQL_ENSURE(!future.HasException());
                ProcessFutureResultQueue(state);
            });
        } else {
            future.Subscribe([state, node=node.Get(), dataProvider](const NThreading::TFuture<void>& future) {
                YQL_ENSURE(!future.HasException());

                TAutoPtr<TState::TItem> item = new TState::TItem;
                item->Node = node; item->DataProvider = dataProvider;
                ProcessAsyncFutureResult(state, item.Release());
            });
        }
    }

    void StartNode(TStringBuf category, const TExprNode& node) {
        auto publicId = Types.TranslateOperationId(node.UniqueId());
        if (publicId) {
            auto x = Progresses.insert({ *publicId,
                TOperationProgress(TString(category), *publicId, TOperationProgress::EState::Started) });
            if (x.second) {
                Writer(x.first->second);
            }
        }
    }

    void FinishNode(TStringBuf category, const TExprNode& node, const TExprNode& newNode) {
        auto publicId = Types.TranslateOperationId(node.UniqueId());
        if (publicId) {
            if (newNode.UniqueId() != node.UniqueId()) {
                Types.NodeToOperationId[newNode.UniqueId()] = *publicId;
            }

            auto progIt = Progresses.find(*publicId);
            YQL_ENSURE(progIt != Progresses.end());

            auto newState = (node.GetState() == TExprNode::EState::ExecutionComplete)
                    ? TOperationProgress::EState::Finished
                    : TOperationProgress::EState::Failed;

            if (progIt->second.State != newState) {
                TString stage = progIt->second.Stage.first;
                progIt->second = TOperationProgress(TString(category), *publicId, newState, stage);
                Writer(progIt->second);
            }
        }
    }

    void OnNodeExecutionComplete(const TExprNode::TPtr& node, TExprContext& ctx) {
        auto nodeId = node->UniqueId();
        YQL_CLOG(INFO, CoreExecution) << "Node #" << nodeId << "<" << node->Content() << "> finished execution";

        auto dataProvider = GetDataProvider(*node);
        if (!dataProvider) {
            return;
        }

        TVector<ITrackableNodeProcessor::TExprNodeAndId> createdNodes;
        dataProvider->GetTrackableNodeProcessor().GetCreatedNodes(*node, createdNodes, ctx);

        TVector<TString> ids;
        for (const auto& c : createdNodes) {
            auto& info = TrackableNodes[c.Id];
            info.Provider = dataProvider;
            info.Node = c.Node;
            ids.push_back(c.Id);
        }
        YQL_CLOG(INFO, CoreExecution) << "Node #" << nodeId << "<" << node->Content() << "> created " << ids.size()
                                      << " trackable nodes: " << JoinSeq(", ", ids);
    }

    TStatus CollectUnusedNodes(const TExprNode& root, TExprContext& ctx) {
        if (TrackableNodes.empty()) {
            return TStatus::Ok;
        }

        YQL_CLOG(TRACE, CoreExecution) << "Collecting unused nodes on root #" << root.UniqueId();

        THashSet<ui64> visited;
        THashSet<TString> usedIds;
        VisitExpr(root, [&](const TExprNode& node) {
            if (node.GetState() == TExprNode::EState::ExecutionComplete) {
                return false;
            }

            auto nodeId = node.UniqueId();
            visited.insert(nodeId);

            TIntrusivePtr<IDataProvider> dataProvider;
            auto providerIt = ProvidersCache.find(nodeId);
            if (providerIt != ProvidersCache.end()) {
                YQL_ENSURE(providerIt->second);
                dataProvider = providerIt->second;
            } else {
                dataProvider = GetDataProvider(node);
                if (dataProvider) {
                    ProvidersCache[nodeId] = dataProvider;
                }
            }

            if (dataProvider) {
                TVector<TString> usedNodes;
                dataProvider->GetTrackableNodeProcessor().GetUsedNodes(node, usedNodes);
                usedIds.insert(usedNodes.begin(), usedNodes.end());
            }

            return true;
        });

        for (auto i = ProvidersCache.begin(); i != ProvidersCache.end();) {
            if (visited.count(i->first) == 0) {
                ProvidersCache.erase(i++);
            } else {
                ++i;
            }
        }

        THashMap<TIntrusivePtr<IDataProvider>, TExprNode::TListType> toCollect;
        for (auto i = TrackableNodes.begin(); i != TrackableNodes.end();) {
            TString id = i->first;
            TTrackableNodeInfo info = i->second;
            if (!usedIds.contains(id)) {
                YQL_ENSURE(info.Node);
                YQL_ENSURE(info.Provider);
                YQL_CLOG(INFO, CoreExecution) << "Marking node " << id << " for collection";
                toCollect[info.Provider].push_back(info.Node);
                TrackableNodes.erase(i++);
            } else {
                ++i;
            }
        }

        TStatus collectStatus = TStatus::Ok;
        for (auto& i : toCollect) {
            const auto& provider = i.first;
            YQL_ENSURE(!i.second.empty());
            auto pos = i.second.front()->Pos();
            TExprNode::TPtr collectNode = ctx.NewList(pos, std::move(i.second));
            TExprNode::TPtr output;
            TStatus status = provider->GetTrackableNodeProcessor().GetCleanupTransformer().Transform(collectNode, output, ctx);
            YQL_ENSURE(status != TStatus::Repeat);

            collectStatus = collectStatus.Combine(status);
            if (status == TStatus::Error) {
                break;
            }

            if (status == TStatus::Async) {
                CollectingNodes[collectNode.Get()] = collectNode;
                auto future = provider->GetTrackableNodeProcessor().GetCleanupTransformer().GetAsyncFuture(*collectNode);
                SubscribeAsyncFuture(collectNode, provider.Get(), future);
            }
        }

        return collectStatus;
    }

private:
    TTypeAnnotationContext& Types;
    TOperationProgressWriter Writer;
    const bool WithFinalize;
    TStatePtr State;
    TNodeOnNodeOwnedMap NewNodes;
    TAutoPtr<IGraphTransformer> FinalizingTransformer;
    THashMap<ui32, TOperationProgress> Progresses;

    struct TTrackableNodeInfo
    {
        TIntrusivePtr<IDataProvider> Provider;
        TExprNode::TPtr Node;
    };

    THashMap<TString, TTrackableNodeInfo> TrackableNodes;
    TNodeOnNodeOwnedMap CollectingNodes;
    THashMap<ui64, TIntrusivePtr<IDataProvider>> ProvidersCache;
    TExprNode::TListType FreshPendingNodes;

    bool DeterministicMode;
    TNodeOnNodeOwnedMap AsyncNodes;
};

IGraphTransformer::TStatus ValidateExecution(const TExprNode::TPtr& node, TExprContext& ctx, const TTypeAnnotationContext& types, TNodeSet& visited);

IGraphTransformer::TStatus ValidateList(const TExprNode::TPtr& node, TExprContext& ctx, const TTypeAnnotationContext& types, TNodeSet& visited) {
    IGraphTransformer::TStatus combinedStatus = IGraphTransformer::TStatus::Ok;
    for (ui32 i = 0; i < node->ChildrenSize(); ++i) {
        combinedStatus = combinedStatus.Combine(ValidateExecution(node->ChildPtr(i), ctx, types, visited));
    }

    return combinedStatus;
}

IGraphTransformer::TStatus ValidateCallable(const TExprNode::TPtr& node, TExprContext& ctx, const TTypeAnnotationContext& types, TNodeSet& visited) {
    using TStatus = IGraphTransformer::TStatus;

    if (node->Content() == CommitName) {
        auto datasink = types.DataSinkMap.FindPtr(node->Child(1)->Child(0)->Content());
        if (!datasink) {
            ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), TStringBuilder() << "Unknown datasink: " << node->Child(1)->Child(0)->Content()));
            return TStatus::Error;
        }

        return ValidateExecution(node->ChildPtr(0), ctx, types, visited);
    }

    if (node->Content() == SyncName) {
        return ValidateList(node, ctx, types, visited);
    }

    if (node->Content() == LeftName) {
        return ValidateExecution(node->ChildPtr(0), ctx, types, visited);
    }

    if (node->Content() == RightName) {
        return ValidateExecution(node->ChildPtr(0), ctx, types, visited);
    }

    IDataProvider* dataProvider = nullptr;
    for (auto& x : types.DataSources) {
        if (x->CanExecute(*node)) {
            dataProvider = x.Get();
            break;
        }
    }

    if (!dataProvider) {
        for (auto& x : types.DataSinks) {
            if (x->CanExecute(*node)) {
                dataProvider = x.Get();
                break;
            }
        }
    }

    if (!dataProvider) {
        ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), TStringBuilder() << "Failed to execute callable with name: " << node->Content()
            << ", you possibly used cross provider/cluster operations or pulled not materialized result in refselect mode"));
        return TStatus::Error;
    }

    if (node->ChildrenSize() < 2) {
        ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), TStringBuilder() << "Executable callable should have at least 2 children"));
        return TStatus::Error;
    }

    if (!dataProvider->ValidateExecution(*node, ctx)) {
        return TStatus::Error;
    }

    TExprNode::TListType childrenToCheck;
    dataProvider->GetRequiredChildren(*node, childrenToCheck);
    IGraphTransformer::TStatus combinedStatus = IGraphTransformer::TStatus::Ok;
    for (ui32 i = 0; i < childrenToCheck.size(); ++i) {
        combinedStatus = combinedStatus.Combine(ValidateExecution(childrenToCheck[i], ctx, types, visited));
    }

    return combinedStatus;
}

IGraphTransformer::TStatus ValidateExecution(const TExprNode::TPtr& node, TExprContext& ctx,
    const TTypeAnnotationContext& types, TNodeSet& visited) {
    using TStatus = IGraphTransformer::TStatus;
    if (node->GetState() == TExprNode::EState::ExecutionComplete) {
        return TStatus::Ok;
    }

    if (!node->GetTypeAnn()) {
        ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), "Node has no type annotation"));
        return TStatus::Error;
    }

    TStatus status = TStatus::Ok;
    switch (node->Type()) {
    case TExprNode::Atom:
    case TExprNode::Argument:
    case TExprNode::Arguments:
    case TExprNode::Lambda:
        ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), TStringBuilder() << "Failed to execute node with type: " << node->Type()));
        return TStatus::Error;

    case TExprNode::List:
        return ValidateList(node, ctx, types, visited);

    case TExprNode::Callable:
        if (visited.cend() != visited.find(node.Get())) {
            return TStatus::Ok;
        }

        status = ValidateCallable(node, ctx, types, visited);
        if (status.Level == TStatus::Ok) {
            visited.insert(node.Get());
        }

        break;

    case TExprNode::World:
        break;

    default:
        YQL_ENSURE(false, "Unknown type");
    }

    return status;
}

}

TAutoPtr<IGraphTransformer> CreateExecutionTransformer(
    TTypeAnnotationContext& types,
    TOperationProgressWriter writer,
    bool withFinalize) {
    return new TExecutionTransformer(types, writer, withFinalize);
}

TAutoPtr<IGraphTransformer> CreateCheckExecutionTransformer(const TTypeAnnotationContext& types, bool checkWorld) {
    return CreateFunctorTransformer([&types, checkWorld](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx)
        -> IGraphTransformer::TStatus {
        output = input;
        if (checkWorld) {
            TNodeSet visited;
            auto status = ValidateExecution(input, ctx, types, visited);
            if (status.Level != IGraphTransformer::TStatus::Ok) {
                return status;
            }
        }

        TParentsMap parentsMap;
        THashSet<TExprNode*> overWinNodes;
        GatherParents(*input, parentsMap);
        bool hasErrors = false;
        THashSet<TIssue> added;
        auto funcCheckExecution = [&](const THashSet<TStringBuf>& notAllowList, bool collectCalcOverWindow, const TExprNode::TPtr& node) {
            if (node->IsCallable("ErrorType")) {
                hasErrors = true;
                const auto err = node->GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TErrorExprType>()->GetError();
                if (added.insert(err).second) {
                    ctx.AddError(err);
                }
            } else if (node->IsCallable("TablePath") || node->IsCallable("TableRecord")) {
                auto issue = TIssue(ctx.GetPosition(node->Pos()), TStringBuilder() << node->Content() << " will be empty");
                SetIssueCode(EYqlIssueCode::TIssuesIds_EIssueCode_CORE_FREE_TABLE_PATH_RECORD, issue);
                if (!ctx.AddWarning(issue)) {
                    hasErrors = true;
                }
            } else if (node->IsCallable("UnsafeTimestampCast")) {
                auto issue = TIssue(ctx.GetPosition(node->Pos()), "Unsafe conversion integral value to Timestamp, consider using date types");
                SetIssueCode(EYqlIssueCode::TIssuesIds_EIssueCode_CORE_CAST_INTEGRAL_TO_TIMESTAMP_UNSAFE, issue);
                if (!ctx.AddWarning(issue)) {
                    hasErrors = true;
                }
            } else if (collectCalcOverWindow && node->IsCallable({"CalcOverWindow", "CalcOverSessionWindow", "CalcOverWindowGroup"})) {
                overWinNodes.emplace(node.Get());
                return false;
            } else if (node->IsCallable(notAllowList)) {
                hasErrors = true;
                const auto err = TIssue(ctx.GetPosition(node->Pos()), TStringBuilder() << "Can't execute " << node->Content());
                if (added.insert(err).second) {
                    ctx.AddError(err);
                }
            } else if (node->Type() != TExprNode::Lambda &&
                (node->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Stream || node->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Flow)) {
                auto parentsIt = parentsMap.find(node.Get());
                if (parentsIt != parentsMap.end()) {
                    ui32 usageCount = 0;
                    for (auto& x : parentsIt->second) {
                        if (x->IsCallable("DependsOn")) {
                            continue;
                        }

                        for (auto& y : x->Children()) {
                            if (y.Get() == node.Get()) {
                                ++usageCount;
                            }
                        }
                    }

                    if (usageCount > 1 && !node->GetConstraint<TEmptyConstraintNode>()) {
                        hasErrors = true;
                        const auto err = TIssue(ctx.GetPosition(node->Pos()), "Multiple stream clients");
                        if (added.insert(err).second) {
                            ctx.AddError(err);
                        }
                    }
                }
            }

            return true;
        };
        static const THashSet<TStringBuf> noExecutionList = {"InstanceOf", "Lag", "Lead", "RowNumber", "Rank", "DenseRank", "PercentRank", "CumeDist", "NTile"};
        static const THashSet<TStringBuf> noExecutionListForCalcOverWindow = {"InstanceOf"};
        VisitExpr(input, [funcCheckExecution](const TExprNode::TPtr& node) {
            bool collectCalcOverWindow = true;
            return funcCheckExecution(noExecutionList, collectCalcOverWindow, node);
        });
        for (auto overWin: overWinNodes) {
            VisitExpr(overWin, [funcCheckExecution](const TExprNode::TPtr& node) {
                bool collectCalcOverWindow = false;
                return funcCheckExecution(noExecutionListForCalcOverWindow, collectCalcOverWindow, node);
            });
        }

        return hasErrors ? IGraphTransformer::TStatus::Error : IGraphTransformer::TStatus::Ok;
    });
};

IGraphTransformer::TStatus RequireChild(const TExprNode& node, ui32 index) {
    switch (node.Child(index)->GetState()) {
    case TExprNode::EState::Error:
    case TExprNode::EState::ExecutionComplete:
        return IGraphTransformer::TStatus::Ok;
    case TExprNode::EState::ExecutionInProgress:
    case TExprNode::EState::ExecutionPending:
        return IGraphTransformer::TStatus::Repeat;
    default:
        break;
    }

    node.Child(index)->SetState(TExprNode::EState::ExecutionRequired);
    return IGraphTransformer::TStatus::Repeat;
}

}

template<>
void Out<NYql::TOperationProgress::EState>(class IOutputStream &o, NYql::TOperationProgress::EState x) {
#define YQL_OPERATION_PROGRESS_STATE_MAP_TO_STRING_IMPL(name, ...) \
    case NYql::TOperationProgress::EState::name: \
        o << #name; \
        return;

    switch (x) {
        YQL_OPERATION_PROGRESS_STATE_MAP(YQL_OPERATION_PROGRESS_STATE_MAP_TO_STRING_IMPL)
    default:
        o << static_cast<int>(x);
        return;
    }
}
