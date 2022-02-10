#include "yql_graph_transformer.h"
#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/library/yql/public/issue/yql_issue_manager.h>

namespace NYql {

namespace {

class TCompositeGraphTransformer : public TGraphTransformerBase {
public:
    TCompositeGraphTransformer(const TVector<TTransformStage>& stages, bool useIssueScopes, bool doCheckArguments)
        : Stages(stages)
        , UseIssueScopes(useIssueScopes)
        , DoCheckArguments(doCheckArguments)
    {
        if (UseIssueScopes) {
            for (const auto& stage : Stages) {
                YQL_ENSURE(!stage.Name.empty());
            }
        }
    }

    void Rewind() override {
        for (auto& stage : Stages) {
            stage.GetTransformer().Rewind();
        }

        Index = 0;
        LastIssueScope = Nothing();
        CheckArgumentsCount = 0;
    }

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) override {
//#define TRACE_NODES
#ifdef TRACE_NODES
        static ui64 TransformsCount = 0;
        ++TransformsCount;
        if ((TransformsCount % 100) == 0) {
            Cout << "\r#transforms: " << TransformsCount << ", #nodes: " << ctx.NextUniqueId;
        }
#endif

        if (Index >= Stages.size()) {
            if (LastIssueScope) {
                ctx.IssueManager.LeaveScope();
                LastIssueScope.Clear();
            }
            return TStatus::Ok;
        }

        if (UseIssueScopes) {
            UpdateIssueScope(ctx.IssueManager);
        }
        auto status = Stages[Index].GetTransformer().Transform(input, output, ctx);
#ifndef NDEBUG
        if (DoCheckArguments && output && output != input) {
            try {
                CheckArguments(*output);
                ++CheckArgumentsCount;
            } catch (yexception& e) {
                e << "at CheckArguments() pass #" << CheckArgumentsCount
                  << ", stage '" << Stages[Index].Name << "'";
                throw;
            }
        }
#else
        Y_UNUSED(DoCheckArguments);
        Y_UNUSED(CheckArgumentsCount);
#endif
        status = HandleStatus(status);
        return status;
    }

    NThreading::TFuture<void> DoGetAsyncFuture(const TExprNode& input) override {
        YQL_ENSURE(Index < Stages.size());
        return Stages[Index].GetTransformer().GetAsyncFuture(input);
    }

    TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) override {
        YQL_ENSURE(Index < Stages.size());
        const auto status = Stages[Index].GetTransformer().ApplyAsyncChanges(input, output, ctx);
        return HandleStatus(status);
    }

    TStatistics GetStatistics() const final {
        if (Statistics.Stages.empty()) {
            Statistics.Stages.resize(Stages.size());
        }

        YQL_ENSURE(Stages.size() == Statistics.Stages.size());
        for (size_t i = 0; i < Stages.size(); ++i) {
            auto& stagePair = Statistics.Stages[i];
            stagePair.first = Stages[i].Name;
            stagePair.second =  Stages[i].GetTransformer().GetStatistics();
        }

        return Statistics;
    }

private:
    virtual TStatus HandleStatus(TStatus status) { 
        if (status.Level == IGraphTransformer::TStatus::Error) {
            return status;
        }

        if (status.HasRestart) {
            // ignore Async status in this case
            Index = 0;
            status = IGraphTransformer::TStatus(IGraphTransformer::TStatus::Repeat, true);
        } else if (status.Level == IGraphTransformer::TStatus::Ok) {
            status = IGraphTransformer::TStatus::Repeat;
            ++Index;
        }

        return status;
    }

    void UpdateIssueScope(TIssueManager& issueManager) {
        YQL_ENSURE(Index < Stages.size());
        const auto scopeIssueCode = Stages[Index].IssueCode;
        const auto scopeIssueMessage = Stages[Index].IssueMessage;
        if (LastIssueScope != scopeIssueCode) {
            if (!LastIssueScope.Empty()) {
                issueManager.LeaveScope();
            }
            issueManager.AddScope([scopeIssueCode, scopeIssueMessage]() {
                auto issue = new TIssue(TPosition(), scopeIssueMessage ? scopeIssueMessage : IssueCodeToString(scopeIssueCode));
                issue->SetCode(scopeIssueCode, GetSeverity(scopeIssueCode));
                return issue;
            });
            LastIssueScope = scopeIssueCode;
        }
    }

protected: 
    TVector<TTransformStage> Stages;
    const bool UseIssueScopes;
    const bool DoCheckArguments;
    size_t Index = 0;
    TMaybe<EYqlIssueCode> LastIssueScope;
    ui64 CheckArgumentsCount = 0;
};

void AddTooManyTransformationsError(TPositionHandle pos, const TStringBuf& where, TExprContext& ctx) {
    ctx.AddError(TIssue(ctx.GetPosition(pos),
                        TStringBuilder() << "YQL: Internal core error! " << where << " take too much iteration: "
                                         << ctx.RepeatTransformLimit
                                         << ". You may set RepeatTransformLimit as flags for config provider."));
}

}

TAutoPtr<IGraphTransformer> CreateCompositeGraphTransformer(const TVector<TTransformStage>& stages, bool useIssueScopes) {
    return new TCompositeGraphTransformer(stages, useIssueScopes, /* doCheckArguments = */ true);
}

TAutoPtr<IGraphTransformer> CreateCompositeGraphTransformerWithNoArgChecks(const TVector<TTransformStage>& stages, bool useIssueScopes) {
    return new TCompositeGraphTransformer(stages, useIssueScopes, /* doCheckArguments = */ false);
}

namespace { 
 
class TChoiceGraphTransformer : public TCompositeGraphTransformer { 
public: 
    TChoiceGraphTransformer( 
        const std::function<bool(const TExprNode::TPtr& input, TExprContext& ctx)>& condition, 
        const TTransformStage& left, 
        const TTransformStage& right) 
        : TCompositeGraphTransformer( 
            {WrapCondition(condition), left, right}, 
            /* useIssueScopes = */ false, 
            /* doCheckArgumentstrue = */ true) 
    { } 
 
private: 
    void Rewind() override { 
        Condition.Clear(); 
        TCompositeGraphTransformer::Rewind(); 
    } 
 
    TStatus HandleStatus(TStatus status) override { 
        if (status.Level == IGraphTransformer::TStatus::Error) { 
            return status; 
        } 
 
        if (status.HasRestart) { 
            // ignore Async status in this case 
            Index = 0; 
            status = IGraphTransformer::TStatus(IGraphTransformer::TStatus::Repeat, true); 
        } else if (status.Level == IGraphTransformer::TStatus::Ok) { 
            status = IGraphTransformer::TStatus::Repeat; 
            YQL_ENSURE(!Condition.Empty(), "Condition must be set"); 
            if (Index == 0 && *Condition) { 
                Index = 1; // left 
            } else if (Index == 0) { 
                Index = 2; // right 
            } else { 
                Index = 3; // end 
            } 
        } 
 
        return status; 
    } 
 
    TTransformStage WrapCondition(const std::function<bool(const TExprNode::TPtr& input, TExprContext& ctx)>& condition) 
    { 
        auto transformer = CreateFunctorTransformer([this, condition](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) { 
            output = input; 
            if (Condition.Empty()) { 
                Condition = condition(input, ctx); 
            } 
            return TStatus::Ok; 
        }); 
 
        return TTransformStage(transformer, "Condition", TIssuesIds::DEFAULT_ERROR); 
    } 
 
    TMaybe<bool> Condition; 
}; 
 
} // namespace 
 
TAutoPtr<IGraphTransformer> CreateChoiceGraphTransformer( 
    const std::function<bool(const TExprNode::TPtr& input, TExprContext& ctx)>& condition, 
    const TTransformStage& left, const TTransformStage& right) 
{ 
    return new TChoiceGraphTransformer(condition, left, right); 
} 
 
IGraphTransformer::TStatus SyncTransform(IGraphTransformer& transformer, TExprNode::TPtr& root, TExprContext& ctx) {
    try {
        for (; ctx.RepeatTransformCounter < ctx.RepeatTransformLimit; ++ctx.RepeatTransformCounter) {
            TExprNode::TPtr newRoot;
            auto status = transformer.Transform(root, newRoot, ctx);
            if (newRoot) {
                root = newRoot;
            }

            switch (status.Level) {
            case IGraphTransformer::TStatus::Ok:
            case IGraphTransformer::TStatus::Error:
                return status;
            case IGraphTransformer::TStatus::Repeat:
                continue;
            case IGraphTransformer::TStatus::Async:
                break;
            default:
                YQL_ENSURE(false, "Unknown status");
            }

            auto future = transformer.GetAsyncFuture(*root);
            future.Wait();
            YQL_ENSURE(!future.HasException());

            status = transformer.ApplyAsyncChanges(root, newRoot, ctx);
            if (newRoot) {
                root = newRoot;
            }

            switch (status.Level) {
            case IGraphTransformer::TStatus::Ok:
            case IGraphTransformer::TStatus::Error:
                return status;
            case IGraphTransformer::TStatus::Repeat:
                break;
            case IGraphTransformer::TStatus::Async:
                YQL_ENSURE(false, "Async status is forbidden for ApplyAsyncChanges");
                break;
            default:
                YQL_ENSURE(false, "Unknown status");
            }
        }
        AddTooManyTransformationsError(root->Pos(), "SyncTransform", ctx);
    }
    catch (const std::exception& e) {
        ctx.AddError(ExceptionToIssue(e));
    }
    return IGraphTransformer::TStatus::Error;
}

IGraphTransformer::TStatus InstantTransform(IGraphTransformer& transformer, TExprNode::TPtr& root, TExprContext& ctx, bool breakOnRestart) {
    try {
        for (; ctx.RepeatTransformCounter < ctx.RepeatTransformLimit; ++ctx.RepeatTransformCounter) {
            TExprNode::TPtr newRoot;
            auto status = transformer.Transform(root, newRoot, ctx);
            if (newRoot) {
                root = newRoot;
            }

            switch (status.Level) {
            case IGraphTransformer::TStatus::Ok:
            case IGraphTransformer::TStatus::Error:
                return status;
            case IGraphTransformer::TStatus::Repeat:
                if (breakOnRestart && status.HasRestart) {
                    return status;
                }

                continue;
            case IGraphTransformer::TStatus::Async:
                ctx.AddError(TIssue(ctx.GetPosition(root->Pos()), "Instant transform can not be delayed"));
                return IGraphTransformer::TStatus::Error;
            default:
                YQL_ENSURE(false, "Unknown status");
            }
        }
        AddTooManyTransformationsError(root->Pos(), "InstantTransform", ctx);
    }
    catch (const std::exception& e) {
        ctx.AddError(ExceptionToIssue(e));
    }
    return IGraphTransformer::TStatus::Error;
}

NThreading::TFuture<IGraphTransformer::TStatus> AsyncTransform(IGraphTransformer& transformer, TExprNode::TPtr& root, TExprContext& ctx,
                                                                bool applyAsyncChanges) {
    try {
        if (applyAsyncChanges) {
            TExprNode::TPtr newRoot;
            auto status = transformer.ApplyAsyncChanges(root, newRoot, ctx);
            if (newRoot) {
                root = newRoot;
            }

            switch (status.Level) {
            case IGraphTransformer::TStatus::Ok:
            case IGraphTransformer::TStatus::Error:
                break;
            case IGraphTransformer::TStatus::Repeat:
                return AsyncTransform(transformer, root, ctx, false /* no async changes */);
            case IGraphTransformer::TStatus::Async:
                YQL_ENSURE(false, "Async status is forbidden for ApplyAsyncChanges");
                break;
            default:
                YQL_ENSURE(false, "Unknown status");
                break;
            }
            return NThreading::MakeFuture(status);
        }
        for (; ctx.RepeatTransformCounter < ctx.RepeatTransformLimit; ++ctx.RepeatTransformCounter) {
            TExprNode::TPtr newRoot;
            auto status = transformer.Transform(root, newRoot, ctx);
            if (newRoot) {
                root = newRoot;
            }

            switch (status.Level) {
            case IGraphTransformer::TStatus::Ok:
            case IGraphTransformer::TStatus::Error:
                return NThreading::MakeFuture(status);
            case IGraphTransformer::TStatus::Repeat:
                // if (currentTime - startTime >= threshold) return NThreading::MakeFuture(IGraphTransformer::TStatus::Yield);
                continue;
            case IGraphTransformer::TStatus::Async:
                break;
            default:
                YQL_ENSURE(false, "Unknown status");
            }
            break;
        }
        if (ctx.RepeatTransformCounter >= ctx.RepeatTransformLimit) {
            AddTooManyTransformationsError(root->Pos(), "AsyncTransform", ctx);
            return NThreading::MakeFuture(IGraphTransformer::TStatus(IGraphTransformer::TStatus::Error));
        }
    }
    catch (const std::exception& e) {
        ctx.AddError(ExceptionToIssue(e));
        return NThreading::MakeFuture(IGraphTransformer::TStatus(IGraphTransformer::TStatus::Error));
    }

    return transformer.GetAsyncFuture(*root).Apply(
        [] (const NThreading::TFuture<void>&) mutable -> NThreading::TFuture<IGraphTransformer::TStatus> {
            return NThreading::MakeFuture(IGraphTransformer::TStatus(IGraphTransformer::TStatus::Async));
        });

}

void AsyncTransform(IGraphTransformer& transformer, TExprNode::TPtr& root, TExprContext& ctx, bool applyAsyncChanges,
                    std::function<void(const IGraphTransformer::TStatus&)> asyncCallback) {
    NThreading::TFuture<IGraphTransformer::TStatus> status = AsyncTransform(transformer, root, ctx, applyAsyncChanges);
    status.Subscribe(
       [asyncCallback](const NThreading::TFuture<IGraphTransformer::TStatus>& status) mutable -> void {
           YQL_ENSURE(!status.HasException());
           asyncCallback(status.GetValue());
       });
}

}

template<>
void Out<NYql::IGraphTransformer::TStatus::ELevel>(class IOutputStream &o, NYql::IGraphTransformer::TStatus::ELevel x) {
#define YQL_GT_STATUS_MAP_TO_STRING_IMPL(name, ...) \
    case NYql::IGraphTransformer::TStatus::name: \
        o << #name; \
        return;

    switch (x) {
        YQL_GT_STATUS_MAP(YQL_GT_STATUS_MAP_TO_STRING_IMPL)
    default:
        o << static_cast<int>(x);
        return;
    }
}
