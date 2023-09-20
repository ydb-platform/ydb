#include "yql_graph_transformer.h"
#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/library/yql/public/issue/yql_issue_manager.h>

namespace NYql {

namespace {

class TCompositeGraphTransformer : public TGraphTransformerBase {
public:
    TCompositeGraphTransformer(const TVector<TTransformStage>& stages, bool useIssueScopes, bool doCheckArguments)
        : Stages_(stages)
        , UseIssueScopes_(useIssueScopes)
        , DoCheckArguments_(doCheckArguments)
    {
        if (UseIssueScopes_) {
            for (const auto& stage : Stages_) {
                YQL_ENSURE(!stage.Name.empty());
            }
        }
    }

    void Rewind() override {
        for (auto& stage : Stages_) {
            stage.GetTransformer().Rewind();
        }

        Index_ = 0;
        CheckArgumentsCount_ = 0;
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

        if (Index_ >= Stages_.size()) {
            return TStatus::Ok;
        }

        auto status = WithScope(ctx, [&]() {
            return Stages_[Index_].GetTransformer().Transform(input, output, ctx);
        });
#ifndef NDEBUG
        if (DoCheckArguments_ && output && output != input) {
            try {
                CheckArguments(*output);
                ++CheckArgumentsCount_;
            } catch (yexception& e) {
                e << "at CheckArguments() pass #" << CheckArgumentsCount_
                  << ", stage '" << Stages_[Index_].Name << "'";
                throw;
            }
        }
#else
        Y_UNUSED(DoCheckArguments_);
        Y_UNUSED(CheckArgumentsCount_);
#endif
        status = HandleStatus(status);
        return status;
    }

    NThreading::TFuture<void> DoGetAsyncFuture(const TExprNode& input) override {
        YQL_ENSURE(Index_ < Stages_.size());
        return Stages_[Index_].GetTransformer().GetAsyncFuture(input);
    }

    TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) override {
        YQL_ENSURE(Index_ < Stages_.size());
        auto status = WithScope(ctx, [&]() {
            return Stages_[Index_].GetTransformer().ApplyAsyncChanges(input, output, ctx);
        });

        status = HandleStatus(status);
        return status;
    }

    TStatistics GetStatistics() const final {
        if (Statistics_.Stages.empty()) {
            Statistics_.Stages.resize(Stages_.size());
        }

        YQL_ENSURE(Stages_.size() == Statistics_.Stages.size());
        for (size_t i = 0; i < Stages_.size(); ++i) {
            auto& stagePair = Statistics_.Stages[i];
            stagePair.first = Stages_[i].Name;
            stagePair.second =  Stages_[i].GetTransformer().GetStatistics();
        }

        return Statistics_;
    }

private:
    virtual TStatus HandleStatus(TStatus status) {
        if (status.Level == IGraphTransformer::TStatus::Error) {
            return status;
        }

        if (status.HasRestart) {
            // ignore Async status in this case
            Index_ = 0;
            status = IGraphTransformer::TStatus(IGraphTransformer::TStatus::Repeat, true);
        } else if (status.Level == IGraphTransformer::TStatus::Ok) {
            status = IGraphTransformer::TStatus::Repeat;
            ++Index_;
        }

        return status;
    }

    template <typename TFunc>
    TStatus WithScope(TExprContext& ctx, TFunc func) {
        if (UseIssueScopes_) {
            TIssueScopeGuard guard(ctx.IssueManager, [&]() {
                const auto scopeIssueCode = Stages_[Index_].IssueCode;
                const auto scopeIssueMessage = Stages_[Index_].IssueMessage;

                auto issue = MakeIntrusive<TIssue>(TPosition(), scopeIssueMessage ? scopeIssueMessage : IssueCodeToString(scopeIssueCode));
                issue->SetCode(scopeIssueCode, GetSeverity(scopeIssueCode));
                return issue;
            });

            return func();
        } else {
            return func();
        }
    }

protected:
    TVector<TTransformStage> Stages_;
    const bool UseIssueScopes_;
    const bool DoCheckArguments_;
    size_t Index_ = 0;
    ui64 CheckArgumentsCount_ = 0;
};

void AddTooManyTransformationsError(TPositionHandle pos, const TStringBuf& where, TExprContext& ctx) {
    ctx.AddError(TIssue(ctx.GetPosition(pos),
                        TStringBuilder() << "YQL: Internal core error! " << where << " takes too much iterations: "
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
        Condition_.Clear();
        TCompositeGraphTransformer::Rewind();
    }

    TStatus HandleStatus(TStatus status) override {
        if (status.Level == IGraphTransformer::TStatus::Error) {
            return status;
        }

        if (status.HasRestart) {
            // ignore Async status in this case
            Index_ = 0;
            status = IGraphTransformer::TStatus(IGraphTransformer::TStatus::Repeat, true);
        } else if (status.Level == IGraphTransformer::TStatus::Ok) {
            status = IGraphTransformer::TStatus::Repeat;
            YQL_ENSURE(!Condition_.Empty(), "Condition must be set");
            if (Index_ == 0 && *Condition_) {
                Index_ = 1; // left
            } else if (Index_ == 0) {
                Index_ = 2; // right
            } else {
                Index_ = 3; // end
            }
        }

        return status;
    }

    TTransformStage WrapCondition(const std::function<bool(const TExprNode::TPtr& input, TExprContext& ctx)>& condition)
    {
        auto transformer = CreateFunctorTransformer([this, condition](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
            output = input;
            if (Condition_.Empty()) {
                Condition_ = condition(input, ctx);
            }
            return TStatus::Ok;
        });

        return TTransformStage(transformer, "Condition", TIssuesIds::DEFAULT_ERROR);
    }

    TMaybe<bool> Condition_;
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

IGraphTransformer::TStatus AsyncTransformStepImpl(IGraphTransformer& transformer, TExprNode::TPtr& root,
                                            TExprContext& ctx, bool applyAsyncChanges, bool breakOnRestart,
                                            const TStringBuf& name)
{
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
                if (breakOnRestart && status.HasRestart) {
                    return status;
                }
                return AsyncTransformStepImpl(transformer, root, ctx, false /* no async changes */, breakOnRestart, name);
            case IGraphTransformer::TStatus::Async:
                YQL_ENSURE(false, "Async status is forbidden for ApplyAsyncChanges");
                break;
            default:
                YQL_ENSURE(false, "Unknown status");
                break;
            }
            return status;
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
                return status;
            case IGraphTransformer::TStatus::Repeat:
                if (breakOnRestart && status.HasRestart) {
                    return status;
                }
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
            AddTooManyTransformationsError(root->Pos(), name, ctx);
            return IGraphTransformer::TStatus::Error;
        }
    }
    catch (const std::exception& e) {
        ctx.AddError(ExceptionToIssue(e));
        return IGraphTransformer::TStatus::Error;
    }

    return IGraphTransformer::TStatus::Async;
}

IGraphTransformer::TStatus InstantTransform(IGraphTransformer& transformer, TExprNode::TPtr& root, TExprContext& ctx, bool breakOnRestart) {
    IGraphTransformer::TStatus status = AsyncTransformStepImpl(transformer, root, ctx, false, breakOnRestart, "InstantTransform");
    if (status.Level == IGraphTransformer::TStatus::Async) {
        ctx.AddError(TIssue(ctx.GetPosition(root->Pos()), "Instant transform can not be delayed"));
        return IGraphTransformer::TStatus::Error;
    }
    return status;
}

IGraphTransformer::TStatus AsyncTransformStep(IGraphTransformer& transformer, TExprNode::TPtr& root,
                                            TExprContext& ctx, bool applyAsyncChanges)
{
    return AsyncTransformStepImpl(transformer, root, ctx, applyAsyncChanges, false, "AsyncTransformStep");
}

NThreading::TFuture<IGraphTransformer::TStatus> AsyncTransform(IGraphTransformer& transformer, TExprNode::TPtr& root, TExprContext& ctx,
                                                                bool applyAsyncChanges) {
    IGraphTransformer::TStatus status = AsyncTransformStepImpl(transformer, root, ctx, applyAsyncChanges, false, "AsyncTransform");
    if (status.Level != IGraphTransformer::TStatus::Async) {
        return NThreading::MakeFuture(status);
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
