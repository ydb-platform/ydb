#include "dq_function_provider.h"

#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/public/issue/yql_issue.h>

#include <library/cpp/threading/future/async.h>

namespace NYql::NDqFunction {
namespace {

using namespace NNodes;

class TDqFunctionResolverTransform : public TGraphTransformerBase {
private:
    struct TResolverContext : public TThrRefBase {
        using TPtr = TIntrusivePtr<TResolverContext>;
        TVector<TIssue> ResolveIssues;
        TDqFunctionsSet FunctionsDescription;
    };
public:
    TDqFunctionResolverTransform(TDqFunctionState::TPtr state)
        : State(state)
    {}

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        output = input;

        if (ctx.Step.IsDone(TExprStep::LoadTablesMetadata)) {
            return TStatus::Ok;
        }

        ResolverContext = MakeIntrusive<TResolverContext>();

        auto functions = State->FunctionsResolver->FunctionsToResolve();
        std::vector<NThreading::TFuture<void>> resolverHandles;
        resolverHandles.reserve(functions.size());
        auto resolverContext = ResolverContext;
        const auto position = ctx.GetPosition(input->Pos());
        for (auto functionDesc : functions) {
            auto gateway = State->GatewayFactory->CreateDqFunctionGateway(
                    functionDesc.Type, State->SecureParams, functionDesc.Connection);

            auto future = gateway->ResolveFunction(State->ScopeFolderId, functionDesc.FunctionName);
            resolverHandles.push_back(future.Apply([resolverContext, position]
                (const NThreading::TFuture<TDqFunctionDescription>& future) {
                    try {
                        resolverContext->FunctionsDescription.emplace(future.GetValue());
                    } catch (const std::exception& e) {
                        resolverContext->ResolveIssues.push_back(ExceptionToIssue(e, position));
                    }
                }));
        }

        if (resolverHandles.empty()) {
            return TStatus::Ok;
        }

        AllFutures = NThreading::WaitExceptionOrAll(resolverHandles);
        return TStatus::Async;
    }

    NThreading::TFuture<void> DoGetAsyncFuture(const TExprNode& input) final {
        Y_UNUSED(input);
        return AllFutures;
    }

    TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {

        Y_UNUSED(ctx);
        YQL_ENSURE(AllFutures.HasValue());
        output = input;

        if (!ResolverContext->ResolveIssues.empty()) {
            ctx.IssueManager.RaiseIssues(TIssues(ResolverContext->ResolveIssues));
            ResolverContext.Reset();
            return TStatus::Error;
        }

        for (auto function : ResolverContext->FunctionsDescription) {
            State->FunctionsDescription.emplace(std::move(function));
        }
        ResolverContext.Reset();
        return TStatus::Ok;
    }

    void Rewind() final {
        ResolverContext.Reset();
        AllFutures = {};
    }

private:
    TDqFunctionState::TPtr State;
    NThreading::TFuture<void> AllFutures;
    TResolverContext::TPtr ResolverContext;
};
}

THolder<IGraphTransformer> CreateDqFunctionMetaLoader(TDqFunctionState::TPtr state) {
    return THolder(new TDqFunctionResolverTransform(state));
}

}
