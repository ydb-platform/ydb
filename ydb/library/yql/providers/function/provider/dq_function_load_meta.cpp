#include "dq_function_provider.h"

#include <library/cpp/threading/future/async.h>
#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/public/issue/yql_issue.h>

#include <memory>
#include <optional>

namespace NYql::NDqFunction {
namespace {

using namespace NNodes;

class TDqFunctionResolverTransform : public TGraphTransformerBase {
private:
    struct TResolverContext : public TThrRefBase {
        using TPtr = TIntrusivePtr<TResolverContext>;

        struct TResult {
            std::optional<TDqFunctionDescription> Description;
            std::optional<TIssue> Issue;
        };

        // Each completion owns a separate slot. Shared containers are updated
        // only on the transformer thread, after all callbacks have completed.
        TVector<std::shared_ptr<TResult>> Results;
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

            auto result = std::make_shared<TResolverContext::TResult>();
            resolverContext->Results.push_back(result);
            auto future = gateway->ResolveFunction(State->ScopeFolderId, functionDesc.FunctionName);
            resolverHandles.push_back(future.Apply([result, position](const NThreading::TFuture<TDqFunctionDescription>& future) {
                try {
                    result->Description = future.GetValue();
                } catch (const std::exception& e) {
                    result->Issue = ExceptionToIssue(e, position);
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
        YQL_ENSURE(AllFutures.HasValue());
        output = input;

        bool failed = false;
        for (const auto& result : ResolverContext->Results) {
            if (result->Issue) {
                ctx.IssueManager.RaiseIssue(*result->Issue);
                failed = true;
            }
        }
        if (failed) {
            ResolverContext.Reset();
            return TStatus::Error;
        }
        for (auto& result : ResolverContext->Results) {
            State->FunctionsDescription.emplace(std::move(*result->Description));
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
