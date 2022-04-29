#include "dq_function_provider_impl.h"

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/providers/common/provider/yql_data_provider_impl.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/providers/function/expr_nodes/dq_function_expr_nodes.h>

namespace NYql::NDqFunction {

namespace {

using namespace NNodes;

class TDqFunctionDataSink : public TDataProviderBase {
public:
    TDqFunctionDataSink(TDqFunctionState::TPtr state)
        : State(std::move(state))
        , PhysicalOptTransformer(CreateDqFunctionPhysicalOptTransformer(State))
        , LoadMetaDataTransformer(CreateDqFunctionMetaLoader(State))
        , IntentDeterminationTransformer(CreateDqFunctionIntentTransformer(State))
        , DqIntegration(CreateDqFunctionDqIntegration(State))
        , TypeAnnotationTransformer(CreateDqFunctionTypeAnnotation(State))
    {}

    TStringBuf GetName() const override {
        return FunctionProviderName;
    }

    bool ValidateParameters(TExprNode& node, TExprContext& ctx, TMaybe<TString>& cluster) override {
        if (node.IsCallable(TCoDataSink::CallableName())) {
            // (DataSink 'Function 'CloudFunction 'connection_name)
            if (!EnsureArgsCount(node, 3, ctx)) {
                return false;
            }
            if (node.Head().Content() == FunctionProviderName) {
                TDqFunctionType functionType{node.Child(1)->Content()};
                if (!State->GatewayFactory->IsKnownFunctionType(functionType)) {
                    ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() <<
                        "Unknown EXTERNAL FUNCTION type '" << functionType << "'"));
                    return false;
                }
                cluster = Nothing();
                return true;
            }
        }

        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), "Invalid ExternalFunction DataSink parameters"));
        return false;
    }

    bool CanParse(const TExprNode& node) override {
        return node.IsCallable(TDqSqlExternalFunction::CallableName())
            || TypeAnnotationTransformer->CanParse(node);
    }

    IGraphTransformer& GetPhysicalOptProposalTransformer() override {
        return *PhysicalOptTransformer;
    }

    IGraphTransformer& GetLoadTableMetadataTransformer() override {
        return *LoadMetaDataTransformer;
    }

    IGraphTransformer& GetIntentDeterminationTransformer() override {
        return *IntentDeterminationTransformer;
    }

    IDqIntegration* GetDqIntegration() override {
        return DqIntegration.Get();
    }

    IGraphTransformer& GetTypeAnnotationTransformer(bool instantOnly) override {
        Y_UNUSED(instantOnly);
        return *TypeAnnotationTransformer;
    }

private:
    const TDqFunctionState::TPtr State;
    const THolder<IGraphTransformer> PhysicalOptTransformer;
    const THolder<IGraphTransformer> LoadMetaDataTransformer;
    const THolder<TVisitorTransformerBase> IntentDeterminationTransformer;
    const THolder<IDqIntegration> DqIntegration;
    const THolder<TVisitorTransformerBase> TypeAnnotationTransformer;
};
} // namespace

TIntrusivePtr<IDataProvider> CreateDqFunctionDataSink(TDqFunctionState::TPtr state) {
    return new TDqFunctionDataSink(std::move(state));
}

}