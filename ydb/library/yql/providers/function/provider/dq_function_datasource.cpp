#include "dq_function_provider_impl.h"

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/providers/common/provider/yql_data_provider_impl.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>

namespace NYql::NDqFunction {

namespace {

using namespace NNodes;

class TDqFunctionDataSource : public TDataProviderBase {
public:
    TDqFunctionDataSource(TDqFunctionState::TPtr state)
        : State(std::move(state))
        , LoadMetaDataTransformer(CreateDqFunctionMetaLoader(State))
    {}

    TStringBuf GetName() const override {
        return FunctionProviderName;
    }

    bool ValidateParameters(TExprNode& node, TExprContext& ctx, TMaybe<TString>& cluster) override {
        if (node.IsCallable(TCoDataSource::CallableName())) {
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

        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), "Invalid ExternalFunction DataSource parameters"));
        return false;
    }

    IGraphTransformer& GetLoadTableMetadataTransformer() override {
        return *LoadMetaDataTransformer;
    }

private:
    const TDqFunctionState::TPtr State;
    const THolder<IGraphTransformer> LoadMetaDataTransformer;
};
}


TIntrusivePtr<IDataProvider> CreateDqFunctionDataSource(TDqFunctionState::TPtr state) {
    return new TDqFunctionDataSource(std::move(state));
}



}