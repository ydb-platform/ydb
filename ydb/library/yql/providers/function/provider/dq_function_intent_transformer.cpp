#include "dq_function_provider.h"

#include <ydb/library/yql/providers/common/transform/yql_visit.h>
#include <ydb/library/yql/providers/function/expr_nodes/dq_function_expr_nodes.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>

namespace NYql::NDqFunction {
namespace {

using namespace NNodes;

class TDqFunctionIntentTransformer : public TVisitorTransformerBase {
public:
    TDqFunctionIntentTransformer(TDqFunctionState::TPtr state)
        : TVisitorTransformerBase(false)
        , State(state)
    {
        AddHandler({TDqSqlExternalFunction::CallableName()}, Hndl(&TDqFunctionIntentTransformer::ExtractFunctionsName));
    }

    TStatus ExtractFunctionsName(TExprBase input, TExprContext& ctx) {
        auto sqlFunction = input.Cast<TDqSqlExternalFunction>();
        auto functionType = TString{sqlFunction.TransformType().Ref().Tail().Content()};
        auto functionName = TString{sqlFunction.TransformName().Ref().Tail().Content()};
        TString connection;
        for (const auto &tuple: sqlFunction.Settings().Ref().Children()) {
            const auto paramName = tuple->Head().Content();
            if (paramName == "connection") {
                connection = TString{tuple->Tail().Tail().Content()};
            }
        }

        if (connection.empty()) {
            ctx.AddError(TIssue(ctx.GetPosition(input.Pos()), TStringBuilder()
                << "Empty CONNECTION name for EXTERNAL FUNCTION '" << functionName << "'"));
            return TStatus::Error;
        }

        State->FunctionsResolver->AddFunction(functionType, functionName, connection);
        return TStatus::Ok;
    }

private:
    TDqFunctionState::TPtr State;
};
}

THolder<TVisitorTransformerBase> CreateDqFunctionIntentTransformer(TDqFunctionState::TPtr state) {
    return THolder(new TDqFunctionIntentTransformer(state));
}

}