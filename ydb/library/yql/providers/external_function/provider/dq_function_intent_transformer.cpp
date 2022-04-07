#include "dq_function_provider.h"

#include <ydb/library/yql/providers/common/transform/yql_visit.h>
#include <ydb/library/yql/providers/external_function/expr_nodes/dq_function_expr_nodes.h>

namespace NYql::NDqFunction {
namespace {

using namespace NNodes;

class TDqFunctionIntentTransformer : public TVisitorTransformerBase {
public:
    TDqFunctionIntentTransformer(TDqFunctionState::TPtr state)
        : TVisitorTransformerBase(false)
        , State(state)
    {
        AddHandler({TFunctionDataSource::CallableName()}, Hndl(&TDqFunctionIntentTransformer::HandleDataSource));
    }

    TStatus HandleDataSource(TExprBase input, TExprContext& ctx) {
        Y_UNUSED(ctx);

        if (!TFunctionDataSource::Match(input.Raw()))
            return TStatus::Ok;

        auto source = input.Cast<TFunctionDataSource>();
        TDqFunctionType functionType{source.Type().Value()};
        TString functionName{source.FunctionName().Value()};
        TString connection{source.Connection().Value()};
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