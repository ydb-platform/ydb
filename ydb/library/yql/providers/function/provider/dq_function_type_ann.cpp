#include "dq_function_provider_impl.h"

#include <ydb/library/yql/providers/function/expr_nodes/dq_function_expr_nodes.h>

namespace NYql::NDqFunction {
namespace {

using namespace NNodes;

class TDqFunctionTypeAnnotationTransformer: public TVisitorTransformerBase {
public:
    TDqFunctionTypeAnnotationTransformer(TDqFunctionState::TPtr state)
        : TVisitorTransformerBase(true)
        , State(state)
    {

        using TSelf = TDqFunctionTypeAnnotationTransformer;
        AddHandler({TFunctionTransformSettings::CallableName()}, Hndl(&TSelf::HandleSettings));
    }

    TStatus HandleSettings(const TExprNode::TPtr& input, TExprContext& ctx) {
        input->SetTypeAnn(ctx.MakeType<TStreamExprType>(ctx.MakeType<TDataExprType>(EDataSlot::String)));
        return TStatus::Ok;
    }

private:
    TDqFunctionState::TPtr State;
};
} // namespace

THolder<TVisitorTransformerBase> CreateDqFunctionTypeAnnotation(TDqFunctionState::TPtr state) {
    return MakeHolder<TDqFunctionTypeAnnotationTransformer>(state);
}

}