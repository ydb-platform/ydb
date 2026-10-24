#include "dq_function_provider_impl.h"
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_type_helpers.h>

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
        AddHandler({"WasmServiceCall"}, Hndl(&TSelf::HandleServiceCall));
        AddHandler({TFunctionTransformSettings::CallableName()}, Hndl(&TSelf::HandleSettings));
    }

    TStatus HandleServiceCall(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!EnsureMinMaxArgsCount(*input, 3, 4, ctx) || !EnsureCallableType(input->Head(), ctx) ||
            !EnsureStructType(*input->Child(1), ctx)) return TStatus::Error;
        const auto* callable = input->Head().GetTypeAnn()->Cast<TCallableExprType>();
        const auto* args = callable->GetArguments()[0].Type->Cast<TListExprType>()->GetItemType();
        if (!IsSameAnnotation(*args, *input->Child(1)->GetTypeAnn())) {
            ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), "Service arguments do not match the manifest input types"));
            return TStatus::Error;
        }
        input->SetTypeAnn(callable->GetReturnType()->Cast<TListExprType>()->GetItemType());
        return TStatus::Ok;
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