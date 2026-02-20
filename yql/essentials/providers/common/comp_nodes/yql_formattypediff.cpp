#include "yql/essentials/core/yql_expr_type_annotation.h"
#include "yql/essentials/minikql/mkql_node_cast.h"
#include <yql/essentials/providers/common/schema/expr/yql_expr_schema.h>
#include "yql_formattypediff.h"
#include "yql_type_resource.h"
#include <yql/essentials/ast/yql_type_string.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/minikql/mkql_string_util.h>

namespace NKikimr::NMiniKQL {

template <bool Pretty>
class TFormatTypeDiffWrapper: public TMutableComputationNode<TFormatTypeDiffWrapper<Pretty>> {
    typedef TMutableComputationNode<TFormatTypeDiffWrapper<Pretty>> TBaseComputation;

public:
    TFormatTypeDiffWrapper(TComputationMutables& mutables, IComputationNode* handle_left, IComputationNode* handle_right)
        : TBaseComputation(mutables)
        , HandleLeft_(handle_left)
        , HandleRight_(handle_right)
    {
    }

    NUdf::TUnboxedValue DoCalculate(TComputationContext& ctx) const {
        const NYql::TTypeAnnotationNode* type_left = GetYqlType(HandleLeft_->GetValue(ctx));
        const NYql::TTypeAnnotationNode* type_right = GetYqlType(HandleRight_->GetValue(ctx));
        if constexpr (Pretty) {
            return MakeString(NYql::GetTypePrettyDiff(*type_left, *type_right));
        } else {
            return MakeString(NYql::GetTypeDiff(*type_left, *type_right));
        }
    }

    void RegisterDependencies() const override {
        this->DependsOn(HandleLeft_);
        this->DependsOn(HandleRight_);
    }

private:
    IComputationNode* HandleLeft_;
    IComputationNode* HandleRight_;
};

IComputationNode* WrapFormatTypeDiff(TCallable& callable, const TComputationNodeFactoryContext& ctx, ui32 exprCtxMutableIndex) {
    Y_UNUSED(exprCtxMutableIndex);
    MKQL_ENSURE(callable.GetInputsCount() == 3, "Expected 3 args");
    bool pretty = AS_VALUE(TDataLiteral, callable.GetInput(2))->AsValue().Get<bool>();
    auto handle_left = LocateNode(ctx.NodeLocator, callable, 0);
    auto handle_right = LocateNode(ctx.NodeLocator, callable, 1);
    if (pretty) {
        return new TFormatTypeDiffWrapper<true>(ctx.Mutables, handle_left, handle_right);
    }
    return new TFormatTypeDiffWrapper<false>(ctx.Mutables, handle_left, handle_right);
}

} // namespace NKikimr::NMiniKQL
