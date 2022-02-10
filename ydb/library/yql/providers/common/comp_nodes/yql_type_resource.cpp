#include "yql_type_resource.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>

namespace NKikimr {
namespace NMiniKQL {

struct TComputationContext;

extern const char YqlTypeResourceTag[] = "_Type";
extern const char YqlCodeResourceTag[] = "_Expr";
extern const char YqlExprContextResourceTag[] = "_ExprContext";

std::shared_ptr<NYql::TExprContext> GetExprContextPtr(TComputationContext& ctx, ui32 index) {
    auto& value = ctx.MutableValues[index];
    if (value.IsInvalid()) {
        value = NUdf::TUnboxedValuePod(new TYqlExprContextResource(std::make_shared<NYql::TExprContext>()));
    }

    return *dynamic_cast<TYqlExprContextResource*>(value.AsBoxed().Get())->Get();
}

NYql::TExprContext& GetExprContext(TComputationContext& ctx, ui32 index) {
    return *GetExprContextPtr(ctx, index);
}

const NYql::TTypeAnnotationNode* GetYqlType(const NUdf::TUnboxedValue& value) {
    return dynamic_cast<TYqlTypeResource*>(value.AsBoxed().Get())->Get()->second;
}

NYql::TExprNode::TPtr GetYqlCode(const NUdf::TUnboxedValue& value) {
    return dynamic_cast<TYqlCodeResource*>(value.AsBoxed().Get())->Get()->second;
}

}
}
