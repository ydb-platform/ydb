#include "yql_typehandle.h"
#include "yql_type_resource.h"

#include <yql/essentials/providers/common/schema/expr/yql_expr_schema.h>
#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>

namespace NKikimr::NMiniKQL {

class TTypeHandleWrapper: public TMutableComputationNode<TTypeHandleWrapper> {
    typedef TMutableComputationNode<TTypeHandleWrapper> TBaseComputation;

public:
    TTypeHandleWrapper(TComputationMutables& mutables, const TString& yson, ui32 exprCtxMutableIndex)
        : TBaseComputation(mutables)
        , Yson_(yson)
        , ExprCtxMutableIndex_(exprCtxMutableIndex)
    {
    }

    NUdf::TUnboxedValue DoCalculate(TComputationContext& ctx) const {
        auto exprCtxPtr = GetExprContextPtr(ctx, ExprCtxMutableIndex_);
        const NYql::TTypeAnnotationNode* type = NYql::NCommon::ParseTypeFromYson(TStringBuf{Yson_}, *exprCtxPtr);
        if (!type) {
            UdfTerminate(exprCtxPtr->IssueManager.GetIssues().ToString().data());
        }

        return NUdf::TUnboxedValuePod(new TYqlTypeResource(exprCtxPtr, type));
    }

    void RegisterDependencies() const override {
    }

private:
    TString Yson_;
    ui32 ExprCtxMutableIndex_;
};

IComputationNode* WrapTypeHandle(TCallable& callable, const TComputationNodeFactoryContext& ctx, ui32 exprCtxMutableIndex) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 arg");
    TString yson(TStringBuf(AS_VALUE(TDataLiteral, callable.GetInput(0))->AsValue().AsStringRef()));
    return new TTypeHandleWrapper(ctx.Mutables, yson, exprCtxMutableIndex);
}

} // namespace NKikimr::NMiniKQL
