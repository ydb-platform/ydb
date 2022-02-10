#include "yql_formatcode.h"
#include "yql_type_resource.h"
#include "yql_position.h"

#include <ydb/library/yql/providers/common/schema/expr/yql_expr_schema.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_impl.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/providers/common/codec/yql_codec.h>

namespace NKikimr {
namespace NMiniKQL {

class TReprCodeWrapper : public TMutableComputationNode<TReprCodeWrapper> {
    typedef TMutableComputationNode<TReprCodeWrapper> TBaseComputation;
public:
    TReprCodeWrapper(TComputationMutables& mutables, IComputationNode* value, const TString& yson, ui32 exprCtxMutableIndex, NYql::TPosition pos)
        : TBaseComputation(mutables)
        , Value_(value)
        , Yson_(yson)
        , ExprCtxMutableIndex_(exprCtxMutableIndex)
        , Pos_(pos)
    {}

    NUdf::TUnboxedValue DoCalculate(TComputationContext& ctx) const {
        auto exprCtxPtr = GetExprContextPtr(ctx, ExprCtxMutableIndex_);
        const NYql::TTypeAnnotationNode* type = NYql::NCommon::ParseTypeFromYson(TStringBuf{Yson_}, *exprCtxPtr, Pos_);
        auto value = Value_->GetValue(ctx);
        auto node = NYql::NCommon::ValueToExprLiteral(type, value, *exprCtxPtr, exprCtxPtr->AppendPosition(Pos_));
        return NUdf::TUnboxedValuePod(new TYqlCodeResource(exprCtxPtr, node));
    }

    void RegisterDependencies() const override {
        DependsOn(Value_);
    }

private:
    IComputationNode* Value_;
    TString Yson_;
    ui32 ExprCtxMutableIndex_;
    NYql::TPosition Pos_;
};

IComputationNode* WrapReprCode(TCallable& callable, const TComputationNodeFactoryContext& ctx, ui32 exprCtxMutableIndex) {
    MKQL_ENSURE(callable.GetInputsCount() == 5, "Expected 5 args");
    auto pos = ExtractPosition(callable);
    auto value = LocateNode(ctx.NodeLocator, callable, 3);
    TString yson(TStringBuf(AS_VALUE(TDataLiteral, callable.GetInput(4))->AsValue().AsStringRef()));
    return new TReprCodeWrapper(ctx.Mutables, value, yson, exprCtxMutableIndex, pos);
}

}
}
