#include "yql_formatcode.h"
#include "yql_type_resource.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_impl.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/ast/serialize/yql_expr_serialize.h>

namespace NKikimr {
namespace NMiniKQL {

class TFormatCodeWrapper : public TMutableComputationNode<TFormatCodeWrapper> {
    typedef TMutableComputationNode<TFormatCodeWrapper> TBaseComputation;
public:
    TFormatCodeWrapper(TComputationMutables& mutables, IComputationNode* code, bool annotatePosition, ui32 exprCtxMutableIndex)
        : TBaseComputation(mutables)
        , Code_(code)
        , AnnotatePosition_(annotatePosition)
        , ExprCtxMutableIndex_(exprCtxMutableIndex)
    {}

    NUdf::TUnboxedValue DoCalculate(TComputationContext& ctx) const {
        auto codeValue = Code_->GetValue(ctx);
        auto code = GetYqlCode(codeValue);
        NYql::TExprContext& exprCtx = GetExprContext(ctx, ExprCtxMutableIndex_);
        NYql::TConvertToAstSettings settings;
        settings.AnnotationFlags = AnnotatePosition_ ?
            NYql::TExprAnnotationFlags::Position :
            NYql::TExprAnnotationFlags::None;
        settings.RefAtoms = true;
        settings.AllowFreeArgs = true;
        auto ast = NYql::ConvertToAst(*code, exprCtx, settings);
        auto str = ast.Root->ToString(NYql::TAstPrintFlags::PerLine | NYql::TAstPrintFlags::ShortQuote);
        return MakeString(str);
    }

    void RegisterDependencies() const override {
        DependsOn(Code_);
    }

private:
    IComputationNode* Code_;
    bool AnnotatePosition_;
    const ui32 ExprCtxMutableIndex_;
};

class TSerializeCodeWrapper : public TMutableComputationNode<TSerializeCodeWrapper> {
    typedef TMutableComputationNode<TSerializeCodeWrapper> TBaseComputation;
public:
    TSerializeCodeWrapper(TComputationMutables& mutables, IComputationNode* code, ui32 exprCtxMutableIndex)
        : TBaseComputation(mutables)
        , Code_(code)
        , ExprCtxMutableIndex_(exprCtxMutableIndex)
    {}

    NUdf::TUnboxedValue DoCalculate(TComputationContext& ctx) const {
        auto codeValue = Code_->GetValue(ctx);
        auto code = GetYqlCode(codeValue);
        NYql::TExprContext& exprCtx = GetExprContext(ctx, ExprCtxMutableIndex_);
        auto str = NYql::SerializeGraph(*code, exprCtx,
            NYql::TSerializedExprGraphComponents::Graph |
            NYql::TSerializedExprGraphComponents::Positions);
        return MakeString(str);
    }

    void RegisterDependencies() const override {
        DependsOn(Code_);
    }

private:
    IComputationNode* Code_;
    const ui32 ExprCtxMutableIndex_;
};

template <bool AnnotatePosition>
IComputationNode* WrapFormatCode(TCallable& callable, const TComputationNodeFactoryContext& ctx, ui32 exprCtxMutableIndex) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 arg");
    auto code = LocateNode(ctx.NodeLocator, callable, 0);
    return new TFormatCodeWrapper(ctx.Mutables, code, AnnotatePosition, exprCtxMutableIndex);
}

template IComputationNode* WrapFormatCode<false>
    (TCallable& callable, const TComputationNodeFactoryContext& ctx, ui32 exprCtxMutableIndex);

template IComputationNode* WrapFormatCode<true>
    (TCallable& callable, const TComputationNodeFactoryContext& ctx, ui32 exprCtxMutableIndex);

IComputationNode* WrapSerializeCode(TCallable& callable, const TComputationNodeFactoryContext& ctx, ui32 exprCtxMutableIndex) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 arg");
    auto code = LocateNode(ctx.NodeLocator, callable, 0);
    return new TSerializeCodeWrapper(ctx.Mutables, code, exprCtxMutableIndex);
}

}
}
