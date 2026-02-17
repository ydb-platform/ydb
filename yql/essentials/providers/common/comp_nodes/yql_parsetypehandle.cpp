#include "yql_parsetypehandle.h"
#include "yql_position.h"
#include "yql_type_resource.h"
#include <yql/essentials/ast/yql_ast.h>
#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/ast/yql_type_string.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/core/type_ann/type_ann_expr.h>
#include <yql/essentials/core/yql_graph_transformer.h>

namespace NKikimr::NMiniKQL {

class TParseTypeHandleWrapper: public TMutableComputationNode<TParseTypeHandleWrapper> {
    typedef TMutableComputationNode<TParseTypeHandleWrapper> TBaseComputation;

public:
    TParseTypeHandleWrapper(TComputationMutables& mutables, IComputationNode* str, ui32 exprCtxMutableIndex, NYql::TPosition pos)
        : TBaseComputation(mutables)
        , Str_(str)
        , ExprCtxMutableIndex_(exprCtxMutableIndex)
        , Pos_(pos)
    {
    }

    NUdf::TUnboxedValue DoCalculate(TComputationContext& ctx) const {
        auto str = Str_->GetValue(ctx);
        TMemoryPool pool(4096);
        NYql::TIssues issues;
        auto parsedType = NYql::ParseType(str.AsStringRef(), pool, issues, Pos_);
        if (!parsedType) {
            UdfTerminate(issues.ToString().data());
        }

        auto exprCtxPtr = GetExprContextPtr(ctx, ExprCtxMutableIndex_);
        auto astRoot = NYql::TAstNode::NewList({}, pool,
                                               NYql::TAstNode::NewList({}, pool,
                                                                       NYql::TAstNode::NewLiteralAtom({}, TStringBuf("return"), pool), parsedType));
        NYql::TExprNode::TPtr exprRoot;
        if (!CompileExpr(*astRoot, exprRoot, *exprCtxPtr, nullptr, nullptr)) {
            UdfTerminate(exprCtxPtr->IssueManager.GetIssues().ToString().data());
        }

        // TODO: Collect type annotation directly from AST.
        NYql::TTypeAnnotationContext typesCtx;
        typesCtx.LangVer = ctx.LangVer;
        auto callableTransformer = NYql::CreateExtCallableTypeAnnotationTransformer(typesCtx);
        auto typeTransformer = NYql::CreateTypeAnnotationTransformer(callableTransformer, typesCtx);
        if (NYql::InstantTransform(*typeTransformer, exprRoot, *exprCtxPtr) != NYql::IGraphTransformer::TStatus::Ok) {
            UdfTerminate(exprCtxPtr->IssueManager.GetIssues().ToString().data());
        }

        return NUdf::TUnboxedValuePod(new TYqlTypeResource(exprCtxPtr, exprRoot->GetTypeAnn()->Cast<NYql::TTypeExprType>()->GetType()));
    }

    void RegisterDependencies() const override {
        DependsOn(Str_);
    }

private:
    IComputationNode* Str_;
    ui32 ExprCtxMutableIndex_;
    NYql::TPosition Pos_;
};

IComputationNode* WrapParseTypeHandle(TCallable& callable, const TComputationNodeFactoryContext& ctx, ui32 exprCtxMutableIndex) {
    MKQL_ENSURE(callable.GetInputsCount() == 4, "Expected 4 args");
    auto pos = ExtractPosition(callable);
    auto str = LocateNode(ctx.NodeLocator, callable, 3);
    return new TParseTypeHandleWrapper(ctx.Mutables, str, exprCtxMutableIndex, pos);
}

} // namespace NKikimr::NMiniKQL
