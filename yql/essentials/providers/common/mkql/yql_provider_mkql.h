#pragma once

#include "yql_type_mkql.h"

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_program_builder.h>

namespace NYql::NCommon {

class IMkqlCallableCompiler;

struct TMkqlBuildContext {
    using TArgumentsMap = TNodeMap<NKikimr::NMiniKQL::TRuntimeNode>;
    using TMemoizedNodesMap = TArgumentsMap;

    const IMkqlCallableCompiler& MkqlCompiler;
    NKikimr::NMiniKQL::TProgramBuilder& ProgramBuilder;
    TExprContext& ExprCtx;
    TMemoizedNodesMap Memoization;
    TMemoizedTypesMap TypeMemoizationHolder;
    TMemoizedTypesMap* TypeMemoization;
    TMkqlBuildContext* const ParentCtx = nullptr;
    const size_t Level = 0ULL;
    const ui64 LambdaId = 0ULL;
    NKikimr::NMiniKQL::TRuntimeNode Parameters;

    TMkqlBuildContext(const IMkqlCallableCompiler& mkqlCompiler, NKikimr::NMiniKQL::TProgramBuilder& builder, TExprContext& exprCtx, ui64 lambdaId = 0ULL, TArgumentsMap&& args = {}, TMemoizedTypesMap* typeMemoization = nullptr)
        : MkqlCompiler(mkqlCompiler)
        , ProgramBuilder(builder)
        , ExprCtx(exprCtx)
        , Memoization(std::move(args))
        , TypeMemoization(typeMemoization ? typeMemoization : &TypeMemoizationHolder)
        , LambdaId(lambdaId)
    {
    }

    TMkqlBuildContext(TMkqlBuildContext& parent, TArgumentsMap&& args, ui64 lambdaId)
        : MkqlCompiler(parent.MkqlCompiler)
        , ProgramBuilder(parent.ProgramBuilder)
        , ExprCtx(parent.ExprCtx)
        , Memoization(std::move(args))
        , TypeMemoization(parent.TypeMemoization)
        , ParentCtx(&parent)
        , Level(parent.Level + 1U)
        , LambdaId(lambdaId)
        , Parameters(parent.Parameters)
    {
    }

    NKikimr::NMiniKQL::TType* BuildType(const TExprNode& owner, const TTypeAnnotationNode& annotation) {
        return NYql::NCommon::BuildType(owner, annotation, ProgramBuilder, *TypeMemoization);
    }
};

class IMkqlCallableCompiler: public TThrRefBase {
public:
    typedef std::function<NKikimr::NMiniKQL::TRuntimeNode(const TExprNode&, TMkqlBuildContext&)> TCompiler;
    virtual bool HasCallable(const std::string_view& name) const = 0;
    virtual TCompiler FindCallable(const std::string_view& name) const = 0;
    virtual TCompiler GetCallable(const std::string_view& name) const = 0;

    ~IMkqlCallableCompiler() override {
    }
};

class TMkqlCallableCompilerBase: public IMkqlCallableCompiler {
public:
    bool HasCallable(const std::string_view& name) const override;
    TCompiler FindCallable(const std::string_view& name) const override;
    TCompiler GetCallable(const std::string_view& name) const override;
    virtual void AddCallable(const std::string_view& name, TCompiler compiler);
    virtual void AddCallable(const std::initializer_list<std::string_view>& names, TCompiler compiler);
    virtual void ChainCallable(const std::string_view& name, TCompiler compiler);
    virtual void ChainCallable(const std::initializer_list<std::string_view>& names, TCompiler compiler);
    virtual void OverrideCallable(const std::string_view& name, TCompiler compiler);

private:
    THashMap<TString, TCompiler> Callables_;

protected:
    void AddSimpleCallables(const std::initializer_list<std::pair<std::string_view, NKikimr::NMiniKQL::TProgramBuilder::UnaryFunctionMethod>>& callables);
    void AddSimpleCallables(const std::initializer_list<std::pair<std::string_view, NKikimr::NMiniKQL::TProgramBuilder::BinaryFunctionMethod>>& callables);
    void AddSimpleCallables(const std::initializer_list<std::pair<std::string_view, NKikimr::NMiniKQL::TProgramBuilder::TernaryFunctionMethod>>& callables);
    void AddSimpleCallables(const std::initializer_list<std::pair<std::string_view, NKikimr::NMiniKQL::TProgramBuilder::ArrayFunctionMethod>>& callables);
    void AddSimpleCallables(const std::initializer_list<std::pair<std::string_view, NKikimr::NMiniKQL::TProgramBuilder::ProcessFunctionMethod>>& callables);
    void AddSimpleCallables(const std::initializer_list<std::pair<std::string_view, NKikimr::NMiniKQL::TProgramBuilder::NarrowFunctionMethod>>& callables);
};

class TMkqlCommonCallableCompiler: public TMkqlCallableCompilerBase {
public:
    bool HasCallable(const std::string_view& name) const override;
    TCompiler FindCallable(const std::string_view& name) const override;
    TCompiler GetCallable(const std::string_view& name) const override;
    void AddCallable(const std::string_view& name, TCompiler compiler) override;
    void AddCallable(const std::initializer_list<std::string_view>& names, TCompiler compiler) override;
    void OverrideCallable(const std::string_view& name, TCompiler compiler) override;

private:
    class TShared: public TMkqlCallableCompilerBase {
    public:
        TShared();
    };

    const TShared& GetShared() const {
        return *Singleton<TShared>();
    }
};

NKikimr::NMiniKQL::EJoinKind GetJoinKind(const TExprNode& owner, const std::string_view& content);

NKikimr::NMiniKQL::TRuntimeNode CombineByKeyImpl(const TExprNode& node, TMkqlBuildContext& ctx);
NKikimr::NMiniKQL::TRuntimeNode MkqlBuildExpr(const TExprNode& node, TMkqlBuildContext& ctx);
NKikimr::NMiniKQL::TRuntimeNode MkqlBuildLambda(const TExprNode& lambda, TMkqlBuildContext& ctx, const NKikimr::NMiniKQL::TRuntimeNode::TList& args);
NKikimr::NMiniKQL::TRuntimeNode::TList MkqlBuildWideLambda(const TExprNode& lambda, TMkqlBuildContext& ctx, const NKikimr::NMiniKQL::TRuntimeNode::TList& args);

} // namespace NYql::NCommon
