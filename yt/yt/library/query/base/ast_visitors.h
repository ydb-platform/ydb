#pragma once

#include "ast.h"

namespace NYT::NQueryClient::NAst {

////////////////////////////////////////////////////////////////////////////////

template <class TResult, class TDerived, class TNode>
struct TAbstractAstVisitor
{
    TDerived* Derived();

    TResult Visit(TNode node);
};

////////////////////////////////////////////////////////////////////////////////

template <class TResult, class TDerived>
struct TBaseAstVisitor
    : public TAbstractAstVisitor<TResult, TDerived, TExpressionPtr>
{
    TExpressionPtr GetExpression(TExpressionPtr expr);

    TResult OnQuery(const TQueryExpressionPtr /*queryExpr*/);
};

////////////////////////////////////////////////////////////////////////////////

template <class TDerived>
struct TAstVisitor
    : public TBaseAstVisitor<void, TDerived>
{
    using TBase = TBaseAstVisitor<void, TDerived>;
    using TBase::Visit;

    void OnLiteral(const TLiteralExpressionPtr literalExpr);
    void OnReference(const TReferenceExpressionPtr referenceExpr);
    void OnAlias(const TAliasExpressionPtr aliasExpr);
    void OnUnary(const TUnaryOpExpressionPtr unaryExpr);
    void OnBinary(const TBinaryOpExpressionPtr binaryExpr);
    void OnFunction(const TFunctionExpressionPtr functionExpr);
    void OnIn(const TInExpressionPtr inExpr);
    void OnBetween(const TBetweenExpressionPtr betweenExpr);
    void OnTransform(const TTransformExpressionPtr transformExpr);
    void OnCase(const TCaseExpressionPtr caseExpr);
    void OnLike(const TLikeExpressionPtr likeExpr);
    void OnQuery(const TQueryExpressionPtr queryExpr);
    void Visit(const std::vector<TExpressionPtr>& tuple);
    void Visit(const std::optional<std::vector<TExpressionPtr>>& nullableTuple);
};

////////////////////////////////////////////////////////////////////////////////

template <class TDerived>
class TAliasVisitingAstVisitor
    : public TAstVisitor<TDerived>
{
public:
    using TBase = TAstVisitor<TDerived>;
    using TBase::Visit;

    explicit TAliasVisitingAstVisitor(const NAst::TAliasMap& aliasMap);

    void OnReference(TReferenceExpressionPtr referenceExpr);

private:
    const NAst::TAliasMap& AliasMap_;
    THashSet<std::string> UsedAliases_;
};

////////////////////////////////////////////////////////////////////////////////

template <class TDerived>
struct TRewriter
    : public TBaseAstVisitor<TExpressionPtr, TDerived>
{
    using TBaseAstVisitor<TExpressionPtr, TDerived>::Visit;

    TObjectsHolder* Head;

    explicit TRewriter(TObjectsHolder* head);

    TExpressionPtr OnLiteral(TLiteralExpressionPtr literalExpr);
    TExpressionPtr OnReference(TReferenceExpressionPtr referenceExpr);
    TExpressionPtr OnAlias(TAliasExpressionPtr aliasExpr);
    TExpressionPtr OnUnary(TUnaryOpExpressionPtr unaryExpr);
    TExpressionPtr OnBinary(TBinaryOpExpressionPtr binaryExpr);
    TExpressionPtr OnFunction(TFunctionExpressionPtr functionExpr);
    TExpressionPtr OnIn(TInExpressionPtr inExpr);
    TExpressionPtr OnBetween(TBetweenExpressionPtr betweenExpr);
    TExpressionPtr OnTransform(TTransformExpressionPtr transformExpr);
    TExpressionPtr OnCase(TCaseExpressionPtr caseExprExpr);
    TExpressionPtr OnLike(TLikeExpressionPtr likeExpr);
    TExpressionPtr OnQuery(TQueryExpressionPtr queryExpr);

    std::vector<TExpressionPtr> Visit(const std::vector<TExpressionPtr>& tuple);
    std::optional<std::vector<TExpressionPtr>> Visit(const std::optional<std::vector<TExpressionPtr>>& nullableTuple);
};

////////////////////////////////////////////////////////////////////////////////

class TReferenceHarvester
    : public TAstVisitor<TReferenceHarvester>
{
public:
    explicit TReferenceHarvester(TColumnSet* storage);

    void OnReference(const TReferenceExpression* referenceExpr);

private:
    TColumnSet* const Storage_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient::NAst

#define AST_VISITORS_INL_H_
#include "ast_visitors-inl.h"
#undef AST_VISITORS_INL_H_
