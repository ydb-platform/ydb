#ifndef AST_VISITORS_INL_H_
#error "Direct inclusion of this file is not allowed, include ast_visitors.h"
// For the sake of sane code completion.
#include "ast_visitors.h"
#endif

#include "helpers.h"

#include <yt/yt/core/misc/finally.h>

namespace NYT::NQueryClient::NAst {

////////////////////////////////////////////////////////////////////////////////

template <class TResult, class TDerived, class TNode>
TDerived* TAbstractAstVisitor<TResult, TDerived, TNode>::Derived()
{
    return static_cast<TDerived*>(this);
}

template <class TResult, class TDerived, class TNode>
TResult TAbstractAstVisitor<TResult, TDerived, TNode>::Visit(TNode node)
{
    CheckStackDepth();
    auto* expr = Derived()->GetExpression(node);

    if (auto* literalExpr = expr->template As<TLiteralExpression>()) {
        return Derived()->OnLiteral(literalExpr);
    } else if (auto* referenceExpr = expr->template As<TReferenceExpression>()) {
        return Derived()->OnReference(referenceExpr);
    } else if (auto* aliasExpr = expr->template As<TAliasExpression>()) {
        return Derived()->OnAlias(aliasExpr);
    } else if (auto* unaryOp = expr->template As<TUnaryOpExpression>()) {
        return Derived()->OnUnary(unaryOp);
    } else if (auto* binaryOp = expr->template As<TBinaryOpExpression>()) {
        return Derived()->OnBinary(binaryOp);
    } else if (auto* functionExpr = expr->template As<TFunctionExpression>()) {
        return Derived()->OnFunction(functionExpr);
    } else if (auto* inExpr = expr->template As<TInExpression>()) {
        return Derived()->OnIn(inExpr);
    } else if (auto* betweenExpr = expr->template As<TBetweenExpression>()) {
        return Derived()->OnBetween(betweenExpr);
    } else if (auto* transformExpr = expr->template As<TTransformExpression>()) {
        return Derived()->OnTransform(transformExpr);
    } else if (auto* caseExpr = expr->template As<TCaseExpression>()) {
        return Derived()->OnCase(caseExpr);
    } else if (auto* likeExpr = expr->template As<TLikeExpression>()) {
        return Derived()->OnLike(likeExpr);
    } else if (auto* queryExpr = expr->template As<TQueryExpression>()) {
        return Derived()->OnQuery(queryExpr);
    }
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

template <class TResult, class TDerived>
TExpressionPtr TBaseAstVisitor<TResult, TDerived>::GetExpression(TExpressionPtr expr)
{
    return expr;
}

template <class TResult, class TDerived>
TResult TBaseAstVisitor<TResult, TDerived>::OnQuery(const TQueryExpressionPtr /*queryExpr*/)
{
    THROW_ERROR_EXCEPTION("Subquery expression not supported yet");
}

////////////////////////////////////////////////////////////////////////////////

template <class TDerived>
void TAstVisitor<TDerived>::OnLiteral(const TLiteralExpressionPtr /*literalExpr*/)
{ }

template <class TDerived>
void TAstVisitor<TDerived>::OnReference(const TReferenceExpressionPtr /*referenceExpr*/)
{ }

template <class TDerived>
void TAstVisitor<TDerived>::OnAlias(const TAliasExpressionPtr /*aliasExpr*/)
{ }

template <class TDerived>
void TAstVisitor<TDerived>::OnUnary(const TUnaryOpExpressionPtr unaryExpr)
{
    Visit(unaryExpr->Operand);
}

template <class TDerived>
void TAstVisitor<TDerived>::OnBinary(const TBinaryOpExpressionPtr binaryExpr)
{
    Visit(binaryExpr->Lhs);
    Visit(binaryExpr->Rhs);
}

template <class TDerived>
void TAstVisitor<TDerived>::OnFunction(const TFunctionExpressionPtr functionExpr)
{
    Visit(functionExpr->Arguments);
}

template <class TDerived>
void TAstVisitor<TDerived>::OnIn(const TInExpressionPtr inExpr)
{
    Visit(inExpr->Expr);
}

template <class TDerived>
void TAstVisitor<TDerived>::OnBetween(const TBetweenExpressionPtr betweenExpr)
{
    Visit(betweenExpr->Expr);
}

template <class TDerived>
void TAstVisitor<TDerived>::OnTransform(const TTransformExpressionPtr transformExpr)
{
    Visit(transformExpr->Expr);
    Visit(transformExpr->DefaultExpr);
}

template <class TDerived>
void TAstVisitor<TDerived>::OnCase(const TCaseExpressionPtr caseExpr)
{
    Visit(caseExpr->OptionalOperand);
    for (const auto& whenThenExpression : caseExpr->WhenThenExpressions) {
        Visit(whenThenExpression.Condition);
        Visit(whenThenExpression.Result);
    }
    Visit(caseExpr->DefaultExpression);
}

template <class TDerived>
void TAstVisitor<TDerived>::OnLike(const TLikeExpressionPtr likeExpr)
{
    Visit(likeExpr->Text);
    Visit(likeExpr->Pattern);
    Visit(likeExpr->EscapeCharacter);
}

template <class TDerived>
void TAstVisitor<TDerived>::OnQuery(const TQueryExpressionPtr /*queryExpr*/)
{ }

template <class TDerived>
void TAstVisitor<TDerived>::Visit(const std::vector<TExpressionPtr>& tuple)
{
    for (auto expr : tuple) {
        Visit(expr);
    }
}

template <class TDerived>
void TAstVisitor<TDerived>::Visit(const std::optional<std::vector<TExpressionPtr>>& nullableTuple)
{
    if (nullableTuple) {
        return Visit(*nullableTuple);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TDerived>
TRewriter<TDerived>::TRewriter(TObjectsHolder* head)
    : Head(head)
{ }

template <class TDerived>
TExpressionPtr TRewriter<TDerived>::OnLiteral(TLiteralExpressionPtr literalExpr)
{
    return literalExpr;
}

template <class TDerived>
TExpressionPtr TRewriter<TDerived>::OnReference(TReferenceExpressionPtr referenceExpr)
{
    return referenceExpr;
}

template <class TDerived>
TExpressionPtr TRewriter<TDerived>::OnAlias(TAliasExpressionPtr aliasExpr)
{
    auto* newExpression = Visit(aliasExpr->Expression);
    if (newExpression == aliasExpr->Expression) {
        return aliasExpr;
    } else {
        return Head->New<TAliasExpression>(NullSourceLocation, newExpression, aliasExpr->Name);
    }
}

template <class TDerived>
TExpressionPtr TRewriter<TDerived>::OnUnary(TUnaryOpExpressionPtr unaryExpr)
{
    auto newOperand = Visit(unaryExpr->Operand);
    if (newOperand == unaryExpr->Operand) {
        return unaryExpr;
    } else {
        return Head->New<TUnaryOpExpression>(NullSourceLocation, unaryExpr->Opcode, std::move(newOperand));
    }
}

template <class TDerived>
TExpressionPtr TRewriter<TDerived>::OnBinary(TBinaryOpExpressionPtr binaryExpr)
{
    auto newLhs = Visit(binaryExpr->Lhs);
    auto newRhs = Visit(binaryExpr->Rhs);

    if (newLhs == binaryExpr->Lhs && newRhs == binaryExpr->Rhs) {
        return binaryExpr;
    } else {
        return Head->New<TBinaryOpExpression>(
            NullSourceLocation,
            binaryExpr->Opcode,
            std::move(newLhs),
            std::move(newRhs));
    }
}

template <class TDerived>
TExpressionPtr TRewriter<TDerived>::OnFunction(TFunctionExpressionPtr functionExpr)
{
    auto newArguments = Visit(functionExpr->Arguments);

    if (newArguments == functionExpr->Arguments) {
        return functionExpr;
    } else {
        return Head->New<TFunctionExpression>(
            NullSourceLocation,
            functionExpr->FunctionName,
            std::move(newArguments));
    }
}

template <class TDerived>
TExpressionPtr TRewriter<TDerived>::OnIn(TInExpressionPtr inExpr)
{
    auto expression = Visit(inExpr->Expr);

    if (expression == inExpr->Expr) {
        return inExpr;
    } else {
        return Head->New<TInExpression>(NullSourceLocation, std::move(expression), inExpr->Values);
    }
}

template <class TDerived>
TExpressionPtr TRewriter<TDerived>::OnBetween(TBetweenExpressionPtr betweenExpr)
{
    auto expression = Visit(betweenExpr->Expr);

    if (expression == betweenExpr->Expr) {
        return betweenExpr;
    } else {
        return Head->New<TBetweenExpression>(NullSourceLocation, std::move(expression), betweenExpr->Values);
    }
}

template <class TDerived>
TExpressionPtr TRewriter<TDerived>::OnTransform(TTransformExpressionPtr transformExpr)
{
    auto expression = Visit(transformExpr->Expr);
    auto newDefault = Visit(transformExpr->DefaultExpr);

    if (newDefault == transformExpr->DefaultExpr && expression == transformExpr->Expr) {
        return transformExpr;
    } else {
        return Head->New<TTransformExpression>(
            NullSourceLocation,
            std::move(expression),
            transformExpr->From,
            transformExpr->To,
            std::move(newDefault));
    }
}

template <class TDerived>
TExpressionPtr TRewriter<TDerived>::OnCase(TCaseExpressionPtr caseExpr)
{
    auto newOptionalOperand = Visit(caseExpr->OptionalOperand);
    auto newDefaultExpression = Visit(caseExpr->DefaultExpression);

    int expressionCount = caseExpr->WhenThenExpressions.size();
    TWhenThenExpressionList newWhenThenExpressions(expressionCount);
    for (int index = 0; index < expressionCount; ++index) {
        newWhenThenExpressions[index] = {
            Visit(caseExpr->WhenThenExpressions[index].Condition),
            Visit(caseExpr->WhenThenExpressions[index].Result)
        };
    }

    if (newWhenThenExpressions == caseExpr->WhenThenExpressions &&
        newOptionalOperand == caseExpr->OptionalOperand &&
        newDefaultExpression == caseExpr->DefaultExpression)
    {
        return caseExpr;
    }

    return Head->New<TCaseExpression>(
        NullSourceLocation,
        std::move(newOptionalOperand),
        std::move(newWhenThenExpressions),
        std::move(newDefaultExpression));
}

template <class TDerived>
TExpressionPtr TRewriter<TDerived>::OnLike(TLikeExpressionPtr likeExpr)
{
    auto newText = Visit(likeExpr->Text);
    auto newPattern = Visit(likeExpr->Pattern);
    auto newEscape = Visit(likeExpr->EscapeCharacter);

    if (newText == likeExpr->Text && newPattern == likeExpr->Pattern && newEscape == likeExpr->EscapeCharacter) {
        return likeExpr;
    } else {
        return Head->New<TLikeExpression>(
            NullSourceLocation,
            std::move(newText),
            likeExpr->Opcode,
            std::move(newPattern),
            std::move(newEscape));
    }
}

template <class TDerived>
TExpressionPtr TRewriter<TDerived>::OnQuery(TQueryExpressionPtr queryExpr)
{
    return queryExpr;
}

template <class TDerived>
std::vector<TExpressionPtr> TRewriter<TDerived>::Visit(const std::vector<TExpressionPtr>& tuple)
{
    int size = tuple.size();
    std::vector<TExpressionPtr> newTuple(size);
    for (int index = 0; index < size; ++index) {
        newTuple[index] = Visit(tuple[index]);
    }
    return newTuple;
}

template <class TDerived>
std::optional<std::vector<TExpressionPtr>> TRewriter<TDerived>::Visit(const std::optional<std::vector<TExpressionPtr>>& nullableTuple)
{
    if (!nullableTuple) {
        return std::nullopt;
    } else {
        return Visit(*nullableTuple);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TDerived>
TAliasVisitingAstVisitor<TDerived>::TAliasVisitingAstVisitor(const NAst::TAliasMap& aliasMap)
    : AliasMap_(aliasMap)
{ }

template <class TDerived>
void TAliasVisitingAstVisitor<TDerived>::OnReference(TReferenceExpressionPtr referenceExpr)
{
    if (referenceExpr->Reference.TableName) {
        return;
    }

    YT_ASSERT(referenceExpr->Reference.CompositeTypeAccessor.IsEmpty());

    const auto& name = referenceExpr->Reference.ColumnName;
    auto it = AliasMap_.find(name);
    if (it != AliasMap_.end()) {
        if (UsedAliases_.insert(name).second) {
            auto finally = Finally([&] {
                UsedAliases_.erase(name);
            });

            Visit(it->second);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient::NAst
