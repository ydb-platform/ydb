#pragma once

#include "ast.h"
#include "query.h"

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

template <class TResult, class TDerived, class TNode, class... TArgs>
struct TAbstractVisitor
{
    TDerived* Derived()
    {
        return static_cast<TDerived*>(this);
    }

    TResult Visit(TNode node, TArgs... args)
    {
        auto expr = Derived()->GetExpression(node);

        if (auto literalExpr = expr->template As<TLiteralExpression>()) {
            return Derived()->OnLiteral(literalExpr, args...);
        } else if (auto referenceExpr = expr->template As<TReferenceExpression>()) {
            return Derived()->OnReference(referenceExpr, args...);
        } else if (auto unaryOp = expr->template As<TUnaryOpExpression>()) {
            return Derived()->OnUnary(unaryOp, args...);
        } else if (auto binaryOp = expr->template As<TBinaryOpExpression>()) {
            return Derived()->OnBinary(binaryOp, args...);
        } else if (auto functionExpr = expr->template As<TFunctionExpression>()) {
            return Derived()->OnFunction(functionExpr, args...);
        } else if (auto inExpr = expr->template As<TInExpression>()) {
            return Derived()->OnIn(inExpr, args...);
        } else if (auto betweenExpr = expr->template As<TBetweenExpression>()) {
            return Derived()->OnBetween(betweenExpr, args...);
        } else if (auto transformExpr = expr->template As<TTransformExpression>()) {
            return Derived()->OnTransform(transformExpr, args...);
        } else if (auto caseExpr = expr->template As<TCaseExpression>()) {
            return Derived()->OnCase(caseExpr, args...);
        } else if (auto likeExpr = expr->template As<TLikeExpression>()) {
            return Derived()->OnLike(likeExpr, args...);
        } else if (auto memberAccessorExpr = expr->template As<TCompositeMemberAccessorExpression>()) {
            return Derived()->OnCompositeMemberAccessor(memberAccessorExpr, args...);
        } else if (auto subqueryExpr = expr->template As<TSubqueryExpression>()) {
            return Derived()->OnSubquery(subqueryExpr, args...);
        }
        YT_ABORT();
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TResult, class TDerived>
struct TBaseVisitor
    : TAbstractVisitor<TResult, TDerived, TConstExpressionPtr>
{
    const TExpression* GetExpression(const TConstExpressionPtr& expr)
    {
        return &*expr;
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TDerived>
struct TVisitor
    : public TBaseVisitor<void, TDerived>
{
    using TBase = TBaseVisitor<void, TDerived>;
    using TBase::Derived;
    using TBase::Visit;

    void OnLiteral(const TLiteralExpression* /*literalExpr*/)
    { }

    void OnReference(const TReferenceExpression* /*referenceExpr*/)
    { }

    void OnUnary(const TUnaryOpExpression* unaryExpr)
    {
        Visit(unaryExpr->Operand);
    }

    void OnBinary(const TBinaryOpExpression* binaryExpr)
    {
        Visit(binaryExpr->Lhs);
        Visit(binaryExpr->Rhs);
    }

    void OnFunction(const TFunctionExpression* functionExpr)
    {
        for (const auto& argument : functionExpr->Arguments) {
            Visit(argument);
        }
    }

    void OnIn(const TInExpression* inExpr)
    {
        for (const auto& argument : inExpr->Arguments) {
            Visit(argument);
        }
    }

    void OnBetween(const TBetweenExpression* betweenExpr)
    {
        for (const auto& argument : betweenExpr->Arguments) {
            Visit(argument);
        }
    }

    void OnTransform(const TTransformExpression* transformExpr)
    {
        for (const auto& argument : transformExpr->Arguments) {
            Visit(argument);
        }
    }

    void OnCase(const TCaseExpression* caseExpr)
    {
        for (const auto& whenThenExpression : caseExpr->WhenThenExpressions) {
            Visit(whenThenExpression->Condition);
            Visit(whenThenExpression->Result);
        }
    }

    void OnLike(const TLikeExpression* likeExpr)
    {
        Visit(likeExpr->Text);
        Visit(likeExpr->Pattern);
        if (likeExpr->EscapeCharacter) {
            Visit(likeExpr->EscapeCharacter);
        }
    }

    void OnCompositeMemberAccessor(const TCompositeMemberAccessorExpression* memberAccessorExpr)
    {
        Visit(memberAccessorExpr->CompositeExpression);
    }

    void OnSubquery(const TSubqueryExpression* subqueryExpr)
    {
        for (const auto& [expression, name] : subqueryExpr->FromExpressions) {
            Visit(expression);
        }

        for (const auto& joinClause : subqueryExpr->JoinClauses) {
            for (const auto& equation : joinClause->SelfEquations) {
                Visit(equation);
            }

            for (const auto& equation : joinClause->ForeignEquations) {
                Visit(equation);
            }

            if (joinClause->Predicate) {
                Visit(joinClause->Predicate);
            }
        }

        if (subqueryExpr->WhereClause) {
            Visit(subqueryExpr->WhereClause);
        }

        if (subqueryExpr->GroupClause) {
            for (const auto& [expression, name] : subqueryExpr->GroupClause->GroupItems) {
                Visit(expression);
            }

            for (const auto& aggregateItem : subqueryExpr->GroupClause->AggregateItems) {
                for (const auto& argument : aggregateItem.Arguments) {
                    Visit(argument);
                }
            }
        }

        if (subqueryExpr->ProjectClause) {
            for (const auto& [expression, name] : subqueryExpr->ProjectClause->Projections) {
                Visit(expression);
            }
        }

        if (subqueryExpr->OrderClause) {
            for (const auto& orderItem : subqueryExpr->OrderClause->OrderItems) {
                Visit(orderItem.Expression);
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TDerived>
struct TRewriter
    : public TBaseVisitor<TConstExpressionPtr, TDerived>
{
    using TBase = TBaseVisitor<TConstExpressionPtr, TDerived>;
    using TBase::Derived;
    using TBase::Visit;

    TConstExpressionPtr OnLiteral(const TLiteralExpression* literalExpr)
    {
        return literalExpr;
    }

    TConstExpressionPtr OnReference(const TReferenceExpression* referenceExpr)
    {
        return referenceExpr;
    }

    TConstExpressionPtr OnUnary(const TUnaryOpExpression* unaryExpr)
    {
        auto newOperand = Visit(unaryExpr->Operand);

        if (newOperand == unaryExpr->Operand) {
            return unaryExpr;
        }

        return New<TUnaryOpExpression>(
            unaryExpr->GetWireType(),
            unaryExpr->Opcode,
            newOperand);
    }

    TConstExpressionPtr OnBinary(const TBinaryOpExpression* binaryExpr)
    {
        auto newLhs = Visit(binaryExpr->Lhs);
        auto newRhs = Visit(binaryExpr->Rhs);

        if (newLhs == binaryExpr->Lhs && newRhs == binaryExpr->Rhs) {
            return binaryExpr;
        }

        return New<TBinaryOpExpression>(
            binaryExpr->GetWireType(),
            binaryExpr->Opcode,
            newLhs,
            newRhs);
    }

    TConstExpressionPtr OnFunction(const TFunctionExpression* functionExpr)
    {
        std::vector<TConstExpressionPtr> newArguments;
        bool allEqual = true;
        for (auto argument : functionExpr->Arguments) {
            auto newArgument = Visit(argument);
            allEqual = allEqual && newArgument == argument;
            newArguments.push_back(newArgument);
        }

        if (allEqual) {
            return functionExpr;
        }

        return New<TFunctionExpression>(
            functionExpr->LogicalType,
            functionExpr->FunctionName,
            std::move(newArguments));
    }

    TConstExpressionPtr OnIn(const TInExpression* inExpr)
    {
        std::vector<TConstExpressionPtr> newArguments;
        bool allEqual = true;
        for (auto argument : inExpr->Arguments) {
            auto newArgument = Visit(argument);
            allEqual = allEqual && newArgument == argument;
            newArguments.push_back(newArgument);
        }

        if (allEqual) {
            return inExpr;
        }

        return New<TInExpression>(
            std::move(newArguments),
            inExpr->Values);
    }

    TConstExpressionPtr OnBetween(const TBetweenExpression* betweenExpr)
    {
        std::vector<TConstExpressionPtr> newArguments;
        bool allEqual = true;
        for (auto argument : betweenExpr->Arguments) {
            auto newArgument = Visit(argument);
            allEqual = allEqual && newArgument == argument;
            newArguments.push_back(newArgument);
        }

        if (allEqual) {
            return betweenExpr;
        }

        return New<TBetweenExpression>(
            std::move(newArguments),
            betweenExpr->Ranges);
    }

    TConstExpressionPtr OnTransform(const TTransformExpression* transformExpr)
    {
        std::vector<TConstExpressionPtr> newArguments;
        bool allEqual = true;
        for (auto argument : transformExpr->Arguments) {
            auto newArgument = Visit(argument);
            allEqual = allEqual && newArgument == argument;
            newArguments.push_back(newArgument);
        }

        TConstExpressionPtr newDefaultExpression;
        if (const auto& defaultExpression = transformExpr->DefaultExpression) {
            newDefaultExpression = Visit(defaultExpression);
            allEqual = allEqual && newDefaultExpression == defaultExpression;
        }

        if (allEqual) {
            return transformExpr;
        }

        return New<TTransformExpression>(
            transformExpr->GetWireType(),
            std::move(newArguments),
            transformExpr->Values,
            newDefaultExpression);
    }

    TConstExpressionPtr OnCase(const TCaseExpression* caseExpr)
    {
        bool allEqual = true;

        TConstExpressionPtr newOptionalOperand;
        if (const auto& optionalOperand = caseExpr->OptionalOperand) {
            newOptionalOperand = Visit(optionalOperand);
            allEqual = allEqual && newOptionalOperand == optionalOperand;
        }

        std::vector<TWhenThenExpressionPtr> newWhenThenExpressions;
        for (auto& caseClause : caseExpr->WhenThenExpressions) {
            auto newCondition = Visit(caseClause->Condition);
            allEqual = allEqual && newCondition == caseClause->Condition;

            auto newResult = Visit(caseClause->Result);
            allEqual = allEqual && newResult == caseClause->Result;

            newWhenThenExpressions.emplace_back(New<TWhenThenExpression>(newCondition, newResult));
        }

        TConstExpressionPtr newDefault;
        if (const auto& defaultExpression = caseExpr->DefaultExpression) {
            newDefault = Visit(defaultExpression);
            allEqual = allEqual && newDefault == defaultExpression;
        }

        if (allEqual) {
            return caseExpr;
        }

        return New<TCaseExpression>(
            caseExpr->GetWireType(),
            std::move(newOptionalOperand),
            std::move(newWhenThenExpressions),
            newDefault);
    }

    TConstExpressionPtr OnLike(const TLikeExpression* likeExpr)
    {
        bool allEqual = true;

        auto newText = Visit(likeExpr->Text);
        allEqual = allEqual && newText == likeExpr->Text;

        auto newPattern = Visit(likeExpr->Pattern);
        allEqual = allEqual && newPattern == likeExpr->Pattern;

        TConstExpressionPtr newEscapeCharacter;
        if (const auto& escapeCharacter = likeExpr->EscapeCharacter) {
            newEscapeCharacter = Visit(likeExpr->EscapeCharacter);
            allEqual = allEqual && newEscapeCharacter == likeExpr->EscapeCharacter;
        }

        if (allEqual) {
            return likeExpr;
        }

        return New<TLikeExpression>(
            std::move(newText),
            likeExpr->Opcode,
            std::move(newPattern),
            std::move(newEscapeCharacter));
    }

    TConstExpressionPtr OnCompositeMemberAccessor(const TCompositeMemberAccessorExpression* memberAccessorExpr)
    {
        auto newCompositeExpression = Visit(memberAccessorExpr->CompositeExpression);
        bool allEqual = newCompositeExpression == memberAccessorExpr->CompositeExpression;

        auto newDictOrListItemAccessor = TCompositeMemberAccessorExpression::TDictOrListItemAccessorExpression{};
        if (memberAccessorExpr->DictOrListItemAccessor) {
            newDictOrListItemAccessor = Visit(memberAccessorExpr->DictOrListItemAccessor);
            allEqual = allEqual && newDictOrListItemAccessor == memberAccessorExpr->DictOrListItemAccessor;
        }

        if (allEqual) {
            return memberAccessorExpr;
        }

        return New<TCompositeMemberAccessorExpression>(
            memberAccessorExpr->LogicalType,
            std::move(newCompositeExpression),
            memberAccessorExpr->NestedStructOrTupleItemAccessor,
            std::move(newDictOrListItemAccessor));
    }

    TConstExpressionPtr OnSubquery(const TSubqueryExpression* subqueryExpr)
    {
        return subqueryExpr;
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TDerived, class TNode, class... TArgs>
struct TAbstractExpressionPrinter
    : TAbstractVisitor<void, TDerived, TNode, TArgs...>
{
    using TBase = TAbstractVisitor<void, TDerived, TNode, TArgs...>;
    using TBase::Derived;
    using TBase::Visit;

    TStringBuilderBase* Builder;
    TInferNameOptions Options;

    TAbstractExpressionPrinter(TStringBuilderBase* builder, const TInferNameOptions& options)
        : Builder(builder)
        , Options(options)
    { }

    static int GetOpPriority(EBinaryOp op)
    {
        switch (op) {
        case EBinaryOp::Multiply:
        case EBinaryOp::Divide:
        case EBinaryOp::Modulo:
            return 0;

        case EBinaryOp::Plus:
        case EBinaryOp::Minus:
        case EBinaryOp::Concatenate:
            return 1;

        case EBinaryOp::LeftShift:
        case EBinaryOp::RightShift:
            return 2;

        case EBinaryOp::BitAnd:
            return 3;

        case EBinaryOp::BitOr:
            return 4;

        case EBinaryOp::Equal:
        case EBinaryOp::NotEqual:
        case EBinaryOp::Less:
        case EBinaryOp::LessOrEqual:
        case EBinaryOp::Greater:
        case EBinaryOp::GreaterOrEqual:
            return 5;

        case EBinaryOp::And:
            return 6;

        case EBinaryOp::Or:
            return 7;

        default:
            YT_ABORT();
        }
    }

    // TODO(lukyan): Provide parent operator priority in arguments and wrap expression if needed.
    static bool CanOmitParenthesis(TConstExpressionPtr expr)
    {
        return
            expr->As<TLiteralExpression>() ||
            expr->As<TReferenceExpression>() ||
            expr->As<TFunctionExpression>() ||
            expr->As<TUnaryOpExpression>() ||
            expr->As<TTransformExpression>() ||
            expr->As<TCaseExpression>() ||
            expr->As<TLikeExpression>() ||
            expr->As<TCompositeMemberAccessorExpression>();
    }

    const TExpression* GetExpression(const TConstExpressionPtr& expr)
    {
        return &*expr;
    }

    void OnOperand(const TUnaryOpExpression* unaryExpr, TArgs... args)
    {
        Visit(unaryExpr->Operand, args...);
    }

    void OnLhs(const TBinaryOpExpression* binaryExpr, TArgs... args)
    {
        Visit(binaryExpr->Lhs, args...);
    }

    void OnRhs(const TBinaryOpExpression* binaryExpr, TArgs... args)
    {
        Visit(binaryExpr->Rhs, args...);
    }

    void OnDefaultExpression(const TTransformExpression* transformExpr, TArgs... args)
    {
        if (const auto& defaultExpression = transformExpr->DefaultExpression) {
            Builder->AppendString(", ");
            Visit(defaultExpression, args...);
        }
    }

    void OnOptionalOperand(const TCaseExpression* caseExpr, TArgs... args)
    {
        if (const auto& optionalOperand = caseExpr->OptionalOperand) {
            Builder->AppendChar(' ');
            Visit(optionalOperand, args...);
        }
    }

    void OnWhenThenExpressions(const TCaseExpression* caseExpr, TArgs... args)
    {
        for (const auto& caseClause : caseExpr->WhenThenExpressions) {
            Builder->AppendString(" WHEN ");
            Visit(caseClause->Condition, args...);
            Builder->AppendString(" THEN ");
            Visit(caseClause->Result, args...);
        }
    }

    void OnDefaultExpression(const TCaseExpression* caseExpr, TArgs... args)
    {
        if (const auto& defaultExpression = caseExpr->DefaultExpression) {
            Builder->AppendString(" ELSE ");
            Visit(defaultExpression, args...);
        }
    }

    void OnLikeText(const TLikeExpression* likeExpr, TArgs... args)
    {
        Visit(likeExpr->Text, args...);
    }

    void OnLikePattern(const TLikeExpression* likeExpr, TArgs... args)
    {
        Visit(likeExpr->Pattern, args...);
    }

    void OnLikeEscapeCharacter(const TLikeExpression* likeExpr, TArgs... args)
    {
        Visit(likeExpr->EscapeCharacter, args...);
    }

    void OnCompositeMemberAccessorColumnReference(const TCompositeMemberAccessorExpression* memberAccessorExpr, TArgs... args)
    {
        Visit(memberAccessorExpr->CompositeExpression, args...);
    }

    template <class T>
    void OnArguments(const T* expr, TArgs... args)
    {
        bool needComma = false;
        for (const auto& argument : expr->Arguments) {
            if (needComma) {
                Builder->AppendString(", ");
            }
            Visit(argument, args...);
            needComma = true;
        }
    }

    void OnLiteral(const TLiteralExpression* literalExpr, TArgs... /*args*/)
    {
        if (Options.OmitValues) {
            Builder->AppendChar('?');
        } else {
            Builder->AppendString(ToString(static_cast<TValue>(literalExpr->Value)));
        }
    }

    void OnReference(const TReferenceExpression* referenceExpr, TArgs... args)
    {
        auto& groupClause = Options.GroupClause;
        if (groupClause) {
            if (auto it = std::find_if(
                    groupClause->GroupItems.begin(),
                    groupClause->GroupItems.end(),
                    [&] (const TNamedItem& item) { return item.Name == referenceExpr->ColumnName; });
                it != groupClause->GroupItems.end())
            {
                auto groupClauseHolder = std::move(groupClause);
                Visit(it->Expression, args...);
                groupClause = std::move(groupClauseHolder);
                return;
            }

            if (auto it = std::find_if(
                    groupClause->AggregateItems.begin(),
                    groupClause->AggregateItems.end(),
                    [&] (const TAggregateItem& item) { return item.Name == referenceExpr->ColumnName; });
                it != groupClause->AggregateItems.end())
            {
                auto groupClauseHolder = std::move(groupClause);
                Builder->AppendString(it->AggregateFunction);
                Builder->AppendChar('(');
                Derived()->OnArguments(&*it, args...);
                Builder->AppendChar(')');
                groupClause = std::move(groupClauseHolder);
                return;
            }
        }
        Builder->AppendString(NAst::FormatId(referenceExpr->ColumnName));
    }

    void OnUnary(const TUnaryOpExpression* unaryExpr, TArgs... args)
    {
        Builder->AppendString(GetUnaryOpcodeLexeme(unaryExpr->Opcode));
        Builder->AppendChar(' ');

        auto needParenthesis = !CanOmitParenthesis(unaryExpr->Operand);
        if (needParenthesis) {
            Builder->AppendChar('(');
        }
        Derived()->OnOperand(unaryExpr, args...);
        if (needParenthesis) {
            Builder->AppendChar(')');
        }
    }

    void OnBinary(const TBinaryOpExpression* binaryExpr, TArgs... args)
    {
        auto needParenthesisLhs = !CanOmitParenthesis(binaryExpr->Lhs);
        if (needParenthesisLhs) {
            if (const auto* lhs = binaryExpr->Lhs->As<TBinaryOpExpression>()) {
                if (GetOpPriority(lhs->Opcode) <= GetOpPriority(binaryExpr->Opcode)) {
                    needParenthesisLhs = false;
                }
            }
        }

        if (needParenthesisLhs) {
            Builder->AppendChar('(');
        }
        Derived()->OnLhs(binaryExpr, args...);
        if (needParenthesisLhs) {
            Builder->AppendChar(')');
        }

        Builder->AppendChar(' ');
        Builder->AppendString(GetBinaryOpcodeLexeme(binaryExpr->Opcode));
        Builder->AppendChar(' ');

        auto needParenthesisRhs = !CanOmitParenthesis(binaryExpr->Rhs);
        if (needParenthesisRhs) {
            if (const auto* rhs = binaryExpr->Rhs->As<TBinaryOpExpression>()) {
                if (GetOpPriority(rhs->Opcode) <= GetOpPriority(binaryExpr->Opcode)) {
                    needParenthesisRhs = false;
                }
            }
        }

        if (needParenthesisRhs) {
            Builder->AppendChar('(');
        }
        Derived()->OnRhs(binaryExpr, args...);
        if (needParenthesisRhs) {
            Builder->AppendChar(')');
        }
    }

    void OnFunction(const TFunctionExpression* functionExpr, TArgs... args)
    {
        Builder->AppendString(functionExpr->FunctionName);
        Builder->AppendChar('(');
        Derived()->OnArguments(functionExpr, args...);
        Builder->AppendChar(')');
    }

    void OnIn(const TInExpression* inExpr, TArgs... args)
    {
        auto needParenthesis = inExpr->Arguments.size() > 1;
        if (needParenthesis) {
            Builder->AppendChar('(');
        }
        Derived()->OnArguments(inExpr, args...);
        if (needParenthesis) {
            Builder->AppendChar(')');
        }

        Builder->AppendString(" IN (");

        if (Options.OmitValues) {
            Builder->AppendString("??");
        } else {
            JoinToString(
                Builder,
                inExpr->Values.begin(),
                inExpr->Values.end(),
                [&] (TStringBuilderBase* builder, const TRow& row) {
                    builder->AppendString(ToString(row));
                });
        }
        Builder->AppendChar(')');
    }

    void OnBetween(const TBetweenExpression* betweenExpr, TArgs... args)
    {
        auto needParenthesis = betweenExpr->Arguments.size() > 1;
        if (needParenthesis) {
            Builder->AppendChar('(');
        }
        Derived()->OnArguments(betweenExpr, args...);
        if (needParenthesis) {
            Builder->AppendChar(')');
        }

        Builder->AppendString(" BETWEEN (");

        if (Options.OmitValues) {
            Builder->AppendString("??");
        } else {
            JoinToString(
                Builder,
                betweenExpr->Ranges.begin(),
                betweenExpr->Ranges.end(),
                [&] (TStringBuilderBase* builder, const TRowRange& range) {
                    builder->AppendString(ToString(range.first));
                    builder->AppendString(" AND ");
                    builder->AppendString(ToString(range.second));
                });
        }
        Builder->AppendChar(')');
    }

    void OnTransform(const TTransformExpression* transformExpr, TArgs... args)
    {
        Builder->AppendString("TRANSFORM(");
        size_t argumentCount = transformExpr->Arguments.size();
        auto needParenthesis = argumentCount > 1;
        if (needParenthesis) {
            Builder->AppendChar('(');
        }
        Derived()->OnArguments(transformExpr, args...);
        if (needParenthesis) {
            Builder->AppendChar(')');
        }

        Builder->AppendString(", (");
        if (Options.OmitValues) {
            Builder->AppendString("??");
        } else {
            JoinToString(
                Builder,
                transformExpr->Values.begin(),
                transformExpr->Values.end(),
                [&] (TStringBuilderBase* builder, const TRow& row) {
                    builder->AppendChar('[');
                    JoinToString(
                        builder,
                        row.Begin(),
                        row.Begin() + argumentCount,
                        [] (TStringBuilderBase* builder, const TValue& value) {
                            builder->AppendString(ToString(value));
                        });
                    builder->AppendChar(']');
                });
        }
        Builder->AppendString("), (");

        if (Options.OmitValues) {
            Builder->AppendString("??");
        } else {
            JoinToString(
                Builder,
                transformExpr->Values.begin(),
                transformExpr->Values.end(),
                [&] (TStringBuilderBase* builder, const TRow& row) {
                    builder->AppendString(ToString(row[argumentCount]));
                });
        }

        Builder->AppendChar(')');

        Derived()->OnDefaultExpression(transformExpr, args...);

        Builder->AppendChar(')');
    }

    void OnCase(const TCaseExpression* caseExpr, TArgs... args)
    {
        Builder->AppendString("CASE");
        Derived()->OnOptionalOperand(caseExpr, args...);
        Derived()->OnWhenThenExpressions(caseExpr, args...);
        Derived()->OnDefaultExpression(caseExpr, args...);
        Builder->AppendString(" END");
    }

    void OnLike(const TLikeExpression* likeExpr, TArgs... args)
    {
        Derived()->OnLikeText(likeExpr, args...);

        Builder->AppendChar(' ');
        Builder->AppendString(GetStringMatchOpcodeLexeme(likeExpr->Opcode));
        Builder->AppendChar(' ');

        Derived()->OnLikePattern(likeExpr, args...);

        if (likeExpr->EscapeCharacter) {
            YT_ASSERT(likeExpr->Opcode != EStringMatchOp::Regex);
            Builder->AppendString(" ESCAPE ");
            Derived()->OnLikeEscapeCharacter(likeExpr, args...);
        }
    }

    void OnCompositeMemberAccessor(const TCompositeMemberAccessorExpression* memberAccessorExpr, TArgs... args)
    {
        Derived()->OnCompositeMemberAccessorColumnReference(memberAccessorExpr, args...);
    }

    void OnSubquery(const TSubqueryExpression* subqueryExpr, TArgs... /*args*/)
    {
        auto inferNameOptions = TInferNameOptions{
            .OmitValues = Options.OmitValues,
            .OmitOffsetAndLimit = Options.OmitOffsetAndLimit,
            .GroupClause = subqueryExpr->GroupClause,
        };
        auto namedItemFormatter = [&] (TStringBuilderBase* builder, const TNamedItem& item) {
            builder->AppendString(InferName(item.Expression, inferNameOptions));
            builder->AppendFormat(" AS %v", item.Name);
        };

        std::vector<std::string> clauses;
        std::string str;

        if (subqueryExpr->ProjectClause) {
            str = JoinToString(subqueryExpr->ProjectClause->Projections, namedItemFormatter);
        } else {
            str = "*";
        }

        clauses.emplace_back("SELECT " + str);

        clauses.push_back(std::string("FROM (") + JoinToString(subqueryExpr->FromExpressions, namedItemFormatter) + ")");

        for (const auto& joinClause : subqueryExpr->JoinClauses) {
            auto selfEquations = std::vector<std::string>();
            {
                selfEquations.reserve(joinClause->SelfEquations.size());
                for (const auto& equation : joinClause->SelfEquations) {
                    selfEquations.push_back(InferName(equation, inferNameOptions));
                }
            }

            auto foreignEquations = std::vector<std::string>();
            {
                foreignEquations.reserve(joinClause->ForeignEquations.size());
                for (const auto& equation : joinClause->ForeignEquations) {
                    foreignEquations.push_back(InferName(equation, inferNameOptions));
                }
            }

            clauses.push_back(Format(
                "%v JOIN [common prefix: %v, foreign prefix: %v] ON (%v) = (%v)",
                joinClause->IsLeft ? "LEFT" : "INNER",
                joinClause->CommonKeyPrefix,
                joinClause->ForeignKeyPrefix,
                JoinToString(selfEquations),
                JoinToString(foreignEquations)));

            if (joinClause->Predicate) {
                clauses.push_back("AND " + InferName(joinClause->Predicate, inferNameOptions));
            }
        }

        if (subqueryExpr->WhereClause) {
            clauses.push_back(std::string("WHERE ") + InferName(subqueryExpr->WhereClause, inferNameOptions));
        }

        inferNameOptions.GroupClause.Reset();

        if (subqueryExpr->GroupClause) {
            auto namedItemFormatter = [&] (TStringBuilderBase* builder, const TNamedItem& item) {
                builder->AppendString(InferName(item.Expression, inferNameOptions));
            };
            clauses.push_back(Format("GROUP BY [common prefix: %v, aggregates: %v] %v",
                subqueryExpr->GroupClause->CommonPrefixWithPrimaryKey,
                MakeFormattableView(subqueryExpr->GroupClause->AggregateItems, [] (auto* builder, const auto& item) {
                    builder->AppendString(item.AggregateFunction);
                }),
                JoinToString(subqueryExpr->GroupClause->GroupItems, namedItemFormatter)));
            if (subqueryExpr->GroupClause->TotalsMode == ETotalsMode::BeforeHaving) {
                clauses.push_back("WITH TOTALS");
            }
        }

        if (subqueryExpr->OrderClause) {
            inferNameOptions.GroupClause = subqueryExpr->GroupClause;
            auto orderItemFormatter = [&] (TStringBuilderBase* builder, const TOrderItem& item) {
                builder->AppendString(InferName(item.Expression, inferNameOptions));
                builder->AppendString(item.Descending ? " DESC" : " ASC");
            };
            clauses.push_back("ORDER BY " + JoinToString(subqueryExpr->OrderClause->OrderItems, orderItemFormatter));
        }

        if (!Options.OmitOffsetAndLimit && subqueryExpr->Offset != 0) {
            clauses.push_back(std::string("OFFSET ") + (Options.OmitValues ? "?" : ToString(subqueryExpr->Offset)));
        }

        if (!Options.OmitOffsetAndLimit && subqueryExpr->Limit) {
            clauses.push_back(std::string("LIMIT ") + (Options.OmitValues ? "?" : ToString(*subqueryExpr->Limit)));
        }

        Builder->AppendChar('(');
        Builder->AppendString(JoinToString(clauses, TStringBuf(" ")));
        Builder->AppendChar(')');
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TSelfifyRewriter
    : public TRewriter<TSelfifyRewriter>
{
    const std::vector<TConstExpressionPtr>& SelfEquations;
    const THashMap<std::string, int>& ForeignReferenceToIndexMap;
    bool Success = true;

    TConstExpressionPtr OnReference(const TReferenceExpression* reference);
};

////////////////////////////////////////////////////////////////////////////////

struct TAddAliasRewriter
    : public TRewriter<TAddAliasRewriter>
{
    const std::optional<std::string>& Alias;

    TConstExpressionPtr OnReference(const TReferenceExpression* reference);
};

////////////////////////////////////////////////////////////////////////////////

class TReferenceHarvester
    : public TVisitor<TReferenceHarvester>
{
public:
    explicit TReferenceHarvester(TColumnSet* storage);

    void OnReference(const TReferenceExpression* referenceExpr);

private:
    TColumnSet* const Storage_;
};

////////////////////////////////////////////////////////////////////////////////

class TReferenceReplacer
    : public TRewriter<TReferenceReplacer>
{
public:
    TReferenceReplacer(TStringBuf from, const TReferenceExpression* to);

    TConstExpressionPtr OnReference(const TReferenceExpression* referenceExpr);

private:
    const TStringBuf From_;
    const TReferenceExpression* To_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
