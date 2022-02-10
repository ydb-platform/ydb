#pragma once

#include "ast_nodes.h"

namespace NYql::NJsonPath {

class TJsonPathTypeChecker : public IAstNodeVisitor {
public:
    TJsonPathTypeChecker(TIssues& Issues);

    void VisitRoot(const TRootNode& node) override;

    void VisitContextObject(const TContextObjectNode& node) override;

    void VisitVariable(const TVariableNode& node) override;

    void VisitLastArrayIndex(const TLastArrayIndexNode& node) override;

    void VisitNumberLiteral(const TNumberLiteralNode& node) override;

    void VisitMemberAccess(const TMemberAccessNode& node) override;

    void VisitWildcardMemberAccess(const TWildcardMemberAccessNode& node) override;

    void VisitArrayAccess(const TArrayAccessNode& node) override;

    void VisitWildcardArrayAccess(const TWildcardArrayAccessNode& node) override;

    void VisitUnaryOperation(const TUnaryOperationNode& node) override;

    void VisitBinaryOperation(const TBinaryOperationNode& node) override;

    void VisitBooleanLiteral(const TBooleanLiteralNode& node) override;

    void VisitNullLiteral(const TNullLiteralNode& node) override;

    void VisitStringLiteral(const TStringLiteralNode& node) override;

    void VisitFilterObject(const TFilterObjectNode& node) override;

    void VisitFilterPredicate(const TFilterPredicateNode& node) override;

    void VisitMethodCall(const TMethodCallNode& node) override;

    void VisitStartsWithPredicate(const TStartsWithPredicateNode& node) override;

    void VisitExistsPredicate(const TExistsPredicateNode& node) override;

    void VisitIsUnknownPredicate(const TIsUnknownPredicateNode& node) override;

    void VisitLikeRegexPredicate(const TLikeRegexPredicateNode& node) override;

    void Error(const TAstNodePtr node, const TStringBuf message);

private:
    TIssues& Issues;
};

}