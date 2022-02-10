#include "type_check.h"

#include <ydb/library/yql/core/issue/protos/issue_id.pb.h>

namespace NYql::NJsonPath {

TJsonPathTypeChecker::TJsonPathTypeChecker(TIssues& issues)
    : Issues(issues)
{
}

void TJsonPathTypeChecker::VisitRoot(const TRootNode& node) {
    node.GetExpr()->Accept(*this);
}

void TJsonPathTypeChecker::VisitContextObject(const TContextObjectNode& node) {
    Y_UNUSED(node);
}

void TJsonPathTypeChecker::VisitVariable(const TVariableNode& node) {
    Y_UNUSED(node);
}

void TJsonPathTypeChecker::VisitLastArrayIndex(const TLastArrayIndexNode& node) {
    Y_UNUSED(node);
}

void TJsonPathTypeChecker::VisitNumberLiteral(const TNumberLiteralNode& node) {
    Y_UNUSED(node);
}

void TJsonPathTypeChecker::VisitMemberAccess(const TMemberAccessNode& node) {
    node.GetInput()->Accept(*this);
}

void TJsonPathTypeChecker::VisitWildcardMemberAccess(const TWildcardMemberAccessNode& node) {
    node.GetInput()->Accept(*this);
}

void TJsonPathTypeChecker::VisitArrayAccess(const TArrayAccessNode& node) {
    node.GetInput()->Accept(*this);

    for (const auto& subscript : node.GetSubscripts()) {
        subscript.From->Accept(*this);
        if (subscript.To) {
            subscript.To->Accept(*this);
        }
    }
}

void TJsonPathTypeChecker::VisitWildcardArrayAccess(const TWildcardArrayAccessNode& node) {
    node.GetInput()->Accept(*this);
}

void TJsonPathTypeChecker::VisitUnaryOperation(const TUnaryOperationNode& node) {
    if (node.GetOp() == EUnaryOperation::Not && node.GetExpr()->GetReturnType() != EReturnType::Bool) {
        Error(node.GetExpr(), "Logical not needs boolean argument");
    }

    node.GetExpr()->Accept(*this);
}

void TJsonPathTypeChecker::VisitBinaryOperation(const TBinaryOperationNode& node) {
    if (node.GetOp() == EBinaryOperation::And || node.GetOp() == EBinaryOperation::Or) {
        if (node.GetLeftExpr()->GetReturnType() != EReturnType::Bool) {
            Error(node.GetLeftExpr(), "Left argument of logical operation needs to be boolean");
        }
        if (node.GetRightExpr()->GetReturnType() != EReturnType::Bool) {
            Error(node.GetRightExpr(), "Right argument of logical operation needs to be boolean");
        }
    }

    node.GetLeftExpr()->Accept(*this);
    node.GetRightExpr()->Accept(*this);
}

void TJsonPathTypeChecker::VisitBooleanLiteral(const TBooleanLiteralNode& node) {
    Y_UNUSED(node);
}

void TJsonPathTypeChecker::VisitNullLiteral(const TNullLiteralNode& node) {
    Y_UNUSED(node);
}

void TJsonPathTypeChecker::VisitStringLiteral(const TStringLiteralNode& node) {
    Y_UNUSED(node);
}

void TJsonPathTypeChecker::VisitFilterObject(const TFilterObjectNode& node) {
    Y_UNUSED(node);
}

void TJsonPathTypeChecker::VisitFilterPredicate(const TFilterPredicateNode& node) {
    node.GetInput()->Accept(*this);

    if (node.GetPredicate()->GetReturnType() != EReturnType::Bool) {
        Error(node.GetPredicate(), "Filter must return boolean value");
    }

    node.GetPredicate()->Accept(*this);
}

void TJsonPathTypeChecker::VisitMethodCall(const TMethodCallNode& node) {
    node.GetInput()->Accept(*this);
}

void TJsonPathTypeChecker::VisitStartsWithPredicate(const TStartsWithPredicateNode& node) {
    node.GetInput()->Accept(*this);
    node.GetPrefix()->Accept(*this);
}

void TJsonPathTypeChecker::VisitExistsPredicate(const TExistsPredicateNode& node) {
    node.GetInput()->Accept(*this);
}

void TJsonPathTypeChecker::VisitIsUnknownPredicate(const TIsUnknownPredicateNode& node) {
    if (node.GetInput()->GetReturnType() != EReturnType::Bool) {
        Error(node.GetInput(), "is unknown predicate expectes boolean argument");
    }
    node.GetInput()->Accept(*this);
}

void TJsonPathTypeChecker::VisitLikeRegexPredicate(const TLikeRegexPredicateNode& node) {
    node.GetInput()->Accept(*this);
}

void TJsonPathTypeChecker::Error(const TAstNodePtr node, const TStringBuf message) {
    Issues.AddIssue(node->GetPos(), message);
    Issues.back().SetCode(TIssuesIds::JSONPATH_TYPE_CHECK_ERROR, TSeverityIds::S_ERROR);
}

}
