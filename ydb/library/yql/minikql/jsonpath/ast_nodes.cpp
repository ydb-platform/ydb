#include "ast_nodes.h"

namespace NYql::NJsonPath {

TAstNode::TAstNode(TPosition pos)
    : Pos(pos)
{
}

TPosition TAstNode::GetPos() const {
    return Pos;
}

EReturnType TAstNode::GetReturnType() const {
    return EReturnType::Any;
}

TRootNode::TRootNode(TPosition pos, TAstNodePtr expr, EJsonPathMode mode)
    : TAstNode(pos)
    , Expr(expr)
    , Mode(mode)
{
}

const TAstNodePtr TRootNode::GetExpr() const {
    return Expr;
}

EJsonPathMode TRootNode::GetMode() const {
    return Mode;
}

void TRootNode::Accept(IAstNodeVisitor& visitor) const {
    return visitor.VisitRoot(*this);
}

EReturnType TRootNode::GetReturnType() const {
    return Expr->GetReturnType();
}

TContextObjectNode::TContextObjectNode(TPosition pos)
    : TAstNode(pos)
{
}

void TContextObjectNode::Accept(IAstNodeVisitor& visitor) const {
    return visitor.VisitContextObject(*this);
}

TVariableNode::TVariableNode(TPosition pos, const TString& name)
    : TAstNode(pos)
    , Name(name)
{
}

const TString& TVariableNode::GetName() const {
    return Name;
}

void TVariableNode::Accept(IAstNodeVisitor& visitor) const {
    visitor.VisitVariable(*this);
}

TLastArrayIndexNode::TLastArrayIndexNode(TPosition pos)
    : TAstNode(pos)
{
}

void TLastArrayIndexNode::Accept(IAstNodeVisitor& visitor) const {
    visitor.VisitLastArrayIndex(*this);
}

TNumberLiteralNode::TNumberLiteralNode(TPosition pos, double value)
    : TAstNode(pos)
    , Value(value)
{
}

double TNumberLiteralNode::GetValue() const {
    return Value;
}

void TNumberLiteralNode::Accept(IAstNodeVisitor& visitor) const {
    return visitor.VisitNumberLiteral(*this);
}

TMemberAccessNode::TMemberAccessNode(TPosition pos, const TString& member, TAstNodePtr input)
    : TAstNode(pos)
    , Member(member)
    , Input(input)
{
}

const TStringBuf TMemberAccessNode::GetMember() const {
    return Member;
}

const TAstNodePtr TMemberAccessNode::GetInput() const {
    return Input;
}

void TMemberAccessNode::Accept(IAstNodeVisitor& visitor) const {
    return visitor.VisitMemberAccess(*this);
}

TWildcardMemberAccessNode::TWildcardMemberAccessNode(TPosition pos, TAstNodePtr input)
    : TAstNode(pos)
    , Input(input)
{
}

const TAstNodePtr TWildcardMemberAccessNode::GetInput() const {
    return Input;
}

void TWildcardMemberAccessNode::Accept(IAstNodeVisitor& visitor) const {
    return visitor.VisitWildcardMemberAccess(*this);
}

TArrayAccessNode::TArrayAccessNode(TPosition pos, TVector<TSubscript> subscripts, TAstNodePtr input)
    : TAstNode(pos)
    , Subscripts(subscripts)
    , Input(input)
{
}

const TVector<TArrayAccessNode::TSubscript>& TArrayAccessNode::GetSubscripts() const {
    return Subscripts;
}

const TAstNodePtr TArrayAccessNode::GetInput() const {
    return Input;
}

void TArrayAccessNode::Accept(IAstNodeVisitor& visitor) const {
    return visitor.VisitArrayAccess(*this);
}

TWildcardArrayAccessNode::TWildcardArrayAccessNode(TPosition pos, TAstNodePtr input)
    : TAstNode(pos)
    , Input(input)
{
}

const TAstNodePtr TWildcardArrayAccessNode::GetInput() const {
    return Input;
}

void TWildcardArrayAccessNode::Accept(IAstNodeVisitor& visitor) const {
    return visitor.VisitWildcardArrayAccess(*this);
}

TUnaryOperationNode::TUnaryOperationNode(TPosition pos, EUnaryOperation op, TAstNodePtr expr)
    : TAstNode(pos)
    , Operation(op)
    , Expr(expr)
{
}

EUnaryOperation TUnaryOperationNode::GetOp() const {
    return Operation;
}

const TAstNodePtr TUnaryOperationNode::GetExpr() const {
    return Expr;
}

void TUnaryOperationNode::Accept(IAstNodeVisitor& visitor) const {
    return visitor.VisitUnaryOperation(*this);
}

EReturnType TUnaryOperationNode::GetReturnType() const {
    return Operation == EUnaryOperation::Not ? EReturnType::Bool : EReturnType::Any;
}

TBinaryOperationNode::TBinaryOperationNode(TPosition pos, EBinaryOperation op, TAstNodePtr leftExpr, TAstNodePtr rightExpr)
    : TAstNode(pos)
    , Operation(op)
    , LeftExpr(leftExpr)
    , RightExpr(rightExpr)
{
}

EBinaryOperation TBinaryOperationNode::GetOp() const {
    return Operation;
}

const TAstNodePtr TBinaryOperationNode::GetLeftExpr() const {
    return LeftExpr;
}

const TAstNodePtr TBinaryOperationNode::GetRightExpr() const {
    return RightExpr;
}

void TBinaryOperationNode::Accept(IAstNodeVisitor& visitor) const {
    return visitor.VisitBinaryOperation(*this);
}

EReturnType TBinaryOperationNode::GetReturnType() const {
    switch (Operation) {
        case EBinaryOperation::Less:
        case EBinaryOperation::LessEqual:
        case EBinaryOperation::Greater:
        case EBinaryOperation::GreaterEqual:
        case EBinaryOperation::Equal:
        case EBinaryOperation::NotEqual:
        case EBinaryOperation::And:
        case EBinaryOperation::Or:
            return EReturnType::Bool;

        default:
            return EReturnType::Any;
    }
}

TBooleanLiteralNode::TBooleanLiteralNode(TPosition pos, bool value)
    : TAstNode(pos)
    , Value(value)
{
}

bool TBooleanLiteralNode::GetValue() const {
    return Value;
}

void TBooleanLiteralNode::Accept(IAstNodeVisitor& visitor) const {
    return visitor.VisitBooleanLiteral(*this);
}

TNullLiteralNode::TNullLiteralNode(TPosition pos)
    : TAstNode(pos)
{
}

void TNullLiteralNode::Accept(IAstNodeVisitor& visitor) const {
    return visitor.VisitNullLiteral(*this);
}

TStringLiteralNode::TStringLiteralNode(TPosition pos, const TString& value)
    : TAstNode(pos)
    , Value(value)
{
}

const TString& TStringLiteralNode::GetValue() const {
    return Value;
}

void TStringLiteralNode::Accept(IAstNodeVisitor& visitor) const {
    return visitor.VisitStringLiteral(*this);
}

TFilterObjectNode::TFilterObjectNode(TPosition pos)
    : TAstNode(pos)
{
}

void TFilterObjectNode::Accept(IAstNodeVisitor& visitor) const {
    return visitor.VisitFilterObject(*this);
}

TFilterPredicateNode::TFilterPredicateNode(TPosition pos, TAstNodePtr predicate, TAstNodePtr input)
    : TAstNode(pos)
    , Predicate(predicate)
    , Input(input)
{
}

const TAstNodePtr TFilterPredicateNode::GetPredicate() const {
    return Predicate;
}

const TAstNodePtr TFilterPredicateNode::GetInput() const {
    return Input;
}

void TFilterPredicateNode::Accept(IAstNodeVisitor& visitor) const {
    return visitor.VisitFilterPredicate(*this);
}

TMethodCallNode::TMethodCallNode(TPosition pos, EMethodType type, TAstNodePtr input)
    : TAstNode(pos)
    , Type(type)
    , Input(input)
{
}

EMethodType TMethodCallNode::GetType() const {
    return Type;
}

const TAstNodePtr TMethodCallNode::GetInput() const {
    return Input;
}

void TMethodCallNode::Accept(IAstNodeVisitor& visitor) const {
    return visitor.VisitMethodCall(*this);
}

TStartsWithPredicateNode::TStartsWithPredicateNode(TPosition pos, TAstNodePtr input, TAstNodePtr prefix)
    : TAstNode(pos)
    , Input(input)
    , Prefix(prefix)
{
}

const TAstNodePtr TStartsWithPredicateNode::GetInput() const {
    return Input;
}

const TAstNodePtr TStartsWithPredicateNode::GetPrefix() const {
    return Prefix;
}

EReturnType TStartsWithPredicateNode::GetReturnType() const {
    return EReturnType::Bool;
}

void TStartsWithPredicateNode::Accept(IAstNodeVisitor& visitor) const {
    return visitor.VisitStartsWithPredicate(*this);
}

TExistsPredicateNode::TExistsPredicateNode(TPosition pos, TAstNodePtr input)
    : TAstNode(pos)
    , Input(input)
{
}

const TAstNodePtr TExistsPredicateNode::GetInput() const {
    return Input;
}

EReturnType TExistsPredicateNode::GetReturnType() const {
    return EReturnType::Bool;
}

void TExistsPredicateNode::Accept(IAstNodeVisitor& visitor) const {
    return visitor.VisitExistsPredicate(*this);
}

TIsUnknownPredicateNode::TIsUnknownPredicateNode(TPosition pos, TAstNodePtr input)
    : TAstNode(pos)
    , Input(input)
{
}

const TAstNodePtr TIsUnknownPredicateNode::GetInput() const {
    return Input;
}

EReturnType TIsUnknownPredicateNode::GetReturnType() const {
    return EReturnType::Bool;
}

void TIsUnknownPredicateNode::Accept(IAstNodeVisitor& visitor) const {
    return visitor.VisitIsUnknownPredicate(*this);
}

TLikeRegexPredicateNode::TLikeRegexPredicateNode(TPosition pos, TAstNodePtr input, NReWrapper::IRePtr&& regex)
    : TAstNode(pos)
    , Input(input)
    , Regex(std::move(regex))
{
}

const TAstNodePtr TLikeRegexPredicateNode::GetInput() const {
    return Input;
}

const NReWrapper::IRePtr& TLikeRegexPredicateNode::GetRegex() const {
    return Regex;
}

EReturnType TLikeRegexPredicateNode::GetReturnType() const {
    return EReturnType::Bool;
}

void TLikeRegexPredicateNode::Accept(IAstNodeVisitor& visitor) const {
    return visitor.VisitLikeRegexPredicate(*this);
}

}
