#include "ast_nodes.h"

#include <utility>

namespace NYql::NJsonPath {

TAstNode::TAstNode(TPosition pos)
    : Pos_(std::move(pos))
{
}

TPosition TAstNode::GetPos() const {
    return Pos_;
}

EReturnType TAstNode::GetReturnType() const {
    return EReturnType::Any;
}

TRootNode::TRootNode(TPosition pos, TAstNodePtr expr, EJsonPathMode mode)
    : TAstNode(pos)
    , Expr_(std::move(expr))
    , Mode_(mode)
{
}

TAstNodePtr TRootNode::GetExpr() const {
    return Expr_;
}

EJsonPathMode TRootNode::GetMode() const {
    return Mode_;
}

void TRootNode::Accept(IAstNodeVisitor& visitor) const {
    return visitor.VisitRoot(*this);
}

EReturnType TRootNode::GetReturnType() const {
    return Expr_->GetReturnType();
}

TContextObjectNode::TContextObjectNode(TPosition pos)
    : TAstNode(pos)
{
}

void TContextObjectNode::Accept(IAstNodeVisitor& visitor) const {
    return visitor.VisitContextObject(*this);
}

TVariableNode::TVariableNode(TPosition pos, TString name)
    : TAstNode(pos)
    , Name_(std::move(name))
{
}

const TString& TVariableNode::GetName() const {
    return Name_;
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
    , Value_(value)
{
}

double TNumberLiteralNode::GetValue() const {
    return Value_;
}

void TNumberLiteralNode::Accept(IAstNodeVisitor& visitor) const {
    return visitor.VisitNumberLiteral(*this);
}

TMemberAccessNode::TMemberAccessNode(TPosition pos, TString member, TAstNodePtr input)
    : TAstNode(pos)
    , Member_(std::move(member))
    , Input_(std::move(input))
{
}

TStringBuf TMemberAccessNode::GetMember() const {
    return Member_;
}

TAstNodePtr TMemberAccessNode::GetInput() const {
    return Input_;
}

void TMemberAccessNode::Accept(IAstNodeVisitor& visitor) const {
    return visitor.VisitMemberAccess(*this);
}

TWildcardMemberAccessNode::TWildcardMemberAccessNode(TPosition pos, TAstNodePtr input)
    : TAstNode(pos)
    , Input_(std::move(input))
{
}

TAstNodePtr TWildcardMemberAccessNode::GetInput() const {
    return Input_;
}

void TWildcardMemberAccessNode::Accept(IAstNodeVisitor& visitor) const {
    return visitor.VisitWildcardMemberAccess(*this);
}

TArrayAccessNode::TArrayAccessNode(TPosition pos, TVector<TSubscript> subscripts, TAstNodePtr input)
    : TAstNode(pos)
    , Subscripts_(std::move(subscripts))
    , Input_(std::move(input))
{
}

const TVector<TArrayAccessNode::TSubscript>& TArrayAccessNode::GetSubscripts() const {
    return Subscripts_;
}

TAstNodePtr TArrayAccessNode::GetInput() const {
    return Input_;
}

void TArrayAccessNode::Accept(IAstNodeVisitor& visitor) const {
    return visitor.VisitArrayAccess(*this);
}

TWildcardArrayAccessNode::TWildcardArrayAccessNode(TPosition pos, TAstNodePtr input)
    : TAstNode(pos)
    , Input_(std::move(input))
{
}

TAstNodePtr TWildcardArrayAccessNode::GetInput() const {
    return Input_;
}

void TWildcardArrayAccessNode::Accept(IAstNodeVisitor& visitor) const {
    return visitor.VisitWildcardArrayAccess(*this);
}

TUnaryOperationNode::TUnaryOperationNode(TPosition pos, EUnaryOperation op, TAstNodePtr expr)
    : TAstNode(pos)
    , Operation_(op)
    , Expr_(std::move(expr))
{
}

EUnaryOperation TUnaryOperationNode::GetOp() const {
    return Operation_;
}

TAstNodePtr TUnaryOperationNode::GetExpr() const {
    return Expr_;
}

void TUnaryOperationNode::Accept(IAstNodeVisitor& visitor) const {
    return visitor.VisitUnaryOperation(*this);
}

EReturnType TUnaryOperationNode::GetReturnType() const {
    return Operation_ == EUnaryOperation::Not ? EReturnType::Bool : EReturnType::Any;
}

TBinaryOperationNode::TBinaryOperationNode(TPosition pos, EBinaryOperation op, TAstNodePtr leftExpr, TAstNodePtr rightExpr)
    : TAstNode(pos)
    , Operation_(op)
    , LeftExpr_(std::move(leftExpr))
    , RightExpr_(std::move(rightExpr))
{
}

EBinaryOperation TBinaryOperationNode::GetOp() const {
    return Operation_;
}

TAstNodePtr TBinaryOperationNode::GetLeftExpr() const {
    return LeftExpr_;
}

TAstNodePtr TBinaryOperationNode::GetRightExpr() const {
    return RightExpr_;
}

void TBinaryOperationNode::Accept(IAstNodeVisitor& visitor) const {
    return visitor.VisitBinaryOperation(*this);
}

EReturnType TBinaryOperationNode::GetReturnType() const {
    switch (Operation_) {
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
    , Value_(value)
{
}

bool TBooleanLiteralNode::GetValue() const {
    return Value_;
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

TStringLiteralNode::TStringLiteralNode(TPosition pos, TString value)
    : TAstNode(pos)
    , Value_(std::move(value))
{
}

const TString& TStringLiteralNode::GetValue() const {
    return Value_;
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
    , Predicate_(std::move(predicate))
    , Input_(std::move(input))
{
}

TAstNodePtr TFilterPredicateNode::GetPredicate() const {
    return Predicate_;
}

TAstNodePtr TFilterPredicateNode::GetInput() const {
    return Input_;
}

void TFilterPredicateNode::Accept(IAstNodeVisitor& visitor) const {
    return visitor.VisitFilterPredicate(*this);
}

TMethodCallNode::TMethodCallNode(TPosition pos, EMethodType type, TAstNodePtr input)
    : TAstNode(pos)
    , Type_(type)
    , Input_(std::move(input))
{
}

EMethodType TMethodCallNode::GetType() const {
    return Type_;
}

TAstNodePtr TMethodCallNode::GetInput() const {
    return Input_;
}

void TMethodCallNode::Accept(IAstNodeVisitor& visitor) const {
    return visitor.VisitMethodCall(*this);
}

TStartsWithPredicateNode::TStartsWithPredicateNode(TPosition pos, TAstNodePtr input, TAstNodePtr prefix)
    : TAstNode(pos)
    , Input_(std::move(input))
    , Prefix_(std::move(prefix))
{
}

TAstNodePtr TStartsWithPredicateNode::GetInput() const {
    return Input_;
}

TAstNodePtr TStartsWithPredicateNode::GetPrefix() const {
    return Prefix_;
}

EReturnType TStartsWithPredicateNode::GetReturnType() const {
    return EReturnType::Bool;
}

void TStartsWithPredicateNode::Accept(IAstNodeVisitor& visitor) const {
    return visitor.VisitStartsWithPredicate(*this);
}

TExistsPredicateNode::TExistsPredicateNode(TPosition pos, TAstNodePtr input)
    : TAstNode(pos)
    , Input_(std::move(input))
{
}

TAstNodePtr TExistsPredicateNode::GetInput() const {
    return Input_;
}

EReturnType TExistsPredicateNode::GetReturnType() const {
    return EReturnType::Bool;
}

void TExistsPredicateNode::Accept(IAstNodeVisitor& visitor) const {
    return visitor.VisitExistsPredicate(*this);
}

TIsUnknownPredicateNode::TIsUnknownPredicateNode(TPosition pos, TAstNodePtr input)
    : TAstNode(pos)
    , Input_(std::move(input))
{
}

TAstNodePtr TIsUnknownPredicateNode::GetInput() const {
    return Input_;
}

EReturnType TIsUnknownPredicateNode::GetReturnType() const {
    return EReturnType::Bool;
}

void TIsUnknownPredicateNode::Accept(IAstNodeVisitor& visitor) const {
    return visitor.VisitIsUnknownPredicate(*this);
}

TLikeRegexPredicateNode::TLikeRegexPredicateNode(TPosition pos, TAstNodePtr input, NReWrapper::IRePtr&& regex)
    : TAstNode(pos)
    , Input_(std::move(input))
    , Regex_(std::move(regex))
{
}

TAstNodePtr TLikeRegexPredicateNode::GetInput() const {
    return Input_;
}

const NReWrapper::IRePtr& TLikeRegexPredicateNode::GetRegex() const {
    return Regex_;
}

EReturnType TLikeRegexPredicateNode::GetReturnType() const {
    return EReturnType::Bool;
}

void TLikeRegexPredicateNode::Accept(IAstNodeVisitor& visitor) const {
    return visitor.VisitLikeRegexPredicate(*this);
}

} // namespace NYql::NJsonPath
