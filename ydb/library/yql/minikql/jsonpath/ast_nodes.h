#pragma once

#include <ydb/library/yql/public/issue/yql_issue.h>

#include <library/cpp/json/json_value.h>
#include <ydb/library/rewrapper/re.h>

namespace NYql::NJsonPath {

class TRootNode;
class TContextObjectNode;
class TVariableNode;
class TLastArrayIndexNode;
class TNumberLiteralNode;
class TAccessorExprNode;
class TMemberAccessNode;
class TWildcardMemberAccessNode;
class TArrayAccessNode;
class TWildcardArrayAccessNode;
class TUnaryOperationNode;
class TBinaryOperationNode;
class TBooleanLiteralNode;
class TNullLiteralNode;
class TStringLiteralNode;
class TFilterObjectNode;
class TFilterPredicateNode;
class TMethodCallNode;
class TStartsWithPredicateNode;
class TExistsPredicateNode;
class TIsUnknownPredicateNode;
class TLikeRegexPredicateNode;

enum class EJsonPathMode {
    Lax = 0,
    Strict = 1,
};

class IAstNodeVisitor {
public:
    virtual void VisitRoot(const TRootNode& node) = 0;
    virtual void VisitContextObject(const TContextObjectNode& node) = 0;
    virtual void VisitVariable(const TVariableNode& node) = 0;
    virtual void VisitLastArrayIndex(const TLastArrayIndexNode& node) = 0;
    virtual void VisitNumberLiteral(const TNumberLiteralNode& node) = 0;
    virtual void VisitMemberAccess(const TMemberAccessNode& node) = 0;
    virtual void VisitWildcardMemberAccess(const TWildcardMemberAccessNode& node) = 0;
    virtual void VisitArrayAccess(const TArrayAccessNode& node) = 0;
    virtual void VisitWildcardArrayAccess(const TWildcardArrayAccessNode& node) = 0;
    virtual void VisitUnaryOperation(const TUnaryOperationNode& node) = 0;
    virtual void VisitBinaryOperation(const TBinaryOperationNode& node) = 0;
    virtual void VisitBooleanLiteral(const TBooleanLiteralNode& node) = 0;
    virtual void VisitNullLiteral(const TNullLiteralNode& node) = 0;
    virtual void VisitStringLiteral(const TStringLiteralNode& node) = 0;
    virtual void VisitFilterObject(const TFilterObjectNode& node) = 0;
    virtual void VisitFilterPredicate(const TFilterPredicateNode& node) = 0;
    virtual void VisitMethodCall(const TMethodCallNode& node) = 0;
    virtual void VisitStartsWithPredicate(const TStartsWithPredicateNode& node) = 0;
    virtual void VisitExistsPredicate(const TExistsPredicateNode& node) = 0;
    virtual void VisitIsUnknownPredicate(const TIsUnknownPredicateNode& node) = 0;
    virtual void VisitLikeRegexPredicate(const TLikeRegexPredicateNode& node) = 0;

    virtual ~IAstNodeVisitor() = default;
};

enum class EReturnType {
    Any = 0,
    Bool = 1,
};

class TAstNode : public TSimpleRefCount<TAstNode> {
public:
    explicit TAstNode(TPosition pos);

    TPosition GetPos() const;

    virtual void Accept(IAstNodeVisitor& visitor) const = 0;

    virtual EReturnType GetReturnType() const;

    virtual ~TAstNode() = default;

private:
    TPosition Pos;
};

using TAstNodePtr = TIntrusivePtr<TAstNode>;

class TRootNode : public TAstNode {
public:
    TRootNode(TPosition pos, TAstNodePtr expr, EJsonPathMode mode);

    const TAstNodePtr GetExpr() const;

    EJsonPathMode GetMode() const;

    void Accept(IAstNodeVisitor& visitor) const override;

    EReturnType GetReturnType() const override;

private:
    TAstNodePtr Expr;
    EJsonPathMode Mode;
};

class TContextObjectNode : public TAstNode {
public:
    explicit TContextObjectNode(TPosition pos);

    void Accept(IAstNodeVisitor& visitor) const override;
};

class TVariableNode : public TAstNode {
public:
    TVariableNode(TPosition pos, const TString& name);

    const TString& GetName() const;

    void Accept(IAstNodeVisitor& visitor) const override;

private:
    TString Name;
};

class TLastArrayIndexNode : public TAstNode {
public:
    explicit TLastArrayIndexNode(TPosition pos);

    void Accept(IAstNodeVisitor& visitor) const override;
};

class TNumberLiteralNode : public TAstNode {
public:
    TNumberLiteralNode(TPosition pos, double value);

    double GetValue() const;

    void Accept(IAstNodeVisitor& visitor) const override;

private:
    double Value;
};

class TMemberAccessNode : public TAstNode {
public:
    TMemberAccessNode(TPosition pos, const TString& member, TAstNodePtr input);

    const TStringBuf GetMember() const;

    const TAstNodePtr GetInput() const;

    void Accept(IAstNodeVisitor& visitor) const override;

private:
    TString Member;
    TAstNodePtr Input;
};

class TWildcardMemberAccessNode : public TAstNode {
public:
    TWildcardMemberAccessNode(TPosition pos, TAstNodePtr input);

    const TAstNodePtr GetInput() const;

    void Accept(IAstNodeVisitor& visitor) const override;

private:
    TAstNodePtr Input;
};

class TArrayAccessNode : public TAstNode {
public:
    struct TSubscript {
        TAstNodePtr From;
        TAstNodePtr To;
    };

    TArrayAccessNode(TPosition pos, TVector<TSubscript> subscripts, TAstNodePtr input);

    const TVector<TSubscript>& GetSubscripts() const;

    const TAstNodePtr GetInput() const;

    void Accept(IAstNodeVisitor& visitor) const override;

private:
    TVector<TSubscript> Subscripts;
    TAstNodePtr Input;
};

class TWildcardArrayAccessNode : public TAstNode {
public:
    TWildcardArrayAccessNode(TPosition pos, TAstNodePtr input);

    const TAstNodePtr GetInput() const;

    void Accept(IAstNodeVisitor& visitor) const override;

private:
    TAstNodePtr Input;
};

enum class EUnaryOperation {
    Plus = 0,
    Minus = 1,
    Not = 2,
};

class TUnaryOperationNode : public TAstNode {
public:
    TUnaryOperationNode(TPosition pos, EUnaryOperation op, TAstNodePtr expr);

    EUnaryOperation GetOp() const;

    const TAstNodePtr GetExpr() const;

    void Accept(IAstNodeVisitor& visitor) const override;

    EReturnType GetReturnType() const override;

private:
    EUnaryOperation Operation;
    TAstNodePtr Expr;
};

enum class EBinaryOperation {
    Add = 0,
    Substract = 1,
    Multiply = 2,
    Divide = 3,
    Modulo = 4,
    Less = 5,
    LessEqual = 6,
    Greater = 7,
    GreaterEqual = 8,
    Equal = 9,
    NotEqual = 10,
    And = 11,
    Or = 12,
};

class TBinaryOperationNode : public TAstNode {
public:
    TBinaryOperationNode(TPosition pos, EBinaryOperation op, TAstNodePtr leftExpr, TAstNodePtr rightExpr);

    EBinaryOperation GetOp() const;

    const TAstNodePtr GetLeftExpr() const;

    const TAstNodePtr GetRightExpr() const;

    void Accept(IAstNodeVisitor& visitor) const override;

    EReturnType GetReturnType() const override;

private:
    EBinaryOperation Operation;
    TAstNodePtr LeftExpr;
    TAstNodePtr RightExpr;
};

class TBooleanLiteralNode : public TAstNode {
public:
    TBooleanLiteralNode(TPosition pos, bool value);

    bool GetValue() const;

    void Accept(IAstNodeVisitor& visitor) const override;

private:
    bool Value;
};

class TNullLiteralNode : public TAstNode {
public:
    explicit TNullLiteralNode(TPosition pos);

    void Accept(IAstNodeVisitor& visitor) const override;
};

class TStringLiteralNode : public TAstNode {
public:
    TStringLiteralNode(TPosition pos, const TString& value);

    const TString& GetValue() const;

    void Accept(IAstNodeVisitor& visitor) const override;

private:
    TString Value;
};

class TFilterObjectNode : public TAstNode {
public:
    explicit TFilterObjectNode(TPosition pos);

    void Accept(IAstNodeVisitor& visitor) const override;
};

class TFilterPredicateNode : public TAstNode {
public:
    TFilterPredicateNode(TPosition pos, TAstNodePtr predicate, TAstNodePtr input);

    const TAstNodePtr GetPredicate() const;

    const TAstNodePtr GetInput() const;

    void Accept(IAstNodeVisitor& visitor) const override;

private:
    TAstNodePtr Predicate;
    TAstNodePtr Input;
};

enum class EMethodType {
    Abs = 0,
    Floor = 1,
    Ceiling = 2,
    Double = 3,
    Type = 4,
    Size = 5,
    KeyValue = 6,
};

class TMethodCallNode : public TAstNode {
public:
    TMethodCallNode(TPosition pos, EMethodType type, TAstNodePtr input);

    EMethodType GetType() const;

    const TAstNodePtr GetInput() const;

    void Accept(IAstNodeVisitor& visitor) const override;

private:
    EMethodType Type;
    TAstNodePtr Input;
};

class TStartsWithPredicateNode : public TAstNode {
public:
    TStartsWithPredicateNode(TPosition pos, TAstNodePtr input, TAstNodePtr prefix);

    const TAstNodePtr GetInput() const;

    const TAstNodePtr GetPrefix() const;

    EReturnType GetReturnType() const override;

    void Accept(IAstNodeVisitor& visitor) const override;

private:
    TAstNodePtr Input;
    TAstNodePtr Prefix;
};

class TExistsPredicateNode : public TAstNode {
public:
    TExistsPredicateNode(TPosition pos, TAstNodePtr input);

    const TAstNodePtr GetInput() const;

    EReturnType GetReturnType() const override;

    void Accept(IAstNodeVisitor& visitor) const override;

private:
    TAstNodePtr Input;
};

class TIsUnknownPredicateNode : public TAstNode {
public:
    TIsUnknownPredicateNode(TPosition pos, TAstNodePtr input);

    const TAstNodePtr GetInput() const;

    EReturnType GetReturnType() const override;

    void Accept(IAstNodeVisitor& visitor) const override;

private:
    TAstNodePtr Input;
};

class TLikeRegexPredicateNode : public TAstNode {
public:
    TLikeRegexPredicateNode(TPosition pos, TAstNodePtr input, NReWrapper::IRePtr&& regex);

    const TAstNodePtr GetInput() const;

    const NReWrapper::IRePtr& GetRegex() const;

    EReturnType GetReturnType() const override;

    void Accept(IAstNodeVisitor& visitor) const override;

private:
    TAstNodePtr Input;
    NReWrapper::IRePtr Regex;
};

}
