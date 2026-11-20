#pragma once
#include "query_common.h"

#include <yt/yt/library/query/misc/objects_holder.h>

#include <yt/yt/client/query_client/table_hint.h>

#include <util/generic/noncopyable.h>

#include <variant>

namespace NYT::NQueryClient::NAst {

////////////////////////////////////////////////////////////////////////////////

#define XX(name) \
struct name; \
using name ## Ptr = name*;

XX(TExpression)
XX(TReferenceExpression)
XX(TAliasExpression)
XX(TLiteralExpression)
XX(TFunctionExpression)
XX(TUnaryOpExpression)
XX(TBinaryOpExpression)
XX(TInExpression)
XX(TBetweenExpression)
XX(TTransformExpression)
XX(TCaseExpression)
XX(TLikeExpression)
XX(TQueryExpression)
XX(TQueryAstHead)

#undef XX


using TIdentifierList = std::vector<TReferenceExpressionPtr>;
using TExpressionList = std::vector<TExpressionPtr>;
using TNullableExpressionList = std::optional<TExpressionList>;

struct TOrderExpression
{
    TExpressionList Expressions;
    bool Descending = false;

    bool operator==(const TOrderExpression&) const = default;
};
using TOrderExpressionList = std::vector<TOrderExpression>;

struct TWhenThenExpression
{
    TExpressionList Condition;
    TExpressionList Result;

    bool operator==(const TWhenThenExpression&) const = default;
};
using TWhenThenExpressionList = std::vector<TWhenThenExpression>;

bool operator==(const TIdentifierList& lhs, const TIdentifierList& rhs);

bool operator==(const TExpressionList& lhs, const TExpressionList& rhs);

////////////////////////////////////////////////////////////////////////////////

struct TNullLiteralValue
{ };

bool operator==(TNullLiteralValue, TNullLiteralValue);

using TLiteralValue = std::variant<
    TNullLiteralValue,
    i64,
    ui64,
    double,
    bool,
    std::string
>;

using TLiteralValueList = std::vector<TLiteralValue>;
using TLiteralValueTuple = std::vector<TLiteralValue>;
using TLiteralValueTupleList = std::vector<TLiteralValueTuple>;
using TLiteralValueRange = std::pair<TLiteralValueTuple, TLiteralValueTuple>;
using TLiteralValueRangeList = std::vector<TLiteralValueRange>;

////////////////////////////////////////////////////////////////////////////////

using TExpressionTuple = std::vector<TExpressionPtr>;
using TExpressionTupleList = std::vector<TExpressionTuple>;
using TExpressionRange = std::pair<TExpressionTuple, TExpressionTuple>;
using TExpressionRangeList = std::vector<TExpressionRange>;

////////////////////////////////////////////////////////////////////////////////

struct TDoubleOrDotIntToken
{
    std::string Representation;

    TTupleItemIndexAccessor AsDotInt() const;
    double AsDouble() const;
};

////////////////////////////////////////////////////////////////////////////////

struct TColumnReference
{
    std::string ColumnName;
    std::optional<std::string> TableName;

    explicit TColumnReference(
        TStringBuf columnName,
        const std::optional<std::string>& tableName = {})
        : ColumnName(columnName)
        , TableName(tableName)
    { }

    bool operator==(const TColumnReference& other) const = default;
};

struct TColumnReferenceHasher
{
    size_t operator()(const TColumnReference& reference) const;
};

struct TColumnReferenceEqComparer
{
    bool operator()(const TColumnReference& lhs, const TColumnReference& rhs) const;
};

////////////////////////////////////////////////////////////////////////////////

using TStructAndTupleMemberAccessorListItem = std::variant<TStructMemberAccessor, TTupleItemIndexAccessor>;
using TStructAndTupleMemberAccessor = std::vector<TStructAndTupleMemberAccessorListItem>;

struct TCompositeTypeMemberAccessor
{
    using TDictOrListItemAccessor = TNullableExpressionList;

    TStructAndTupleMemberAccessor NestedStructOrTupleItemAccessor;
    TDictOrListItemAccessor DictOrListItemAccessor;

    bool IsEmpty() const;

    bool operator==(const TCompositeTypeMemberAccessor& other) const = default;
};

struct TReference
    : public TColumnReference
{
    TCompositeTypeMemberAccessor CompositeTypeAccessor;

    TReference() = default;

    explicit TReference(
        TStringBuf columnName,
        const std::optional<std::string>& tableName = {},
        const TCompositeTypeMemberAccessor& compositeTypeAccessor = {})
        : TColumnReference(columnName, tableName)
        , CompositeTypeAccessor(compositeTypeAccessor)
    { }

    bool operator==(const TReference& other) const = default;
};

struct TReferenceHasher
{
    size_t operator()(const TReference& reference) const;
};

struct TReferenceEqComparer
{
    bool operator()(const TReference& lhs, const TReference& rhs) const;
};

////////////////////////////////////////////////////////////////////////////////

struct TExpression
{
    TSourceLocation SourceLocation;

    explicit TExpression(const TSourceLocation& sourceLocation)
        : SourceLocation(sourceLocation)
    { }

    template <class TDerived>
    const TDerived* As() const
    {
        return dynamic_cast<const TDerived*>(this);
    }

    template <class TDerived>
    TDerived* As()
    {
        return dynamic_cast<TDerived*>(this);
    }

    TStringBuf GetSource(TStringBuf source) const;

    virtual ~TExpression() = default;
};

template <class T, class... TArgs>
TExpressionList MakeExpression(TObjectsHolder* holder, TArgs&& ... args)
{
    return TExpressionList(1, holder->Register(new T(std::forward<TArgs>(args)...)));
}

bool operator==(const TExpression& lhs, const TExpression& rhs);

////////////////////////////////////////////////////////////////////////////////

struct TLiteralExpression
    : public TExpression
{
    TLiteralValue Value;

    TLiteralExpression(
        const TSourceLocation& sourceLocation,
        TLiteralValue value)
        : TExpression(sourceLocation)
        , Value(std::move(value))
    { }
};

////////////////////////////////////////////////////////////////////////////////

struct TReferenceExpression
    : public TExpression
{
    TReference Reference;

    TReferenceExpression(
        const TSourceLocation& sourceLocation,
        TStringBuf columnName,
        std::optional<std::string> tableName = {},
        TCompositeTypeMemberAccessor compositeTypeAccessor = {})
        : TExpression(sourceLocation)
        , Reference(columnName, std::move(tableName), std::move(compositeTypeAccessor))
    { }

    TReferenceExpression(
        const TSourceLocation& sourceLocation,
        const TReference& reference)
        : TExpression(sourceLocation)
        , Reference(reference)
    { }
};

////////////////////////////////////////////////////////////////////////////////

struct TAliasExpression
    : public TExpression
{
    TExpressionPtr Expression;
    std::string Name;

    TAliasExpression(
        const TSourceLocation& sourceLocation,
        const TExpressionPtr& expression,
        TStringBuf name)
        : TExpression(sourceLocation)
        , Expression(expression)
        , Name(name)
    { }
};

////////////////////////////////////////////////////////////////////////////////

struct TFunctionExpression
    : public TExpression
{
    std::string FunctionName;
    TExpressionList Arguments;

    TFunctionExpression(
        const TSourceLocation& sourceLocation,
        TStringBuf functionName,
        TExpressionList arguments)
        : TExpression(sourceLocation)
        , FunctionName(functionName)
        , Arguments(std::move(arguments))
    { }
};

////////////////////////////////////////////////////////////////////////////////

struct TUnaryOpExpression
    : public TExpression
{
    EUnaryOp Opcode;
    TExpressionList Operand;

    TUnaryOpExpression(
        const TSourceLocation& sourceLocation,
        EUnaryOp opcode,
        TExpressionList operand)
        : TExpression(sourceLocation)
        , Opcode(opcode)
        , Operand(std::move(operand))
    { }
};

////////////////////////////////////////////////////////////////////////////////

struct TBinaryOpExpression
    : public TExpression
{
    EBinaryOp Opcode;
    TExpressionList Lhs;
    TExpressionList Rhs;

    TBinaryOpExpression(
        const TSourceLocation& sourceLocation,
        EBinaryOp opcode,
        TExpressionList lhs,
        TExpressionList rhs)
        : TExpression(sourceLocation)
        , Opcode(opcode)
        , Lhs(std::move(lhs))
        , Rhs(std::move(rhs))
    { }
};

////////////////////////////////////////////////////////////////////////////////

struct TInExpression
    : public TExpression
{
    TExpressionList Expr;
    TLiteralValueTupleList Values;

    TInExpression(
        const TSourceLocation& sourceLocation,
        TExpressionList expression,
        TLiteralValueTupleList values)
        : TExpression(sourceLocation)
        , Expr(std::move(expression))
        , Values(std::move(values))
    { }
};

////////////////////////////////////////////////////////////////////////////////

struct TBetweenExpression
    : public TExpression
{
    TExpressionList Expr;
    TLiteralValueRangeList Values;

    TBetweenExpression(
        const TSourceLocation& sourceLocation,
        TExpressionList expression,
        const TLiteralValueRangeList& values)
        : TExpression(sourceLocation)
        , Expr(std::move(expression))
        , Values(values)
    { }
};

////////////////////////////////////////////////////////////////////////////////

struct TTransformExpression
    : public TExpression
{
    TExpressionList Expr;
    TLiteralValueTupleList From;
    TLiteralValueTupleList To;
    TNullableExpressionList DefaultExpr;

    TTransformExpression(
        const TSourceLocation& sourceLocation,
        TExpressionList expression,
        const TLiteralValueTupleList& from,
        const TLiteralValueTupleList& to,
        TNullableExpressionList defaultExpr)
        : TExpression(sourceLocation)
        , Expr(std::move(expression))
        , From(from)
        , To(to)
        , DefaultExpr(std::move(defaultExpr))
    { }
};

////////////////////////////////////////////////////////////////////////////////

struct TCaseExpression
    : public TExpression
{
    TNullableExpressionList OptionalOperand;
    TWhenThenExpressionList WhenThenExpressions;
    TNullableExpressionList DefaultExpression;

    TCaseExpression(
        const TSourceLocation& sourceLocation,
        TNullableExpressionList optionalOperand,
        TWhenThenExpressionList whenThenExpressions,
        TNullableExpressionList defaultExpression)
        : TExpression(sourceLocation)
        , OptionalOperand(std::move(optionalOperand))
        , WhenThenExpressions(std::move(whenThenExpressions))
        , DefaultExpression(std::move(defaultExpression))
    { }
};

////////////////////////////////////////////////////////////////////////////////

struct TLikeExpression
    : public TExpression
{
    TExpressionList Text;
    EStringMatchOp Opcode;
    TExpressionList Pattern;
    TNullableExpressionList EscapeCharacter;

    TLikeExpression(
        const TSourceLocation& sourceLocation,
        TExpressionList text,
        EStringMatchOp opcode,
        TExpressionList pattern,
        TNullableExpressionList EscapeCharacter)
        : TExpression(sourceLocation)
        , Text(std::move(text))
        , Opcode(opcode)
        , Pattern(std::move(pattern))
        , EscapeCharacter(std::move(EscapeCharacter))
    { }
};

////////////////////////////////////////////////////////////////////////////////

struct TTableDescriptor
{
    NYPath::TYPath Path;
    std::optional<std::string> Alias;
    TTableHint Hint;

    TTableDescriptor() = default;

    explicit TTableDescriptor(
        NYPath::TYPath path,
        std::optional<std::string> alias = std::nullopt,
        TTableHint hint = {})
        : Path(std::move(path))
        , Alias(std::move(alias))
        , Hint(std::move(hint))
    { }
};

bool operator==(const TTableDescriptor& lhs, const TTableDescriptor& rhs);

////////////////////////////////////////////////////////////////////////////////

struct TJoin
{
    bool IsLeft;
    TTableDescriptor Table;
    TIdentifierList Fields;

    TExpressionList Lhs;
    TExpressionList Rhs;

    TNullableExpressionList Predicate;

    TJoin(
        bool isLeft,
        const TTableDescriptor& table,
        const TIdentifierList& fields,
        const TNullableExpressionList& predicate)
        : IsLeft(isLeft)
        , Table(table)
        , Fields(fields)
        , Predicate(predicate)
    { }

    TJoin(
        bool isLeft,
        const TTableDescriptor& table,
        const TExpressionList& lhs,
        const TExpressionList& rhs,
        const TNullableExpressionList& predicate)
        : IsLeft(isLeft)
        , Table(table)
        , Lhs(lhs)
        , Rhs(rhs)
        , Predicate(predicate)
    { }
};

bool operator==(const TJoin& lhs, const TJoin& rhs);

////////////////////////////////////////////////////////////////////////////////

struct TArrayJoin
{
    bool IsLeft;
    TExpressionList Columns;

    TNullableExpressionList Predicate;

    TArrayJoin(
        bool isLeft,
        const TExpressionList& columns,
        const TNullableExpressionList& predicate)
        : IsLeft(isLeft)
        , Columns(columns)
        , Predicate(predicate)
    { }

    bool operator==(const TArrayJoin& other) const = default;
};

////////////////////////////////////////////////////////////////////////////////

struct TUnionAllClause
{
    std::vector<TQueryAstHeadPtr> Subqueries;
    std::optional<std::string> Alias;

    bool operator==(const TUnionAllClause& other) const = default;
};

struct TQuery
{
    std::variant<
        TTableDescriptor,
        TQueryAstHeadPtr,
        TExpressionList,
        TUnionAllClause> FromClause;
    std::optional<TTableDescriptor> WithIndex;
    std::vector<std::variant<TJoin, TArrayJoin>> Joins;

    TNullableExpressionList SelectExprs;
    TNullableExpressionList WherePredicate;

    TNullableExpressionList GroupExprs;
    ETotalsMode TotalsMode = ETotalsMode::None;
    TNullableExpressionList HavingPredicate;

    TOrderExpressionList OrderExpressions;

    std::optional<i64> Offset;
    std::optional<i64> Limit;

    bool operator==(const TQuery& other) const;
};

////////////////////////////////////////////////////////////////////////////////

using TAliasMap = THashMap<std::string, TExpressionPtr>;

struct TAstHead
    : public TObjectsHolder
{
    std::variant<TQuery, TExpressionPtr> Ast;
    TAliasMap AliasMap;
};

struct TQueryAstHead
{
    TQuery Ast;
    TAliasMap AliasMap;
    std::optional<std::string> Alias;
};

////////////////////////////////////////////////////////////////////////////////

struct TQueryExpression
    : public TExpression
{
    TQuery Query;
    TAliasMap AliasMap;

    TQueryExpression(
        const TSourceLocation& sourceLocation,
        TQuery query,
        TAliasMap aliasMap)
        : TExpression(sourceLocation)
        , Query(std::move(query))
        , AliasMap(std::move(aliasMap))
    { }
};

////////////////////////////////////////////////////////////////////////////////

TStringBuf GetSource(TSourceLocation sourceLocation, TStringBuf source);

std::string FormatId(TStringBuf id);
std::string FormatLiteralValue(const TLiteralValue& value);
std::string FormatReference(const TReference& ref);
std::string FormatExpression(const TExpression& expr);
std::string FormatExpressionConcise(const TExpression& expr);
std::string FormatExpression(const TExpressionList& exprs);
std::string FormatJoin(const TJoin& join);
std::string FormatArrayJoin(const TArrayJoin& join);
std::string FormatQuery(const TQuery& query);
std::string FormatQueryConcise(const TQuery& query);
std::string InferColumnName(const TExpression& expr);
std::string InferColumnName(const TColumnReference& ref);

////////////////////////////////////////////////////////////////////////////////

NYPath::TYPath GetMainTablePath(const TQuery& query);

////////////////////////////////////////////////////////////////////////////////

NAst::TExpressionPtr BuildAndExpression(
    TObjectsHolder* holder,
    NAst::TExpressionPtr lhs,
    NAst::TExpressionPtr rhs);

NAst::TExpressionPtr BuildOrExpression(
    TObjectsHolder* holder,
    NAst::TExpressionPtr lhs,
    NAst::TExpressionPtr rhs);

NAst::TExpressionPtr BuildConcatenationExpression(
    TObjectsHolder* holder,
    NAst::TExpressionPtr lhs,
    NAst::TExpressionPtr rhs,
    TStringBuf separator);

//! For commutative operations only.
NAst::TExpressionPtr BuildBinaryOperationTree(
    TObjectsHolder* holder,
    std::vector<NAst::TExpressionPtr> leaves,
    EBinaryOp opCode);

////////////////////////////////////////////////////////////////////////////////

TExpressionList MakeInExpressionFromExpressionTupleList(
    TObjectsHolder* holder,
    const TSourceLocation& sourceLocation,
    const TExpressionList& expr,
    const TExpressionTupleList& expressions);

////////////////////////////////////////////////////////////////////////////////

TExpressionList MakeBetweenExpressionFromExpressionTupleRangeList(
    TObjectsHolder* holder,
    const TSourceLocation& sourceLocation,
    const TExpressionList& expr,
    const TExpressionRangeList& expressions);

TExpressionList MakeBetweenExpressionFromExpressionTupleRange(
    TObjectsHolder* holder,
    const TSourceLocation& sourceLocation,
    const TExpressionList& expr,
    const TExpressionTuple& lower,
    const TExpressionTuple& upper);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient::NAst
