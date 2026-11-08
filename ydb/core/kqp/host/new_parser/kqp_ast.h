#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

#include <memory>
#include <optional>
#include <variant>

namespace NKikimr::NKqp::NNewYql {

// Offsets are zero-based Unicode code-point offsets, with an exclusive end.
// Line is one-based and Column is zero-based, matching the ANTLR token locations.
struct TSourceRange {
    size_t Begin = 0;
    size_t End = 0;
    ui32 Line = 1;
    ui32 Column = 0;
};

// Preserve quoting and escapes until name resolution, without retaining tokens.
struct TIdentifier {
    TString Spelling;
    TSourceRange Range;
};

struct TExpression;
struct TQuery;
struct TSource;
using TExpressionPtr = std::unique_ptr<TExpression>;
using TQueryPtr = std::unique_ptr<TQuery>;
using TSourcePtr = std::unique_ptr<TSource>;

enum class ESetQuantifier { Default, All, Distinct };
enum class ELiteralKind { Integer, Real, String, Boolean, Null, Blob };
enum class EUnaryOp { Plus, Minus, BitNot, Not, IsNull, IsNotNull };
enum class EBinaryOp {
    Or, And, Xor, Equal, NotEqual, Less, LessOrEqual, Greater, GreaterOrEqual,
    IsDistinctFrom, IsNotDistinctFrom, Add, Subtract, Multiply, Divide, Modulo,
    BitAnd, BitOr, BitXor, ShiftLeft, ShiftRight, RotateLeft, RotateRight, Concat, Coalesce
};

struct TLiteral {
    ELiteralKind Kind;
    // Includes numeric suffixes, string delimiters and escapes. Type checking
    // decodes the value; constructing the AST must not lose this information.
    TString Spelling;
};

struct TName {
    TIdentifier Name;
    std::optional<TIdentifier> Module;
};

struct TParameter {
    TString Spelling; // Includes '$'.
};

struct TStar {
    std::optional<TIdentifier> Qualifier;
};

struct TUnary {
    EUnaryOp Op;
    TExpressionPtr Operand;
};

struct TBinary {
    EBinaryOp Op;
    TExpressionPtr Left;
    TExpressionPtr Right;
};

struct TNamedExpression {
    TExpressionPtr Expression;
    std::optional<TIdentifier> Alias;
};

enum class ESortDirection { Default, Ascending, Descending };
struct TSortSpecification {
    TExpressionPtr Expression;
    ESortDirection Direction = ESortDirection::Default;
};

enum class EWindowFrameUnits { Rows, Range, Groups };
enum class EWindowBoundKind { CurrentRow, Preceding, Following, UnboundedPreceding, UnboundedFollowing };
enum class EWindowExclusion { Default, CurrentRow, Group, Ties, NoOthers };
struct TWindowBound {
    TSourceRange Range;
    EWindowBoundKind Kind = EWindowBoundKind::CurrentRow;
    TExpressionPtr Offset; // Only for Preceding and Following.
};

struct TWindowFrame {
    TSourceRange Range;
    EWindowFrameUnits Units = EWindowFrameUnits::Rows;
    TWindowBound Begin;
    // Absent for the single-bound syntax; resolve its implicit end later.
    std::optional<TWindowBound> End;
    EWindowExclusion Exclusion = EWindowExclusion::Default;
};

struct TWindowSpecification {
    TSourceRange Range;
    std::optional<TIdentifier> BaseWindow;
    TVector<TNamedExpression> PartitionBy;
    bool CompactPartition = false;
    TVector<TSortSpecification> OrderBy;
    std::optional<TWindowFrame> Frame;
};

struct TWindowReference {
    TSourceRange Range;
    // OVER w and OVER (w) have distinct inheritance rules.
    std::variant<TIdentifier, TWindowSpecification> Value;
};

struct TWindowDefinition {
    TSourceRange Range;
    TIdentifier Name;
    TWindowSpecification Specification;
};

enum class ENullTreatment { Default, Respect, Ignore };
struct TCall {
    TExpressionPtr Callable;
    TVector<TNamedExpression> Arguments;
    ESetQuantifier Quantifier = ESetQuantifier::Default;
    bool StarArgument = false;
    ENullTreatment NullTreatment = ENullTreatment::Default;
    TExpressionPtr Filter;
    std::optional<TWindowReference> Over;
};

struct TMember {
    TExpressionPtr Object;
    TIdentifier Member;
};

struct TIndex {
    TExpressionPtr Object;
    TExpressionPtr Index;
};

struct TTuple {
    TVector<TNamedExpression> Elements;
};

struct TList {
    TVector<TExpressionPtr> Elements;
};

struct TWhen {
    TExpressionPtr When;
    TExpressionPtr Then;
};

struct TCase {
    TExpressionPtr Operand; // Null for a searched CASE.
    TVector<TWhen> Branches;
    TExpressionPtr Else;
};

struct TTypeTag {
    TSourceRange Range;
    std::variant<TIdentifier, TLiteral, TParameter> Value;
};

struct TTypeMember;
struct TTypeName {
    TIdentifier Name;
    TVector<TTypeName> Arguments;
    TVector<TString> Parameters; // For example, Decimal precision and scale.
    ui32 OptionalDepth = 0;
    bool Binding = false; // Name contains a '$'-prefixed type reference.
    TVector<TTypeMember> Members; // Struct and Variant fields.
    TVector<TTypeTag> Tags; // Enum, Resource and Tagged type arguments.
};

struct TTypeMember {
    std::optional<TTypeTag> Name; // Absent for positional Variant alternatives.
    TTypeName Type;
};

struct TCast {
    TExpressionPtr Value;
    TTypeName Type;
    bool Bitcast = false;
};

struct TSubquery {
    TQueryPtr Query;
};

struct TExists {
    TQueryPtr Query;
};

struct TBetween {
    TExpressionPtr Value;
    TExpressionPtr Lower;
    TExpressionPtr Upper;
    bool Negated = false;
    bool Symmetric = false;
};

struct TIn {
    TExpressionPtr Value;
    TExpressionPtr Collection;
    bool Negated = false;
    bool Compact = false;
};

enum class EPatternOp { Like, ILike, Glob, Regexp, RLike, Match };
struct TPatternMatch {
    EPatternOp Op;
    TExpressionPtr Value;
    TExpressionPtr Pattern;
    TExpressionPtr Escape;
    bool Negated = false;
};

struct TExpression {
    TSourceRange Range;
    std::variant<TLiteral, TName, TParameter, TStar, TUnary, TBinary, TCall, TMember, TIndex,
        TTuple, TList, TCase, TCast, TSubquery, TExists, TBetween, TIn, TPatternMatch> Value;
};

struct TTable {
    TIdentifier Name;
    std::optional<TIdentifier> Cluster;
    std::optional<TIdentifier> Provider;
    bool Binding = false; // Name refers to a '$'-prefixed named expression.
    bool Anonymous = false; // '@' table prefix.
};

struct TDerivedTable {
    TQueryPtr Query;
};

enum class EJoinKind { Inner, Cross, Left, Right, Full, LeftOnly, RightOnly, LeftSemi, RightSemi, Exclusion };
struct TJoin {
    EJoinKind Kind;
    TSourcePtr Left;
    TSourcePtr Right;
    TExpressionPtr On;
    TVector<TIdentifier> Using;
    bool Natural = false;
    bool LeftAny = false;
    bool RightAny = false;
};

enum class ESamplingMode { Sample, Bernoulli, System };
struct TSampling {
    TSourceRange Range;
    ESamplingMode Mode = ESamplingMode::Sample;
    // SAMPLE uses a fraction; TABLESAMPLE uses a percentage. Keep the mode
    // explicit rather than normalizing these expressions during parsing.
    TExpressionPtr Rate;
    TExpressionPtr Seed; // REPEATABLE, absent when not specified.
};

enum class EHintValueForm { None, Scalar, List };
struct TGenericTableHint {
    TIdentifier Name;
    EHintValueForm ValueForm = EHintValueForm::None;
    TVector<TTypeTag> Values;
};

enum class EColumnNullability { Default, Nullable, NotNull };
struct TSchemaColumn {
    TSourceRange Range;
    TTypeTag Name;
    TTypeName Type;
    EColumnNullability Nullability = EColumnNullability::Default;
};

enum class ESchemaHintKind { Schema, Columns };
struct TSchemaHint {
    ESchemaHintKind Kind = ESchemaHintKind::Schema;
    std::variant<TTypeName, TVector<TSchemaColumn>> Value;
};

struct TWatermarkHint {
    TExpressionPtr Expression;
};

struct TTableHint {
    TSourceRange Range;
    std::variant<TGenericTableHint, TSchemaHint, TWatermarkHint> Value;
};

struct TSource {
    TSourceRange Range;
    std::variant<TTable, TDerivedTable, TJoin> Value;
    std::optional<TIdentifier> Alias;
    TVector<TIdentifier> ColumnAliases;
    std::optional<TSampling> Sampling;
    TVector<TTableHint> Hints; // Preserve order and repeated hint names.
};

enum class EGroupingKind { Ordinary, Rollup, Cube, Sets };
struct TGroupingElement {
    TSourceRange Range;
    EGroupingKind Kind = EGroupingKind::Ordinary;
    TNamedExpression Expression; // Only for Ordinary.
    TVector<TGroupingElement> Elements;
};

struct TSelect {
    ESetQuantifier Quantifier = ESetQuantifier::Default;
    TVector<TNamedExpression> Columns;
    TSourcePtr From;
    TExpressionPtr Where;
    TVector<TGroupingElement> GroupBy;
    bool CompactGroupBy = false;
    ESetQuantifier GroupQuantifier = ESetQuantifier::Default;
    std::optional<TIdentifier> GroupHint;
    TExpressionPtr Having;
    TVector<TWindowDefinition> Windows;
    TVector<TSortSpecification> OrderBy;
    bool AssumeOrderBy = false;
};

enum class ESetOperation { Union, Intersect, Except };
struct TSetOperation {
    ESetOperation Op;
    ESetQuantifier Quantifier = ESetQuantifier::Default;
    TQueryPtr Left;
    TQueryPtr Right;
};

struct TCte {
    TIdentifier Name;
    TVector<TIdentifier> Columns;
    TQueryPtr Query;
    bool Recursive = false;
};

struct TQuery {
    TSourceRange Range;
    TVector<TCte> With;
    std::variant<TSelect, TSetOperation> Body;
    TExpressionPtr Limit;
    TExpressionPtr Offset;
};

struct TQueryStatement {
    TQueryPtr Query;
};

struct TPragmaValue {
    TSourceRange Range;
    std::variant<TIdentifier, TLiteral, TParameter> Value;
};

enum class EPragmaValueForm { None, Assignment, List };
struct TPragmaStatement {
    std::optional<TIdentifier> Namespace;
    TIdentifier Name;
    EPragmaValueForm ValueForm = EPragmaValueForm::None;
    TVector<TPragmaValue> Values;
};

enum class EExplainMode { None, Explain, QueryPlan };
struct TStatement {
    TSourceRange Range;
    EExplainMode Explain = EExplainMode::None;
    std::variant<TQueryStatement, TPragmaStatement> Value;
};

struct TScript {
    TString File;
    bool AnsiLexer = false;
    TVector<TStatement> Statements; // Includes pragmas in source order.
};

} // namespace NKikimr::NKqp::NNewYql
