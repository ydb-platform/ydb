#pragma once

#include "node.h"

namespace NSQLTranslationV1 {

struct TYqlSourceAlias {
    TString Name;
    TVector<TString> Columns;
};

struct TYqlSource {
    TNodePtr Node;
    TMaybe<TYqlSourceAlias> Alias;
};

enum class EYqlJoinKind {
    Cross,
    Inner,
    Left,
    Right,
};

struct TYqlJoinConstraint {
    EYqlJoinKind Kind;
    TNullable<TNodePtr> Condition;
};

struct TYqlJoin {
    TVector<TYqlSource> Sources;
    TVector<TYqlJoinConstraint> Constraints;
};

struct TPlainAsterisk {};

using TProjection = std::variant<
    TVector<TNodePtr>,
    TPlainAsterisk>;

struct TGroupBy {
    TVector<TNodePtr> Keys;
};

struct TOrderBy {
    TVector<TSortSpecificationPtr> Keys;
};

struct TYqlTableRefArgs {
    TString Service;
    TString Cluster;
    TString Key;
    bool IsAnonymous = false;
};

struct TYqlValuesArgs {
    TVector<TVector<TNodePtr>> Rows;
};

struct TYqlSetItemArgs {
    TPosition Position;
    TProjection Projection;
    TMaybe<TYqlJoin> Source;
    TMaybe<TNodePtr> Where;
    TMaybe<TGroupBy> GroupBy;
    TMaybe<TNodePtr> Having;
    TMaybe<TOrderBy> OrderBy;
    TMaybe<TNodePtr> Limit;
    TMaybe<TNodePtr> Offset;
};

enum class EYqlSetOp {
    Push = 0,
    Union,
    UnionAll,
    Except,
    ExceptAll,
    Intersect,
    IntersectAll,
};

struct TYqlSelectArgs {
    TVector<TYqlSetItemArgs> SetItems;
    TVector<EYqlSetOp> SetOps;
    TMaybe<TOrderBy> OrderBy;
    TMaybe<TNodePtr> Limit;
    TMaybe<TNodePtr> Offset;
};

EYqlSetOp AllQualified(EYqlSetOp op);

TNodePtr GetYqlSource(const TNodePtr& node);

TNodePtr ToTableExpression(TNodePtr source);

TYqlSelectArgs DestructYqlSelect(TNodePtr node);

TNodePtr BuildYqlTableRef(TPosition position, TYqlTableRefArgs&& args);

TNodePtr BuildYqlValues(TPosition position, TYqlValuesArgs&& args);

TNodePtr BuildYqlSelect(TPosition position, TYqlSelectArgs&& args);

TNodePtr WrapYqlSelectSubExpr(TNodePtr node);

TNodePtr BuildYqlScalarSubquery(TNodePtr node);

TNodePtr BuildYqlExistsSubquery(TNodePtr node);

TNodePtr BuildYqlInSubquery(TNodePtr node, TNodePtr expression);

TNodePtr BuildYqlStatement(TNodePtr node);

} // namespace NSQLTranslationV1
