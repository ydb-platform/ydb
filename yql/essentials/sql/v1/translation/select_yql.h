#pragma once

#include "node.h"

namespace NSQLTranslationV1 {

struct TFlattenByArg {
    TNodePtr Column;
    TNodePtr Expression;
};

struct TFlattenByArgs {
    TVector<TFlattenByArg> Arguments;
};

struct TYqlColumnRef {
    TPosition Position;
    TString Name;
};

struct TYqlSourceAlias {
    enum class EKind {
        Subquery,
        CTE,
        IntoValues,
    };

    TPosition Position;
    TString Name;
    TVector<TYqlColumnRef> Columns;
    EKind Kind = EKind::Subquery;
};

struct TYqlSource {
    TNodePtr Node;
    TMaybe<TYqlSourceAlias> Alias;
    TNodePtr Flatten;
};

enum class EYqlJoinKind {
    Cross,
    Inner,
    Full,
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

using TProjection = TVector<TNodePtr>;

struct TYqlWithout {
    struct TColumn {
        TPosition Position;
        TString Source;
        TString Name;
    };

    TVector<TColumn> Columns;
    bool IsIfExists = false;
};

struct TGroupingSets {
    struct TRollup {
        TVector<TNodePtr> Expressions;
    };

    struct TCube {
        TVector<TNodePtr> Expressions;
    };

    TVector<TVector<TNodePtr>> Sets;
};

struct TGroupBy {
    using TElement = std::variant<
        /* Ordinary */ TNodePtr,
        TGroupingSets,
        TGroupingSets::TRollup,
        TGroupingSets::TCube>;

    TVector<TElement> Elements;
    bool IsCompact = false;
};

struct TOrderBy {
    TVector<TSortSpecificationPtr> Keys;
};

struct TWindow {
    TString Name;
    TVector<TNodePtr> PartitionBy;
    TMaybe<TOrderBy> OrderBy;
    TFrameSpecificationPtr Frame;
};

struct TYqlTableRefArgs {
    TString Service;
    TDeferredAtom Cluster;
    TDeferredAtom Key;
    TViewDescription View;
    bool IsAnonymous = false;
};

struct TYqlValuesArgs {
    TVector<TVector<TNodePtr>> Rows;
};

struct TYqlSetItemArgs {
    TPosition Position;
    bool Distinct = false;
    TProjection Projection;
    TMaybe<TYqlWithout> Without;
    TMaybe<TYqlJoin> Source;
    TMaybe<TNodePtr> Where;
    TMaybe<TGroupBy> GroupBy;
    TMaybe<TNodePtr> Having;
    TMap<TString, TWindow> Windows;
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

TSourcePtr ToTableExpression(TYqlSource source);

TNodePtr ToTableExpression(TNodePtr source);

TYqlSelectArgs DestructYqlSelect(TNodePtr node);

TNodePtr BuildYqlTableRef(TPosition position, TYqlTableRefArgs&& args);

TNodePtr BuildYqlSelf(TPosition position);

TNodePtr BuildYqlFlattenColumns(TPosition position);

TNodePtr BuildYqlFlattenBy(
    TPosition position,
    TString mode,
    TFlattenByArgs&& args);

TSourcePtr BuildYqlValues(TPosition position, TYqlValuesArgs&& args);

TSourcePtr BuildYqlSelect(TPosition position, TYqlSelectArgs&& args);

TNodePtr WrapYqlSelectSubExpr(TNodePtr node);

TNodePtr BuildYqlScalarSubquery(TNodePtr node);

TNodePtr BuildYqlExistsSubquery(TNodePtr node);

TNodePtr BuildYqlInSubquery(TNodePtr node, TNodePtr expression);

TNodePtr BuildYqlStar(TPosition position, TMaybe<TString> source = Nothing());

TNodePtr BuildYqlStatement(TNodePtr node);

} // namespace NSQLTranslationV1
