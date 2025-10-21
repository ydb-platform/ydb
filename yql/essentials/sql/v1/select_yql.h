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

struct TPlainAsterisk {};

using TProjection = std::variant<
    TVector<TNodePtr>,
    TPlainAsterisk>;

struct TOrderBy {
    TVector<TSortSpecificationPtr> Keys;
};

struct TYqlTableRefArgs {
    TString Service;
    TString Cluster;
    TString Key;
};

struct TYqlValuesArgs {
    TVector<TVector<TNodePtr>> Rows;
};

struct TYqlSelectArgs {
    TProjection Projection;
    TMaybe<TYqlSource> Source;
    TMaybe<TNodePtr> Where;
    TMaybe<TNodePtr> Limit;
    TMaybe<TNodePtr> Offset;
    TMaybe<TOrderBy> OrderBy;
};

TNodePtr BuildYqlTableRef(TPosition position, TYqlTableRefArgs&& args);

TNodePtr BuildYqlValues(TPosition position, TYqlValuesArgs&& args);

TNodePtr BuildYqlSelect(TPosition position, TYqlSelectArgs&& args);

TNodePtr BuildYqlStatement(TNodePtr node);

} // namespace NSQLTranslationV1
