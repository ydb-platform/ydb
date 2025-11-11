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

struct TYqlValuesArgs {
    TVector<TVector<TNodePtr>> Rows;
};

struct TYqlSelectArgs {
    TVector<TNodePtr> Terms;
    TMaybe<TYqlSource> Source;
};

TNodePtr BuildYqlValues(TPosition position, TYqlValuesArgs&& args);

TNodePtr BuildYqlSelect(TPosition position, TYqlSelectArgs&& args);

} // namespace NSQLTranslationV1
