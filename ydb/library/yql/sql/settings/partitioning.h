#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NSQLTranslation {
    // returns error message if any
    TString ParsePartitionedByBinding(const TString& name, const TString& value, TVector<TString>& columns);
}  // namespace NSQLTranslation
