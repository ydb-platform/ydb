#pragma once

#include <yql/essentials/sql/v1/complete/core/statement.h>
#include <yql/essentials/sql/v1/complete/name/service/ranking/frequency.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/hash.h>

namespace NSQLComplete {

    struct TNameSet {
        TVector<TString> Pragmas;
        TVector<TString> Types;
        TVector<TString> Functions;
        THashMap<EStatementKind, TVector<TString>> Hints;
    };

    TNameSet Pruned(TNameSet names, const TFrequencyData& frequency);

    TNameSet LoadDefaultNameSet();

    // TODO(YQL-19747): Mirate YDB CLI to LoadDefaultNameSet
    TNameSet MakeDefaultNameSet();

} // namespace NSQLComplete
