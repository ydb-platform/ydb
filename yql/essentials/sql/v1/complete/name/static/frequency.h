#pragma once

#include <util/generic/string.h>
#include <util/generic/hash.h>

namespace NSQLComplete {

    struct TFrequencyData {
        THashMap<TString, size_t> Pragmas;
        THashMap<TString, size_t> Types;
        THashMap<TString, size_t> Functions;
        THashMap<TString, size_t> Hints;
    };

    TFrequencyData ParseJsonFrequencyData(const TStringBuf text);

    TFrequencyData LoadFrequencyData();

} // namespace NSQLComplete
