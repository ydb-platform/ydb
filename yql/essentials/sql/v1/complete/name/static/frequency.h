#pragma once

#include <util/generic/string.h>
#include <util/generic/hash.h>

namespace NSQLComplete {

    struct TFrequencyData {
        THashMap<TString, size_t> Types;
        THashMap<TString, size_t> Functions;
    };

    TFrequencyData ParseJsonFrequencyData(const TStringBuf text);

    TFrequencyData LoadFrequencyData();

} // namespace NSQLComplete
