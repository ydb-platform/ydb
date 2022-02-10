#pragma once
#include "defs.h"

namespace NKikimr {
namespace NKeyValue {

struct TKeyRange {
    bool HasFrom;
    TString KeyFrom;
    bool IncludeFrom;
    bool HasTo;
    TString KeyTo;
    bool IncludeTo;

    static TKeyRange WholeDatabase() {
        return {
                false, // HasFrom
                {},    // KeyFrom
                false,    // IncludeFrom
                false, // HasTo
                {},    // KeyTo
                false     // IncludeTo
            };
    };
};

} // NKeyValue
} // NKikimr
