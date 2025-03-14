#pragma once

#include <util/system/defaults.h>


namespace NKikimr {
namespace NMiniKQL {

struct TRunParams {
    size_t RowsPerRun = 0;
    size_t NumRuns = 0;
    size_t MaxKey = 0; // for numeric keys, the range is [0..MaxKey]
    bool LongStringKeys = false;
};

}
}
