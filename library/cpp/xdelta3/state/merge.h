#pragma once

#include <library/cpp/xdelta3/xdelta_codec/codec.h>

#include <util/system/types.h>

#include <string.h>

#ifdef __cplusplus
namespace NXdeltaAggregateColumn {
extern "C" {
#endif

// total Data size = Offset + Size
struct TSpan {
    const ui8* Data;
    size_t Offset;
    size_t Size;
};

int MergeStates(
    XDeltaContext* context,
    const ui8* lhsSata,
    size_t lhsSize,
    const ui8* rhsData,
    size_t rhsSize,
    struct TSpan* result);

#ifdef __cplusplus
}
}
#endif
