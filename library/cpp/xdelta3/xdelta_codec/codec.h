#pragma once

#include <util/system/types.h>

#include <string.h>

struct _xdelta_context {
  void* opaque;
  void* (*allocate)(void* opaque, size_t size);
  void (*free)(void* opaque, void* ptr);
};
typedef struct _xdelta_context XDeltaContext;

#ifdef __cplusplus
namespace NXdeltaAggregateColumn {
extern "C" {
#endif

ui8* ApplyPatch(
    XDeltaContext* context,
    size_t headroomSize,
    const ui8* base,
    size_t baseSize,
    const ui8* patch,
    size_t patchSize,
    size_t stateSize,
    size_t* resultSize);

ui8* ComputePatch(
    XDeltaContext* context,
    const ui8* from,
    size_t fromSize,
    const ui8* to,
    size_t toSize,
    size_t* patchSize);

ui8* MergePatches(
    XDeltaContext* context,
    size_t headroomSize,
    const ui8* patch1,
    size_t patch1_size,
    const ui8* patch2,
    size_t patch2_size,
    size_t* patch3_size);

#ifdef __cplusplus
}
}
#endif
