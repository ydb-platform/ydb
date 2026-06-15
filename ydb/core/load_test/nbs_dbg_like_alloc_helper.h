#pragma once

#include "nbs_dbg_like_load_defs.h"

#include <ydb/core/protos/base.pb.h>
#include <ydb/core/protos/blobstorage.pb.h>
#include <ydb/core/protos/load_test.pb.h>

#include <util/generic/string.h>

#include <expected>
#include <vector>

namespace NKikimr::NNbsDbgLike {

ui32 GetHostsPerDbg(const TEvLoadTestRequest::TNbsDbgLikeLoad::TAllocConfig& cfg);

// Builds a TEvControllerAllocateDDiskBlockGroup request body (Queries form).
// When `dealloc=true`, sets TargetNumVChunks=0 for every query (BSC then
// releases the DBG; see ddisk.cpp:516-538).
void BuildAllocateRequest(
    NKikimrBlobStorage::TEvControllerAllocateDDiskBlockGroup& rec,
    const TEvLoadTestRequest::TNbsDbgLikeLoad::TAllocConfig& cfg,
    bool dealloc);

// Parses a BSC alloc reply (Responses/Nodes form, see blobstorage.proto).
// Validates ResponsesSize == cfg.NumDirectBlockGroups and NodesSize == cfg.HostsPerDbg
// (default 5) per response; sets DbgIndex from the request order and
// DirectBlockGroupId from the response. On failure returns a human-readable reason.
std::expected<std::vector<TDirectBlockGroup>, TString> ParseAllocateResult(
    const NKikimrBlobStorage::TEvControllerAllocateDDiskBlockGroupResult& rec,
    const TEvLoadTestRequest::TNbsDbgLikeLoad::TAllocConfig& cfg);

// Address routing parameters derived from a TConfigureTablet run config.
// Shared by the proxy tablet (to route requests) and the per-DBG worker (to
// decode addresses) so they cannot drift apart.
struct TRoutingParams {
    ui32 ActiveDbgs = 0;
    ui32 IoSizeBytes = 0;
    ui64 BytesPerDbg = 0;
    bool IoValid = false;  // true iff the requested IoSizeBytes is usable
};

// Computes routing params from the run config. ActiveDbgs clamps
// NumDirectBlockGroupsToUse to [0, numDbgs]; 0 (and values > numDbgs) mean
// "all", and ActiveDbgs is only 0 when numDbgs is 0. IoValid is true only when
// IoSizeBytes is non-zero, does not exceed VChunkSizeBytes, evenly divides it,
// and TargetNumVChunks is non-zero (so BytesPerDbg > 0); when valid, IoSizeBytes
// and BytesPerDbg are populated.
TRoutingParams ComputeRoutingParams(
    const TEvLoadTestRequest::TNbsDbgLikeLoad::TConfigureTablet& cfg,
    const TEvLoadTestRequest::TNbsDbgLikeLoad::TAllocConfig& allocConfig,
    ui32 numDbgs);

} // namespace NKikimr::NNbsDbgLike
