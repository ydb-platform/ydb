#pragma once

#include "nbs_dbg_like_load_defs.h"

#include <ydb/core/protos/load_test.pb.h>

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

} // namespace NKikimr::NNbsDbgLike
