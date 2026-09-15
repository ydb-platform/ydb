#include "nbs_dbg_like_alloc_helper.h"
#include "nbs_dbg_like_load_defs.h"

#include <util/string/builder.h>
#include <algorithm>

namespace NKikimr::NNbsDbgLike {

ui32 GetHostsPerDbg(const TEvLoadTestRequest::TNbsDbgLikeLoad::TAllocConfig& cfg) {
    const ui32 hosts = cfg.GetHostsPerDbg();
    if (hosts == 0) {
        return kHostsPerDbgMax;
    }
    return std::min(hosts, kHostsPerDbgMax);
}

void BuildAllocateRequest(
    NKikimrBlobStorage::TEvControllerAllocateDDiskBlockGroup& rec,
    const TEvLoadTestRequest::TNbsDbgLikeLoad::TAllocConfig& cfg,
    bool dealloc)
{
    rec.SetTabletId(cfg.GetTabletId());
    rec.SetDDiskPoolName(cfg.GetDDiskPoolName());
    rec.SetPersistentBufferDDiskPoolName(cfg.GetPersistentBufferDDiskPoolName());

    const ui32 n = cfg.GetNumDirectBlockGroups();
    const ui32 vChunks = dealloc ? 0u : cfg.GetTargetNumVChunks();
    for (ui32 i = 0; i < n; ++i) {
        auto* q = rec.AddQueries();
        q->SetDirectBlockGroupId(i);
        q->SetTargetNumVChunks(vChunks);
    }
}

std::expected<std::vector<TDirectBlockGroup>, TString> ParseAllocateResult(
    const NKikimrBlobStorage::TEvControllerAllocateDDiskBlockGroupResult& rec,
    const TEvLoadTestRequest::TNbsDbgLikeLoad::TAllocConfig& cfg)
{
    const ui32 hostsPerDbg = GetHostsPerDbg(cfg);
    const auto status = rec.GetStatus();
    if (status != NKikimrProto::OK && status != NKikimrProto::ALREADY) {
        return std::unexpected(TStringBuilder()
            << "BSC alloc status " << NKikimrProto::EReplyStatus_Name(status)
            << " " << rec.GetErrorReason());
    }

    const ui32 expectedN = cfg.GetNumDirectBlockGroups();
    if (rec.ResponsesSize() != expectedN) {
        return std::unexpected(TStringBuilder()
            << "BSC returned " << rec.ResponsesSize()
            << " responses, expected " << expectedN);
    }

    std::vector<TDirectBlockGroup> dbgs;
    dbgs.reserve(expectedN);
    for (size_t i = 0; i < rec.ResponsesSize(); ++i) {
        const auto& resp = rec.GetResponses(i);
        if (resp.NodesSize() != hostsPerDbg) {
            return std::unexpected(TStringBuilder()
                << "BSC response[" << i << "] (DBG "
                << resp.GetDirectBlockGroupId() << ") has "
                << resp.NodesSize() << " nodes, expected " << hostsPerDbg);
        }
        TDirectBlockGroup d;
        d.DbgIndex = static_cast<ui32>(i);
        d.DirectBlockGroupId = resp.GetDirectBlockGroupId();
        for (size_t k = 0; k < hostsPerDbg; ++k) {
            const auto& node = resp.GetNodes(k);
            d.DDiskIds[k].CopyFrom(node.GetDDiskId());
            d.PBIds[k].CopyFrom(node.GetPersistentBufferDDiskId());
        }
        dbgs.push_back(std::move(d));
    }
    return dbgs;
}

TRoutingParams ComputeRoutingParams(
    const TEvLoadTestRequest::TNbsDbgLikeLoad::TConfigureTablet& cfg,
    const TEvLoadTestRequest::TNbsDbgLikeLoad::TAllocConfig& allocConfig,
    ui32 numDbgs)
{
    TRoutingParams params;

    const ui32 want = cfg.GetNumDirectBlockGroupsToUse();
    params.ActiveDbgs = (want == 0 || want > numDbgs) ? numDbgs : want;

    const ui32 wantIo = cfg.GetIoSizeBytes();
    const ui64 vChunkSizeBytes = allocConfig.GetVChunkSizeBytes();
    const ui32 targetNumVChunks = allocConfig.GetTargetNumVChunks();
    const ui64 bytesPerDbg = static_cast<ui64>(targetNumVChunks) * vChunkSizeBytes;
    params.IoValid = wantIo != 0 && vChunkSizeBytes != 0 && bytesPerDbg != 0
        && wantIo <= vChunkSizeBytes && vChunkSizeBytes % wantIo == 0;
    if (params.IoValid) {
        params.IoSizeBytes = wantIo;
        params.BytesPerDbg = bytesPerDbg;
    }

    return params;
}

} // namespace NKikimr::NNbsDbgLike
