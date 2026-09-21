#include "volume_grow.h"

#include "partition_direct_actor.h"

#include <ydb/core/nbs/cloud/blockstore/config/config.h>
#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/region_geometry.h>

#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/protos/base.pb.h>
#include <ydb/core/protos/blockstore_config.pb.h>

#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

#include <util/generic/ptr.h>
#include <util/generic/string.h>

#include <memory>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NActors;
using namespace NKikimr;

namespace {

// Inputs for a bulk DDisk claim. Pool names and tablet id go on the BSC
// record; VChunkSize is used to compute TargetNumVChunks.
struct TDiskClaimParams
{
    ui64 TabletId = 0;
    TString DDiskPoolName;
    TString PersistentBufferDDiskPoolName;
    ui64 VChunkSize = 0;
};

void AddClaimQueries(
    NKikimrBlobStorage::TEvControllerAllocateDDiskBlockGroup* record,
    ui64 blockCount,
    ui32 blockSize,
    ui64 vChunkSize)
{
    const ui64 regionsCount = GetRegionCount(blockCount, blockSize, vChunkSize);
    const ui32 vChunkPerDbgCount = GetVChunkCountPerDirectBlockGroup(
        regionsCount,
        DefaultVolumeDirectBlockGroupCount);
    for (size_t i = 0; i < DefaultVolumeDirectBlockGroupCount; ++i) {
        auto* query = record->AddQueries();
        query->SetDirectBlockGroupId(i);
        query->SetTargetNumVChunks(vChunkPerDbgCount);
    }
}

// Why a post-create UpdateVolumeConfig is or is not a grow.
enum class EGrowCheck
{
    // Requested size is larger: claim extra vchunks.
    Grow,
    // Requested version is older than persisted.
    StaleVersion,
    // Block size differs; grow ignores it.
    BlockSizeChange,
    // Requested size is smaller.
    Shrink,
    // Same size; version-only bump.
    SameSize,
};

// Grow iff newBlockCount > current and version/block-size are acceptable.
EGrowCheck ShouldGrowVolume(
    const NKikimrBlockStore::TVolumeConfig& current,
    const NKikimrBlockStore::TVolumeConfig& requested,
    ui64 newBlockCount,
    ui64 currentBlockCount)
{
    if (requested.GetVersion() < current.GetVersion()) {
        return EGrowCheck::StaleVersion;
    }
    if (requested.HasBlockSize() &&
        requested.GetBlockSize() != current.GetBlockSize())
    {
        return EGrowCheck::BlockSizeChange;
    }
    if (newBlockCount < currentBlockCount) {
        return EGrowCheck::Shrink;
    }
    if (newBlockCount == currentBlockCount) {
        return EGrowCheck::SameSize;
    }
    return EGrowCheck::Grow;
}

// Builds a Queries claim for every DirectBlockGroup: TargetNumVChunks is the
// per-group count for blockCount. Idempotent: BSC only claims the delta.
NKikimrBlobStorage::TEvControllerAllocateDDiskBlockGroup MakeDDiskClaimRecord(
    const TDiskClaimParams& params,
    ui64 blockCount,
    ui32 blockSize)
{
    NKikimrBlobStorage::TEvControllerAllocateDDiskBlockGroup record;
    record.SetDDiskPoolName(params.DDiskPoolName);
    record.SetPersistentBufferDDiskPoolName(
        params.PersistentBufferDDiskPoolName);
    record.SetTabletId(params.TabletId);
    AddClaimQueries(&record, blockCount, blockSize, params.VChunkSize);
    return record;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleGrowUpdateVolumeConfig(
    const NKikimr::TEvBlockStore::TEvUpdateVolumeConfig::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    if (VolumeGrowInFlight) {
        ReplyUpdateVolumeConfig(
            ctx,
            ev->Sender,
            ev->Cookie,
            ev->Get()->Record.GetTxId(),
            NKikimrBlockStore::ERROR_UPDATE_IN_PROGRESS);
        return;
    }
    if (AddHostInFlight.has_value() || RemoveHostInFlight.has_value()) {
        ReplyUpdateVolumeConfig(
            ctx,
            ev->Sender,
            ev->Cookie,
            ev->Get()->Record.GetTxId(),
            NKikimrBlockStore::ERROR_UPDATE_IN_PROGRESS);
        return;
    }

    const auto& volumeConfig = ev->Get()->Record.GetVolumeConfig();
    const ui64 txId = ev->Get()->Record.GetTxId();

    if (volumeConfig.PartitionsSize() != 1) {
        LOG_CRIT(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s UpdateVolumeConfig with %u partitions, expected 1; "
            "replying OK so SchemeShard does not abort",
            LogTitle.GetWithTime().c_str(),
            volumeConfig.PartitionsSize());
        ReplyUpdateVolumeConfig(
            ctx,
            ev->Sender,
            ev->Cookie,
            ev->Get()->Record.GetTxId(),
            NKikimrBlockStore::OK);
        return;
    }

    const ui64 newBlockCount = volumeConfig.GetPartitions(0).GetBlockCount();
    const ui64 currentBlockCount =
        VolumeConfig.PartitionsSize()
            ? VolumeConfig.GetPartitions(0).GetBlockCount()
            : 0;

    const EGrowCheck check = ShouldGrowVolume(
        VolumeConfig,
        volumeConfig,
        newBlockCount,
        currentBlockCount);
    if (check != EGrowCheck::Grow) {
        if (check == EGrowCheck::StaleVersion) {
            LOG_INFO(
                ctx,
                NKikimrServices::NBS_PARTITION,
                "%s Ignoring stale UpdateVolumeConfig version %u (have %u)",
                LogTitle.GetWithTime().c_str(),
                volumeConfig.GetVersion(),
                VolumeConfig.GetVersion());
        } else if (check == EGrowCheck::BlockSizeChange) {
            LOG_INFO(
                ctx,
                NKikimrServices::NBS_PARTITION,
                "%s Ignoring UpdateVolumeConfig block size change %u -> %u",
                LogTitle.GetWithTime().c_str(),
                VolumeConfig.GetBlockSize(),
                volumeConfig.GetBlockSize());
        } else if (check == EGrowCheck::Shrink) {
            LOG_CRIT(
                ctx,
                NKikimrServices::NBS_PARTITION,
                "%s Rejected UpdateVolumeConfig that is not a grow: "
                "currentBlockCount=%lu newBlockCount=%lu",
                LogTitle.GetWithTime().c_str(),
                currentBlockCount,
                newBlockCount);
        }
        ReplyUpdateVolumeConfig(
            ctx,
            ev->Sender,
            ev->Cookie,
            ev->Get()->Record.GetTxId(),
            NKikimrBlockStore::OK);
        return;
    }

    VolumeGrowInFlight = TVolumeGrowInFlight{
        .VolumeConfig = volumeConfig,
        .ReplyTo = ev->Sender,
        .Cookie = ev->Cookie,
        .TxId = txId,
    };

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Growing %lu -> %lu blocks",
        LogTitle.GetWithTime().c_str(),
        currentBlockCount,
        newBlockCount);

    auto request = std::make_unique<
        TEvBlobStorage::TEvControllerAllocateDDiskBlockGroup>();
    request->Record = MakeDDiskClaimRecord(
        TDiskClaimParams{
            .TabletId = TabletID(),
            .DDiskPoolName = StorageConfig->GetDDiskPoolName(),
            .PersistentBufferDDiskPoolName =
                StorageConfig->GetPersistentBufferDDiskPoolName(),
            .VChunkSize = StorageConfig->GetVChunkSize(),
        },
        volumeConfig.GetPartitions(0).GetBlockCount(),
        volumeConfig.GetBlockSize());
    SendToBsc(ctx, THolder<IEventBase>(request.release()));
}

void TPartitionActor::HandleGrowAllocateResult(
    const TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult::TPtr& ev,
    const TActorContext& ctx)
{
    Y_ABORT_UNLESS(VolumeGrowInFlight);

    const auto& record = ev->Get()->Record;
    const bool failed = record.GetStatus() != NKikimrProto::OK;
    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Grow OnBscAllocateResult status=%s reason=%s",
        LogTitle.GetWithTime().c_str(),
        NKikimrProto::EReplyStatus_Name(record.GetStatus()).c_str(),
        record.GetErrorReason().c_str());

    if (failed) {
        LOG_ERROR(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s Resize DDisk allocation failed: %s %s",
            LogTitle.GetWithTime().c_str(),
            NKikimrProto::EReplyStatus_Name(record.GetStatus()).c_str(),
            record.GetErrorReason().c_str());
        ReplyUpdateVolumeConfig(
            ctx,
            VolumeGrowInFlight->ReplyTo,
            VolumeGrowInFlight->Cookie,
            VolumeGrowInFlight->TxId,
            NKikimrBlockStore::ERROR_UPDATE_IN_PROGRESS);
        VolumeGrowInFlight.reset();
        return;
    }

    ExecuteTx(
        ctx,
        CreateTx<TStoreVolumeConfig>(VolumeGrowInFlight->VolumeConfig));
}

void TPartitionActor::FinishVolumeGrow(const TActorContext& ctx)
{
    Y_ABORT_UNLESS(VolumeGrowInFlight);

    const auto grow = std::move(*VolumeGrowInFlight);
    VolumeGrowInFlight.reset();

    ReplyUpdateVolumeConfig(
        ctx,
        grow.ReplyTo,
        grow.Cookie,
        grow.TxId,
        NKikimrBlockStore::OK);

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Grown config persisted, restarting tablet",
        LogTitle.GetWithTime().c_str());
    ctx.Send(Tablet(), new TEvents::TEvPoisonPill());
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
