#include "phantom_flag_storage_state.h"
#include "phantom_flag_storage_processor.h"

#include <ydb/core/util/stlog.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclogmsgreader.h>

#define YDB_LOG_THIS_FILE_COMPONENT BS_PHANTOM_FLAG_STORAGE


namespace NKikimr {

namespace NSyncLog {

TPhantomFlagStorageState::TPhantomFlagStorageState(TIntrusivePtr<TSyncLogCtx> slCtx)
    : SlCtx(slCtx)
    , GType(slCtx->VCtx->Top->GType)
    , Thresholds(GType)
{}

void TPhantomFlagStorageState::InitializePersistent(TPhantomFlagStorageData&& data,
        TActorId syncLogKeeperId, TActorId chunkKeeperId, ui32 appendBlockSize) {
    Persistent = true;

    NActors::IActor* processorActor = CreatePhantomFlagStorageProcessor(std::move(data),
            TPhantomFlagStorageProcessorContext{
                .SyncLogCtx = SlCtx,
                .SyncLogKeeperId = syncLogKeeperId,
                .ChunkKeeperId = chunkKeeperId,
                .AppendBlockSize = appendBlockSize,
            });
    ProcessorId = TActivationContext::Register(processorActor);

    if (!data.Chunks.empty()) {
        Active = true;
        Building = true;
        TActivationContext::Send(new IEventHandle(ProcessorId, syncLogKeeperId, new TEvPhantomFlagStorageGetSnapshot));
    }
}

void TPhantomFlagStorageState::StartBuilding() {
    if (GType.BlobSubgroupSize() > MaxExpectedDisksInGroup) {
        YDB_LOG_ERROR(VDISKP(SlCtx->VCtx, "Attempted to start phantom flag storage building on unsupported configuration"),
            {"marker", "BSPFS01"},
            {"maxExpectedDisksInGroup", MaxExpectedDisksInGroup},
            {"groupSize", GType.BlobSubgroupSize()});
        // PhantomFlagStorage doesn't work with weird group configurations to minimize memory consumption
        return;
    }
    Active = true;
    Building = true;
}

void TPhantomFlagStorageState::ProcessBlobRecordFromSyncLog(const TLogoBlobRec* blobRec, ui64 sizeLimit,
        ui64 blobSizeLimit) {
    Y_DEBUG_ABORT_UNLESS(!Persistent);
    BlobSizeLimit = blobSizeLimit;
    AdjustSize(sizeLimit);
    if (!Active) {
        return;
    }

    if (blobRec->Ingress.IsDoNotKeep(GType) &&
            (Building || Thresholds.IsBehindThresholdOnUnsynced(blobRec->LogoBlobID(), SyncedMask))) {
        YDB_LOG_DEBUG_COMP(BS_PHANTOM_FLAG_STORAGE, VDISKP(SlCtx->VCtx, "Try to add DoNotKeepFlag flag to PhantomFlagStorage"),
            {"marker", "BSPFS09"},
            {"blobId", blobRec->LogoBlobID()},
            {"building", Building},
            {"syncedMask", SyncedMask.to_ullong()},
            {"thresholds", Thresholds});
        AddFlag(*blobRec);
    }
}

void TPhantomFlagStorageState::ProcessBlobRecordFromNeighbour(ui32 orderNumber, const TLogoBlobRec* blobRec) {
    if (blobRec->Ingress.IsKeep(GType)) {
        Thresholds.AddBlob(orderNumber, blobRec->LogoBlobID());
    }
}

void TPhantomFlagStorageState::ProcessBarrierRecordFromNeighbour(ui32 orderNumber, const TBarrierRec* barrierRec) {
    if (barrierRec->Hard) {
        Thresholds.AddHardBarrier(orderNumber, barrierRec->TabletId, barrierRec->Channel,
                barrierRec->CollectGeneration, barrierRec->CollectStep);
    }
}

void TPhantomFlagStorageState::FinishInitialBuilding(TPhantomFlags&& flags, TPhantomFlagThresholds&& thresholds,
        ui64 sizeLimit, ui64 blobSizeLimit) {
    if (!Active) {
        // PhantomFlagStorage was deactivated while building, do nothing
        return;
    }

    BlobSizeLimit = blobSizeLimit;

    if (Persistent) {
        std::vector<TPhantomFlagStorageItem> items;
        items.reserve(flags.size() + thresholds.GetList().size());
        for (const TLogoBlobRec& rec : flags) {
            items.push_back(TPhantomFlagStorageItem::CreateFlag(&rec));
        }
        std::vector<TPhantomFlagThresholds::TThreshold> thresholdList = thresholds.GetList();
        for (const auto [tabletId, channel, generation, step, orderNumber] : thresholdList) {
            items.push_back(TPhantomFlagStorageItem::CreateThreshold(orderNumber,
                    tabletId, channel, generation, step));
        }
        if (!items.empty()) {
            TActivationContext::Send(new IEventHandle(ProcessorId, TActorId{},
                    new TEvPhantomFlagStorageWriteItems(std::move(items))));
        }
        Thresholds.Merge(std::move(thresholds));
    } else {
        AdjustSize(sizeLimit);
        ui64 flagsAdded = 0;
        for (const TLogoBlobRec& rec : flags) {
            if (!AddFlag(rec)) {
                break;
            }
            ++flagsAdded;
        }
        Thresholds.Merge(std::move(thresholds));

        YDB_LOG_DEBUG(VDISKP(SlCtx->VCtx, "Finish building"),
            {"marker", "BSPFS06"},
            {"flagsAdded", flagsAdded},
            {"flagsReceived", flags.size()});
    }

    Building = false;
}

void TPhantomFlagStorageState::Recover(TPhantomFlagThresholds&& thresholdsBatch, bool eof) {
    YDB_LOG_DEBUG(VDISKP(SlCtx->VCtx, "Recovering PhantomFlagStorage"),
        {"marker", "BSPFS10"},
        {"eof", eof});
    Thresholds.Merge(std::move(thresholdsBatch));
    if (eof) {
        Building = false;
    }
}

void TPhantomFlagStorageState::Deactivate() {
    YDB_LOG_NOTICE(VDISKP(SlCtx->VCtx, "Deactivating PhantomFlagStorage"),
        {"marker", "BSPFS07"},
        {"flagsDropped", StoredFlags.size()});
    Thresholds.Clear();
    Active = false;
    Building = false;
    if (Persistent) {
        TActivationContext::Send(new IEventHandle(ProcessorId, TActorId{}, new TEvPhantomFlagStorageDrop));
    } else {
        StoredFlags.clear();
    }
}

void TPhantomFlagStorageState::RequestSnapshot(TEvPhantomFlagStorageGetSnapshot::TPtr ev) const {
    if (Persistent) {
        TActivationContext::Send(ev->Forward(ProcessorId));
    } else {
        YDB_LOG_DEBUG(VDISKP(SlCtx->VCtx, "Acquiring snapshot"),
            {"marker", "BSPFS05"},
            {"flagsCount", StoredFlags.size()});
        auto res = std::make_unique<TEvPhantomFlagStorageGetSnapshotResult>(
                TPhantomFlags(StoredFlags),
                TPhantomFlagThresholds(Thresholds),
                std::unordered_set<ui32>{},
                /*eof=*/true);
        TActivationContext::Send(new IEventHandle(ev->Sender, ev->Recipient, res.release()));
    }
}

bool TPhantomFlagStorageState::IsActive() const {
    return Active;
}

bool TPhantomFlagStorageState::IsPersistent() const {
    return Persistent;
}

TActorId TPhantomFlagStorageState::GetProcessorId() const {
    return ProcessorId;
}

TPhantomFlagThresholds TPhantomFlagStorageState::GetThresholdsCopy() {
    return Thresholds;
}

void TPhantomFlagStorageState::ProcessLocalSyncData(ui32 orderNumber, const TString& data) {
    if (!Active) {
        return;
    }

    auto blobHandler = [&] (const NSyncLog::TLogoBlobRec* rec) {
        ProcessBlobRecordFromNeighbour(orderNumber, rec);
    };
    auto blockHandler = [&] (const NSyncLog::TBlockRec*) {
        // nothing to do
    };
    auto barrierHandler = [&] (const NSyncLog::TBarrierRec* rec) {
        ProcessBarrierRecordFromNeighbour(orderNumber, rec);
    };
    auto blockHandlerV2 = [&](const NSyncLog::TBlockRecV2*) {
        // nothing to do
    };

    // process synclog data
    NSyncLog::TFragmentReader fragment(data);
    fragment.ForEach(blobHandler, blockHandler, barrierHandler, blockHandlerV2);
}


ui64 TPhantomFlagStorageState::EstimateFlagsMemoryConsumption() const {
    return StoredFlags.capacity() * sizeof(decltype(StoredFlags)::value_type);
}

ui64 TPhantomFlagStorageState::EstimateThresholdsMemoryConsumption() const {
    return Thresholds.EstimatedMemoryConsumption();
}

void TPhantomFlagStorageState::AdjustSize(ui64 sizeLimit) {
    ui32 newCapacity = sizeLimit / sizeof(decltype(StoredFlags)::value_type);
    if (newCapacity > MaxFlagsStoredCount) {
        StoredFlags.reserve(newCapacity);
        YDB_LOG_DEBUG(VDISKP(SlCtx->VCtx, "Reserving additional space for PhantomFlagStorage"),
            {"marker", "BSPFS03"},
            {"oldCapacity", MaxFlagsStoredCount},
            {"newCapacity", newCapacity},
            {"actualCapacity", StoredFlags.capacity()});
    } else if (newCapacity < MaxFlagsStoredCount) {
        ui32 flagsDropped = 0;
        if (newCapacity < StoredFlags.size()) {
            flagsDropped = StoredFlags.size() - newCapacity;
            StoredFlags = TPhantomFlags(StoredFlags.begin(), StoredFlags.begin() + newCapacity);
        }
        StoredFlags.shrink_to_fit();
        StoredFlags.reserve(newCapacity);
        YDB_LOG_DEBUG(VDISKP(SlCtx->VCtx, "Shrinking PhantomFlagStorage"),
            {"marker", "BSPFS04"},
            {"oldCapacity", MaxFlagsStoredCount},
            {"newCapacity", newCapacity},
            {"actualCapacity", StoredFlags.capacity()},
            {"flagsDropped", flagsDropped});
    }
    MaxFlagsStoredCount = newCapacity;
}

bool TPhantomFlagStorageState::AddFlag(const TLogoBlobRec& blobRec) {
    if (BlobSizeLimit && blobRec.LogoBlobID().BlobSize() < BlobSizeLimit) {
        return true;
    }
    if (StoredFlags.size() < StoredFlags.capacity()) {
        StoredFlags.emplace_back(blobRec);
        return true;
    } else {
        YDB_LOG_INFO(VDISKP(SlCtx->VCtx, "Cannot add flag to PhantomFlagStorage, memory limit reached"),
            {"marker", "BSPFS02"},
            {"capacity", StoredFlags.capacity()},
            {"size", StoredFlags.size()},
            {"blobId", blobRec.LogoBlobID()});
        return false;
    }
}

void TPhantomFlagStorageState::UpdateSyncedMask(const TSyncedMask& newSyncedMask) {
    SyncedMask = newSyncedMask;
}

void TPhantomFlagStorageState::UpdateMetrics() {
    SlCtx->PhantomFlagStorageGroup.IsPhantomFlagStorageActive() = Active;
    SlCtx->PhantomFlagStorageGroup.IsPhantomFlagStorageBuilding() = Building;
    SlCtx->PhantomFlagStorageGroup.StoredFlagsCount() = StoredFlags.size();
    ui64 storedFlagsMem = StoredFlags.capacity() * sizeof(decltype(StoredFlags)::value_type);
    SlCtx->PhantomFlagStorageGroup.StoredFlagsMemoryConsumption() = storedFlagsMem;
    SlCtx->PhantomFlagStorageGroup.ThresholdsMemoryConsumption() = Thresholds.EstimatedMemoryConsumption();
}

std::optional<TPhantomFlagStorageData> TPhantomFlagStorageState::GetPersistentData() const {
    return PersistentData;
}

void TPhantomFlagStorageState::UpdatePersistentData(std::optional<TPhantomFlagStorageData>&& data) {
    PersistentData = std::move(data);
}

void TPhantomFlagStorageState::Terminate() {
    if (ProcessorId != TActorId{}) {
        TActivationContext::Send(new IEventHandle(ProcessorId, TActorId{}, new TEvents::TEvPoisonPill));
    }
}

} // namespace NSyncLog

} // namespace NKikimr
