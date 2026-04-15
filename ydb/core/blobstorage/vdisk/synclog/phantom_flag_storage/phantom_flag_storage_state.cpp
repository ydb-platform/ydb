#include "phantom_flag_storage_state.h"
#include "phantom_flag_storage_processor.h"

#include <ydb/core/util/stlog.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclogmsgreader.h>

namespace NKikimr {

namespace NSyncLog {

TPhantomFlagStorageState::TPhantomFlagStorageState(TIntrusivePtr<TSyncLogCtx> slCtx)
    : SlCtx(slCtx)
    , GType(slCtx->VCtx->Top->GType)
    , Thresholds(GType)
{}

void TPhantomFlagStorageState::InitializePersistent(TPhantomFlagStorageData&& data,
        TActorId syncLogKeeperId, TActorId chunkKeeperId, ui32 appendBlockSize) {
    IsPersistent = true;

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
        STLOG(PRI_ERROR, BS_PHANTOM_FLAG_STORAGE, BSPFS01,
                VDISKP(SlCtx->VCtx, "Attempted to start phantom flag storage building on unsupported configuration"),
                (MaxExpectedDisksInGroup, MaxExpectedDisksInGroup),
                (GroupSize, GType.BlobSubgroupSize()));
        // PhantomFlagStorage doesn't work with weird group configurations to minimize memory consumption
        return;
    }
    Active = true;
    Building = true;
}

void TPhantomFlagStorageState::ProcessBlobRecordFromSyncLog(const TLogoBlobRec* blobRec, ui64 sizeLimit) {
    AdjustSize(sizeLimit);
    if (!Active) {
        return;
    }

    if (blobRec->Ingress.IsDoNotKeep(GType) &&
            (Building || Thresholds.IsBehindThresholdOnUnsynced(blobRec->LogoBlobID(), SyncedMask))) {
        STLOG(PRI_DEBUG, BS_PHANTOM_FLAG_STORAGE, BSPFS09,
                VDISKP(SlCtx->VCtx, "Try to add DoNotKeepFlag flag to PhantomFlagStorage"),
                (BlobId, blobRec->LogoBlobID().ToString()),
                (Building, Building),
                (SyncedMask, SyncedMask.to_ullong()),
                (Thresholds, Thresholds.ToString()));
        if (IsPersistent) {
            AddItemToWriteBuffer(TPhantomFlagStorageItem::CreateFlag(blobRec));
        } else {
            AddFlag(*blobRec);
        }
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
        ui64 sizeLimit) {
    if (!Active) {
        // PhantomFlagStorage was deactivated while building, do nothing
        return;
    }

    if (IsPersistent) {
        for (const TLogoBlobRec& rec : flags) {
            AddItemToWriteBuffer(TPhantomFlagStorageItem::CreateFlag(&rec));
        }
        std::vector<TPhantomFlagThresholds::TThreshold> thresholdList = thresholds.GetList();
        for (const auto [tabletId, channel, generation, step, orderNumber] : thresholdList) {
            AddItemToWriteBuffer(TPhantomFlagStorageItem::CreateThreshold(orderNumber,
                    tabletId, channel, generation, step));
        }
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

        STLOG(PRI_DEBUG, BS_PHANTOM_FLAG_STORAGE, BSPFS06,
                VDISKP(SlCtx->VCtx, "Finish building"),
                (FlagsAdded, flagsAdded),
                (FlagsReceived, flags.size()));
    }

    Building = false;
}

void TPhantomFlagStorageState::Recover(TPhantomFlagStorageSnapshot&& snapshot) {
    STLOG(PRI_DEBUG, BS_PHANTOM_FLAG_STORAGE, BSPFS10,
            VDISKP(SlCtx->VCtx, "Recovering PhantomFlagStorage"));
    Building = false;
    Thresholds.Merge(std::move(snapshot.Thresholds));
}

void TPhantomFlagStorageState::Deactivate() {
    STLOG(PRI_NOTICE, BS_PHANTOM_FLAG_STORAGE, BSPFS07,
            VDISKP(SlCtx->VCtx, "Deactivating PhantomFlagStorage"),
            (FlagsDropped, StoredFlags.size()));
    Thresholds.Clear();
    Active = false;
    Building = false;
    if (IsPersistent) {
        TActivationContext::Send(new IEventHandle(ProcessorId, TActorId{}, new TEvPhantomFlagStorageDrop));
        WriteBuffer.clear();
        WriteBufferSize = 0;
    } else {
        StoredFlags.clear();
    }
}

void TPhantomFlagStorageState::RequestSnapshot(TEvPhantomFlagStorageGetSnapshot::TPtr ev) const {
    if (IsPersistent) {
        TActivationContext::Send(ev->Forward(ProcessorId));
    } else {
        STLOG(PRI_DEBUG, BS_PHANTOM_FLAG_STORAGE, BSPFS05,
                VDISKP(SlCtx->VCtx, "Acquiring snapshot"),
                (FlagsCount, StoredFlags.size()));
        auto res = std::make_unique<TEvPhantomFlagStorageGetSnapshotResult>(TPhantomFlagStorageSnapshot(StoredFlags, Thresholds));
        TActivationContext::Send(new IEventHandle(ev->Sender, ev->Recipient, res.release()));
    }
}

bool TPhantomFlagStorageState::IsActive() const {
    return Active;
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
        STLOG(PRI_DEBUG, BS_PHANTOM_FLAG_STORAGE, BSPFS03,
                VDISKP(SlCtx->VCtx, "Reserving additional space for PhantomFlagStorage"),
                (OldCapacity, MaxFlagsStoredCount),
                (NewCapacity, newCapacity),
                (ActualCapacity, StoredFlags.capacity()));
    } else if (newCapacity < MaxFlagsStoredCount) {
        ui32 flagsDropped = 0;
        if (newCapacity < StoredFlags.size()) {
            flagsDropped = StoredFlags.size() - newCapacity;
            StoredFlags = TPhantomFlags(StoredFlags.begin(), StoredFlags.begin() + newCapacity);
        }
        StoredFlags.shrink_to_fit();
        StoredFlags.reserve(newCapacity);
        STLOG(PRI_DEBUG, BS_PHANTOM_FLAG_STORAGE, BSPFS04,
                VDISKP(SlCtx->VCtx, "Shrinking PhantomFlagStorage"),
                (OldCapacity, MaxFlagsStoredCount),
                (NewCapacity, newCapacity),
                (ActualCapacity, StoredFlags.capacity()),
                (FlagsDropped, flagsDropped));
    }
    MaxFlagsStoredCount = newCapacity;
}

bool TPhantomFlagStorageState::AddFlag(const TLogoBlobRec& blobRec) {
    if (StoredFlags.size() < StoredFlags.capacity()) {
        StoredFlags.emplace_back(blobRec);
        return true;
    } else {
        STLOG(PRI_INFO, BS_PHANTOM_FLAG_STORAGE, BSPFS02,
                VDISKP(SlCtx->VCtx, "Cannot add flag to PhantomFlagStorage, memory limit reached"),
                (Capacity, StoredFlags.capacity()),
                (Size, StoredFlags.size()),
                (BlobId, blobRec.LogoBlobID().ToString()));
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

void TPhantomFlagStorageState::AddItemToWriteBuffer(const TPhantomFlagStorageItem& item) {
    if (WriteBufferSize + item.SerializedSize() > WriteBufferSizeLimit) {
        FlushWriteBuffer();
    }
    WriteBuffer.push_back(item);
    WriteBufferSize += item.SerializedSize();
}

void TPhantomFlagStorageState::FlushWriteBuffer() {
    if (!WriteBuffer.empty()) {
        auto ev = std::make_unique<TEvPhantomFlagStorageWriteItems>(std::move(WriteBuffer));
        TActivationContext::Send(new IEventHandle(ProcessorId, TActorId{}, ev.release()));
        WriteBufferSize = 0;
        WriteBufferFlushTimestamp = TActivationContext::Monotonic();
    }
}

void TPhantomFlagStorageState::FlushWriteBufferIfNeeded() {
    TMonotonic now = TActivationContext::Monotonic();
    if (now - WriteBufferFlushTimestamp > WriteBufferFlushPeriod) {
        FlushWriteBuffer();
    }
}

void TPhantomFlagStorageState::SyncLogIsCut() {
    FlushWriteBuffer();
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
        WriteBuffer.clear();
        WriteBufferSize = 0;
    }
}

} // namespace NSyncLog

} // namespace NKikimr
