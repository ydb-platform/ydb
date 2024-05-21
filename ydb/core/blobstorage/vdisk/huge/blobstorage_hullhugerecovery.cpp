#include "blobstorage_hullhugerecovery.h"
#include "blobstorage_hullhugeheap.h"
#include <library/cpp/random_provider/random_provider.h>


using namespace NKikimrServices;

namespace NKikimr {
    namespace NHuge {

        ////////////////////////////////////////////////////////////////////////////
        // THullHugeRecoveryLogPos
        ////////////////////////////////////////////////////////////////////////////
        TString THullHugeRecoveryLogPos::ToString() const {
            TStringStream str;
            str << "{ChunkAllocationLsn# " << ChunkAllocationLsn
                << " ChunkFreeingLsn# " << ChunkFreeingLsn
                << " HugeBlobLoggedLsn# " << HugeBlobLoggedLsn
                << " LogoBlobsDbSlotDelLsn# " << LogoBlobsDbSlotDelLsn
                << " BlocksDbSlotDelLsn# " << BlocksDbSlotDelLsn
                << " BarriersDbSlotDelLsn# " << BarriersDbSlotDelLsn
                << " EntryPointLsn# " << EntryPointLsn << "}";
            return str.Str();
        }

        TString THullHugeRecoveryLogPos::Serialize() const {
            TStringStream str;
            str.Write(&ChunkAllocationLsn, sizeof(ui64));
            str.Write(&ChunkFreeingLsn, sizeof(ui64));
            str.Write(&HugeBlobLoggedLsn, sizeof(ui64));
            str.Write(&LogoBlobsDbSlotDelLsn, sizeof(ui64));
            str.Write(&BlocksDbSlotDelLsn, sizeof(ui64));
            str.Write(&BarriersDbSlotDelLsn, sizeof(ui64));
            str.Write(&EntryPointLsn, sizeof(ui64));
            return str.Str();
        }

        void THullHugeRecoveryLogPos::ParseFromString(const TString &serialized) {
            ParseFromArray(serialized.data(), serialized.size());
        }

        void THullHugeRecoveryLogPos::ParseFromArray(const char* data, size_t size) {
            const char *cur = data;
            const char *end = data + size;
            for (ui64 *var : {&ChunkAllocationLsn, &ChunkFreeingLsn, &HugeBlobLoggedLsn, &LogoBlobsDbSlotDelLsn,
                    &BlocksDbSlotDelLsn, &BarriersDbSlotDelLsn, &EntryPointLsn}) {
                Y_ABORT_UNLESS(static_cast<size_t>(end - cur) >= sizeof(*var));
                memcpy(var, cur, sizeof(*var));
                cur += sizeof(*var);
            }
            Y_ABORT_UNLESS(cur == end);
        }

        bool THullHugeRecoveryLogPos::CheckEntryPoint(const TString &serialized) {
            return serialized.size() == SerializedSize;
        }

        ////////////////////////////////////////////////////////////////////////////
        // THullHugeKeeperPersState
        ////////////////////////////////////////////////////////////////////////////
        const ui32 THullHugeKeeperPersState::Signature = 0x18A0CE62;

        THullHugeKeeperPersState::THullHugeKeeperPersState(TIntrusivePtr<TVDiskContext> vctx,
                                                           const ui32 chunkSize,
                                                           const ui32 appendBlockSize,
                                                           const ui32 minHugeBlobInBytes,
                                                           const ui32 oldMinHugeBlobInBytes,
                                                           const ui32 milestoneHugeBlobInBytes,
                                                           const ui32 maxBlobInBytes,
                                                           const ui32 overhead,
                                                           const ui32 freeChunksReservation,
                                                           std::function<void(const TString&)> logFunc)
            : VCtx(std::move(vctx))
            , LogPos(THullHugeRecoveryLogPos::Default())
            , Heap(new NHuge::THeap(VCtx->VDiskLogPrefix, chunkSize, appendBlockSize,
                                    minHugeBlobInBytes, oldMinHugeBlobInBytes, milestoneHugeBlobInBytes,
                                    maxBlobInBytes, overhead, freeChunksReservation))
            , AllocatedSlots()
            , Guid(TAppData::RandomProvider->GenRand64())
        {
            logFunc(VDISKP(VCtx->VDiskLogPrefix,
                "Recovery started (guid# %" PRIu64 " entryLsn# null): State# %s",
                Guid, ToString().data()));
        }

        THullHugeKeeperPersState::THullHugeKeeperPersState(TIntrusivePtr<TVDiskContext> vctx,
                                                           const ui32 chunkSize,
                                                           const ui32 appendBlockSize,
                                                           const ui32 minHugeBlobInBytes,
                                                           const ui32 oldMinHugeBlobInBytes,
                                                           const ui32 milestoneHugeBlobInBytes,
                                                           const ui32 maxBlobInBytes,
                                                           const ui32 overhead,
                                                           const ui32 freeChunksReservation,
                                                           const ui64 entryPointLsn,
                                                           const TString &entryPointData,
                                                           std::function<void(const TString&)> logFunc)
            : VCtx(std::move(vctx))
            , LogPos(THullHugeRecoveryLogPos::Default())
            , Heap(new NHuge::THeap(VCtx->VDiskLogPrefix, chunkSize, appendBlockSize,
                                    minHugeBlobInBytes, oldMinHugeBlobInBytes, milestoneHugeBlobInBytes,
                                    maxBlobInBytes, overhead, freeChunksReservation))
            , AllocatedSlots()
            , Guid(TAppData::RandomProvider->GenRand64())
            , PersistentLsn(entryPointLsn)
        {
            ParseFromString(entryPointData);
            Y_ABORT_UNLESS(entryPointLsn == LogPos.EntryPointLsn);
            logFunc(VDISKP(VCtx->VDiskLogPrefix,
                "Recovery started (guid# %" PRIu64 " entryLsn# %" PRIu64 "): State# %s",
                Guid, entryPointLsn, ToString().data()));
        }

        THullHugeKeeperPersState::THullHugeKeeperPersState(TIntrusivePtr<TVDiskContext> vctx,
                                                           const ui32 chunkSize,
                                                           const ui32 appendBlockSize,
                                                           const ui32 minHugeBlobInBytes,
                                                           const ui32 oldMinHugeBlobInBytes,
                                                           const ui32 milestoneHugeBlobInBytes,
                                                           const ui32 maxBlobInBytes,
                                                           const ui32 overhead,
                                                           const ui32 freeChunksReservation,
                                                           const ui64 entryPointLsn,
                                                           const TContiguousSpan &entryPointData,
                                                           std::function<void(const TString&)> logFunc)
            : VCtx(std::move(vctx))
            , LogPos(THullHugeRecoveryLogPos::Default())
            , Heap(new NHuge::THeap(VCtx->VDiskLogPrefix, chunkSize, appendBlockSize,
                                    minHugeBlobInBytes, oldMinHugeBlobInBytes, milestoneHugeBlobInBytes,
                                    maxBlobInBytes, overhead, freeChunksReservation))
            , AllocatedSlots()
            , Guid(TAppData::RandomProvider->GenRand64())
            , PersistentLsn(entryPointLsn)
        {
            ParseFromArray(entryPointData.GetData(), entryPointData.GetSize());
            Y_ABORT_UNLESS(entryPointLsn == LogPos.EntryPointLsn);
            logFunc(VDISKP(VCtx->VDiskLogPrefix,
                "Recovery started (guid# %" PRIu64 " entryLsn# %" PRIu64 "): State# %s",
                Guid, entryPointLsn, ToString().data()));
        }

        THullHugeKeeperPersState::~THullHugeKeeperPersState() {
        }

        TString THullHugeKeeperPersState::Serialize() const {
            TStringStream str;
            // signature
            str.Write(&Signature, sizeof(ui32));

            // log pos
            TString serializedLogPos = LogPos.Serialize();
            Y_DEBUG_ABORT_UNLESS(serializedLogPos.size() == THullHugeRecoveryLogPos::SerializedSize);
            str.Write(serializedLogPos.data(), THullHugeRecoveryLogPos::SerializedSize);

            // heap
            TString serializedHeap = Heap->Serialize();
            ui32 heapSize = serializedHeap.size();
            str.Write(&heapSize, sizeof(ui32));
            str.Write(serializedHeap.data(), heapSize);

            // chunks to free -- obsolete field
            const ui32 chunksSize = 0;
            Y_ABORT_UNLESS(!chunksSize);
            str.Write(&chunksSize, sizeof(ui32));

            // allocated slots
            ui32 slotsSize = AllocatedSlots.size();
            str.Write(&slotsSize, sizeof(ui32));
            for (const auto &x : AllocatedSlots) {
                x.Serialize(str);
                ui64 refPointLsn = 0; // refPointLsn (for backward compatibility, can be removed)
                str.Write(&refPointLsn, sizeof(ui64));
            }

            return str.Str();
        }

        void THullHugeKeeperPersState::ParseFromString(const TString &data) {
            ParseFromArray(data.data(), data.size());
        }

        void THullHugeKeeperPersState::ParseFromArray(const char* data, size_t size) {
            Y_UNUSED(size);
            AllocatedSlots.clear();

            const char *cur = data;
            cur += sizeof(ui32); // signature

            // log pos
            LogPos.ParseFromString(TString(cur, cur + THullHugeRecoveryLogPos::SerializedSize));
            cur += THullHugeRecoveryLogPos::SerializedSize; // log pos

            // heap
            ui32 heapSize = ReadUnaligned<ui32>(cur);
            cur += sizeof(ui32); // heap size
            Heap->ParseFromString(TString(cur, cur + heapSize));
            cur += heapSize;

            // chunks to free
            ui32 chunksSize = ReadUnaligned<ui32>(cur);
            cur += sizeof(ui32); // chunks size
            Y_ABORT_UNLESS(!chunksSize);

            // allocated slots
            ui32 slotsSize = ReadUnaligned<ui32>(cur);
            cur += sizeof(ui32); // slots size
            for (ui32 i = 0; i < slotsSize; i++) {
                NHuge::THugeSlot hugeSlot;
                hugeSlot.Parse(cur, cur + NHuge::THugeSlot::SerializedSize);
                cur += NHuge::THugeSlot::SerializedSize;
                cur += sizeof(ui64); // refPointLsn (for backward compatibility, can be removed)
                bool inserted = AllocatedSlots.insert(hugeSlot).second;
                Y_ABORT_UNLESS(inserted);
            }
        }

        TString THullHugeKeeperPersState::ExtractLogPosition(const TString &data) {
            const char *cur = data.data();
            cur += sizeof(ui32); // signature
            return TString(cur, cur + THullHugeRecoveryLogPos::SerializedSize);
        }

        TContiguousSpan THullHugeKeeperPersState::ExtractLogPosition(TContiguousSpan data) {
            const char *cur = data.data();
            cur += sizeof(ui32); // signature
            return TContiguousSpan(cur, THullHugeRecoveryLogPos::SerializedSize);
        }

        bool THullHugeKeeperPersState::CheckEntryPoint(const TString &data) {
            return CheckEntryPoint(TContiguousSpan(data));
        }

        bool THullHugeKeeperPersState::CheckEntryPoint(TContiguousSpan data) {
            const char *cur = data.data();
            const char *end = cur + data.size();

            if (size_t(end - cur) < sizeof(ui32) + THullHugeRecoveryLogPos::SerializedSize + sizeof(ui32))
                return false;

            // signature
            ui32 signature = ReadUnaligned<ui32>(cur);
            cur += sizeof(ui32); // signature
            if (signature != Signature)
                return false;

            // log pos
            if (!THullHugeRecoveryLogPos::CheckEntryPoint(TString(cur, cur + THullHugeRecoveryLogPos::SerializedSize))) //FIXME(innokentii) unnecessary copy
                return false;
            cur += THullHugeRecoveryLogPos::SerializedSize; // log pos

            // heap
            ui32 heapSize = ReadUnaligned<ui32>(cur);
            cur += sizeof(ui32); // heap size
            if (size_t(end - cur) < heapSize)
                return false;
            if (!NHuge::THeap::CheckEntryPoint(TString(cur, cur + heapSize)))
                return false;
            cur += heapSize;

            // chunks to free
            if (size_t(end - cur) < sizeof(ui32))
                return false;
            ui32 chunksSize = ReadUnaligned<ui32>(cur);
            cur += sizeof(ui32); // chunks size
            if (size_t(end - cur) < chunksSize * sizeof(ui32))
                return false;
            cur += chunksSize * sizeof(ui32);

            // allocated slots
            if (size_t(end - cur) < sizeof(ui32))
                return false;
            ui32 slotsSize = ReadUnaligned<ui32>(cur);
            cur += sizeof(ui32); // slots size
            if (size_t(end - cur) != slotsSize * (NHuge::THugeSlot::SerializedSize + sizeof(ui64)))
                return false;

            return true;
        }

        TString THullHugeKeeperPersState::ToString() const {
            TStringStream str;
            str << "LogPos: " << LogPos.ToString();
            str << " AllocatedSlots:";
            if (!AllocatedSlots.empty()) {
                for (const auto &x : AllocatedSlots) {
                    str << " " << x.ToString();
                }
            } else {
                str << " empty";
            }
            str << " " << Heap->ToString();
            return str.Str();
        }

        void THullHugeKeeperPersState::RenderHtml(IOutputStream &str) const {
            str << "LogPos: " << LogPos.ToString() << "<br/>";
            str << "AllocatedSlots:";
            if (!AllocatedSlots.empty()) {
                for (const auto &x : AllocatedSlots) {
                    str << " " << x.ToString();
                }
            } else {
                str << " empty<br>";
            }
            Heap->RenderHtml(str);
        }

        ui64 THullHugeKeeperPersState::FirstLsnToKeep(ui64 minInFlightLsn) const {
            const ui64 res = Min(minInFlightLsn, PersistentLsn);

            Y_VERIFY_S(FirstLsnToKeepReported <= res, "FirstLsnToKeepReported# " << FirstLsnToKeepReported
                << " res# " << res << " state# " << FirstLsnToKeepDecomposed() << " minInFlightLsn# " << minInFlightLsn);
            FirstLsnToKeepReported = res;

            return res;
        }

        TString THullHugeKeeperPersState::FirstLsnToKeepDecomposed() const {
            TStringStream str;
            str << "{LogPos# " << LogPos.EntryPointLsn << "}";
            return str.Str();
        }

        bool THullHugeKeeperPersState::WouldNewEntryPointAdvanceLog(ui64 freeUpToLsn, ui64 minInFlightLsn,
                ui32 itemsAfterCommit) const {
            return freeUpToLsn < minInFlightLsn && (PersistentLsn <= freeUpToLsn || itemsAfterCommit > 10000);
        }

        // initiate commit
        void THullHugeKeeperPersState::InitiateNewEntryPointCommit(ui64 lsn, ui64 minInFlightLsn) {
            Y_ABORT_UNLESS(lsn > LogPos.EntryPointLsn);
            LogPos.EntryPointLsn = lsn;
            PersistentLsn = Min(lsn, minInFlightLsn);

            // these metabases never have huge blobs and we never care about them actually
            LogPos.BlocksDbSlotDelLsn = lsn;
            LogPos.BarriersDbSlotDelLsn = lsn;
        }

        // finish commit
        void THullHugeKeeperPersState::EntryPointCommitted(ui64 entryPointLsn) {
            Y_ABORT_UNLESS(entryPointLsn == LogPos.EntryPointLsn);
        }

        // chunk allocation
        TRlas THullHugeKeeperPersState::Apply(
                const TActorContext &ctx,
                ui64 lsn,
                const NHuge::TAllocChunkRecoveryLogRec &rec)
        {
            if (lsn > LogPos.ChunkAllocationLsn) {
                LOG_DEBUG(ctx, BS_HULLHUGE,
                          VDISKP(VCtx->VDiskLogPrefix,
                                "Recovery(guid# %" PRIu64 " lsn# %" PRIu64 " entryLsn# %" PRIu64 "): "
                                "AllocChunk apply: %s",
                                Guid, lsn, LogPos.EntryPointLsn, rec.ToString().data()));
                Heap->RecoveryModeAddChunk(rec.ChunkId);
                LogPos.ChunkAllocationLsn = lsn;
                PersistentLsn = Min(PersistentLsn, lsn);
                return TRlas(true, false);
            } else {
                // skip
                LOG_DEBUG(ctx, BS_HULLHUGE,
                          VDISKP(VCtx->VDiskLogPrefix,
                                "Recovery(guid# %" PRIu64 " lsn# %" PRIu64 " entryLsn# %" PRIu64 "): "
                                "AllocChunk skip: %s",
                                Guid, lsn, LogPos.EntryPointLsn, rec.ToString().data()));
                return TRlas(true, true);
            }
        }

        // free chunk
        TRlas THullHugeKeeperPersState::Apply(
                const TActorContext &ctx,
                ui64 lsn,
                const NHuge::TFreeChunkRecoveryLogRec &rec)
        {
            if (lsn > LogPos.ChunkFreeingLsn) {
                // apply
                LOG_DEBUG(ctx, BS_HULLHUGE,
                          VDISKP(VCtx->VDiskLogPrefix,
                                "Recovery(guid# %" PRIu64 " lsn# %" PRIu64 " entryLsn# %" PRIu64 "): "
                                "FreeChunk apply(remove): %s",
                                Guid, lsn, LogPos.EntryPointLsn, rec.ToString().data()));
                Heap->RecoveryModeRemoveChunks(rec.ChunkIds);
                LogPos.ChunkFreeingLsn = lsn;
                PersistentLsn = Min(PersistentLsn, lsn);
                return TRlas(true, false);
            } else {
                // skip
                LOG_DEBUG(ctx, BS_HULLHUGE,
                          VDISKP(VCtx->VDiskLogPrefix,
                                "Recovery(guid# %" PRIu64 " lsn# %" PRIu64 " entryLsn# %" PRIu64 "): "
                                "FreeChunk skip: %s",
                                Guid, lsn, LogPos.EntryPointLsn, rec.ToString().data()));
                return TRlas(true, true);
            }
        }

        // apply deleted slots
        TRlas THullHugeKeeperPersState::ApplySlotsDeletion(
                const TActorContext &ctx,
                ui64 lsn,
                const TDiskPartVec &rec,
                ESlotDelDbType type)
        {
            ui64 *logPosDelLsn = nullptr;
            switch (type) {
                case LogoBlobsDb:
                    logPosDelLsn = &LogPos.LogoBlobsDbSlotDelLsn;
                    break;
                case BlocksDb:
                    logPosDelLsn = &LogPos.BlocksDbSlotDelLsn;
                    break;
                case BarriersDb:
                    logPosDelLsn = &LogPos.BarriersDbSlotDelLsn;
                    break;
                default:
                    Y_ABORT("Unexpected case");
            }
            if (lsn > *logPosDelLsn) {
                // apply
                LOG_DEBUG(ctx, BS_HULLHUGE,
                          VDISKP(VCtx->VDiskLogPrefix,
                                "Recovery(guid# %" PRIu64 " lsn# %" PRIu64 " entryLsn# %" PRIu64 "): "
                                "RmHugeBlobs apply: %s",
                                Guid, lsn, LogPos.EntryPointLsn, rec.ToString().data()));
                for (const auto &x : rec)
                    Heap->RecoveryModeFree(x);

                *logPosDelLsn = lsn;
                PersistentLsn = Min(PersistentLsn, lsn);
                return TRlas(true, false);
            } else {
                // skip
                LOG_DEBUG(ctx, BS_HULLHUGE,
                          VDISKP(VCtx->VDiskLogPrefix,
                                "Recovery(guid# %" PRIu64 " lsn# %" PRIu64 " entryLsn# %" PRIu64 "): "
                                "RmHugeBlobs skip: %s",
                                Guid, lsn, LogPos.EntryPointLsn, rec.ToString().data()));
                return TRlas(true, true);
            }
        }

        // apply huge blob written
        TRlas THullHugeKeeperPersState::Apply(
                const TActorContext &ctx,
                ui64 lsn,
                const NHuge::TPutRecoveryLogRec &rec)
        {
            if (rec.DiskAddr == TDiskPart()) {
                // this is metadata part, no actual slot exists here
                if (lsn > LogPos.HugeBlobLoggedLsn) {
                    LogPos.HugeBlobLoggedLsn = lsn;
                    return TRlas(true, false);
                } else {
                    return TRlas(true, true);
                }
            }

            NHuge::THugeSlot hugeSlot(Heap->ConvertDiskPartToHugeSlot(rec.DiskAddr));
            if (lsn > LogPos.HugeBlobLoggedLsn) {
                // apply
                TAllocatedSlots::iterator it = AllocatedSlots.find(hugeSlot);
                if (it != AllocatedSlots.end()) {
                    AllocatedSlots.erase(it);
                    LOG_DEBUG(ctx, BS_HULLHUGE,
                              VDISKP(VCtx->VDiskLogPrefix,
                                    "Recovery(guid# %" PRIu64 " lsn# %" PRIu64 " entryLsn# %" PRIu64 "): "
                                    "HugeBlob apply(1): rec# %s hugeSlot# %s",
                                    Guid, lsn, LogPos.EntryPointLsn, rec.ToString().data(), hugeSlot.ToString().data()));
                } else {
                    LOG_DEBUG(ctx, BS_HULLHUGE,
                              VDISKP(VCtx->VDiskLogPrefix,
                                    "Recovery(guid# %" PRIu64 " lsn# %" PRIu64 " entryLsn# %" PRIu64 "): "
                                    "HugeBlob apply(2): rec# %s hugeSlot# %s",
                                    Guid, lsn, LogPos.EntryPointLsn, rec.ToString().data(), hugeSlot.ToString().data()));
                    Heap->RecoveryModeAllocate(rec.DiskAddr);
                }
                LogPos.HugeBlobLoggedLsn = lsn;
                PersistentLsn = Min(PersistentLsn, lsn);
                return TRlas(true, false);
            } else {
                // skip
                LOG_DEBUG(ctx, BS_HULLHUGE,
                          VDISKP(VCtx->VDiskLogPrefix,
                                "Recovery(guid# %" PRIu64 " lsn# %" PRIu64 " entryLsn# %" PRIu64 "): "
                                "HugeBlob skip: rec# %s hugeSlot# %s",
                                Guid, lsn, LogPos.EntryPointLsn, rec.ToString().data(), hugeSlot.ToString().data()));
                return TRlas(true, true);
            }
        }

        TRlas THullHugeKeeperPersState::ApplyEntryPoint(
                const TActorContext &ctx,
                ui64 lsn,
                const TString &data)
        {
            if (!CheckEntryPoint(data))
                return TRlas(false, true);

            TString logPosSerialized = ExtractLogPosition(data);
            auto logPos = THullHugeRecoveryLogPos::Default();
            logPos.ParseFromString(logPosSerialized);
            Y_ABORT_UNLESS(logPos.EntryPointLsn == lsn);

            LOG_DEBUG(ctx, BS_HULLHUGE,
                    VDISKP(VCtx->VDiskLogPrefix,
                        "Recovery(guid# %" PRIu64 " lsn# %" PRIu64 " entryLsn# %" PRIu64 "): "
                        "EntryPoint: logPos# %s",
                        Guid, lsn, LogPos.EntryPointLsn, logPos.ToString().data()));

            return TRlas(true, false);
        }

        TRlas THullHugeKeeperPersState::ApplyEntryPoint(
                const TActorContext &ctx,
                ui64 lsn,
                const TContiguousSpan &data)
        {
            if (!CheckEntryPoint(data))
                return TRlas(false, true);

            TContiguousSpan logPosSerialized = ExtractLogPosition(data);
            auto logPos = THullHugeRecoveryLogPos::Default();
            logPos.ParseFromArray(logPosSerialized.GetData(), logPosSerialized.GetSize());
            Y_ABORT_UNLESS(logPos.EntryPointLsn == lsn);

            LOG_DEBUG(ctx, BS_HULLHUGE,
                    VDISKP(VCtx->VDiskLogPrefix,
                        "Recovery(guid# %" PRIu64 " lsn# %" PRIu64 " entryLsn# %" PRIu64 "): "
                        "EntryPoint: logPos# %s",
                        Guid, lsn, LogPos.EntryPointLsn, logPos.ToString().data()));

            return TRlas(true, false);
        }

        void THullHugeKeeperPersState::FinishRecovery(const TActorContext &ctx) {
            // handle AllocatedSlots
            if (!AllocatedSlots.empty()) {
                for (const auto &x : AllocatedSlots) {
                    Heap->RecoveryModeFree(x.GetDiskPart());
                }
                AllocatedSlots.clear();
            }

            Recovered = true;
            LOG_DEBUG(ctx, BS_HULLHUGE,
                VDISKP(VCtx->VDiskLogPrefix, "Recovery(guid# %" PRIu64 ") finished", Guid));
        }

        void THullHugeKeeperPersState::GetOwnedChunks(TSet<TChunkIdx>& chunks) const {
            Heap->GetOwnedChunks(chunks);
        }

    } // NHuge
} // NKikimr
