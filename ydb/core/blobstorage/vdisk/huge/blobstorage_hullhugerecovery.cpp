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

        void THullHugeRecoveryLogPos::ParseFromString(const TString& prefix, const TString &serialized) {
            ParseFromArray(prefix, serialized.data(), serialized.size());
        }

        void THullHugeRecoveryLogPos::ParseFromArray(const TString& prefix, const char* data, size_t size) {
            const char *cur = data;
            const char *end = data + size;
            for (ui64 *var : {&ChunkAllocationLsn, &ChunkFreeingLsn, &HugeBlobLoggedLsn, &LogoBlobsDbSlotDelLsn,
                    &BlocksDbSlotDelLsn, &BarriersDbSlotDelLsn, &EntryPointLsn}) {
                Y_VERIFY_S(static_cast<size_t>(end - cur) >= sizeof(*var), prefix);
                memcpy(var, cur, sizeof(*var));
                cur += sizeof(*var);
            }
            Y_VERIFY_S(cur == end, prefix);
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
                                                           const ui32 milestoneHugeBlobInBytes,
                                                           const ui32 maxBlobInBytes,
                                                           const ui32 overhead,
                                                           const ui32 freeChunksReservation,
                                                           std::function<void(const TString&)> logFunc)
            : VCtx(std::move(vctx))
            , LogPos(THullHugeRecoveryLogPos::Default())
            , Heap(new NHuge::THeap(VCtx->VDiskLogPrefix, chunkSize, appendBlockSize,
                                    minHugeBlobInBytes, milestoneHugeBlobInBytes,
                                    maxBlobInBytes, overhead, freeChunksReservation))
            , Guid(TAppData::RandomProvider->GenRand64())
        {
            Heap->FinishRecovery();
            logFunc(VDISKP(VCtx->VDiskLogPrefix,
                "Recovery started (guid# %" PRIu64 " entryLsn# null): State# %s",
                Guid, ToString().data()));
        }

        THullHugeKeeperPersState::THullHugeKeeperPersState(TIntrusivePtr<TVDiskContext> vctx,
                                                           const ui32 chunkSize,
                                                           const ui32 appendBlockSize,
                                                           const ui32 minHugeBlobInBytes,
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
                                    minHugeBlobInBytes, milestoneHugeBlobInBytes,
                                    maxBlobInBytes, overhead, freeChunksReservation))
            , Guid(TAppData::RandomProvider->GenRand64())
            , PersistentLsn(entryPointLsn)
        {
            ParseFromString(entryPointData);
            Heap->FinishRecovery();
            Y_VERIFY_S(entryPointLsn == LogPos.EntryPointLsn, VCtx->VDiskLogPrefix);
            logFunc(VDISKP(VCtx->VDiskLogPrefix,
                "Recovery started (guid# %" PRIu64 " entryLsn# %" PRIu64 "): State# %s",
                Guid, entryPointLsn, ToString().data()));
        }

        THullHugeKeeperPersState::THullHugeKeeperPersState(TIntrusivePtr<TVDiskContext> vctx,
                                                           const ui32 chunkSize,
                                                           const ui32 appendBlockSize,
                                                           const ui32 minHugeBlobInBytes,
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
                                    minHugeBlobInBytes, milestoneHugeBlobInBytes,
                                    maxBlobInBytes, overhead, freeChunksReservation))
            , Guid(TAppData::RandomProvider->GenRand64())
            , PersistentLsn(entryPointLsn)
        {
            ParseFromArray(entryPointData.GetData(), entryPointData.GetSize());
            Heap->FinishRecovery();
            Y_VERIFY_S(entryPointLsn == LogPos.EntryPointLsn, VCtx->VDiskLogPrefix);
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
            Y_VERIFY_DEBUG_S(serializedLogPos.size() == THullHugeRecoveryLogPos::SerializedSize, VCtx->VDiskLogPrefix);
            str.Write(serializedLogPos.data(), THullHugeRecoveryLogPos::SerializedSize);

            // heap
            std::vector<bool> inLockedChunks;
            inLockedChunks.reserve(SlotsInFlight.size());
            for (const THugeSlot& slot : SlotsInFlight) {
                inLockedChunks.push_back(Heap->ReleaseSlot(slot)); // mark this slot as free one for the means of serialization
            }
            TString serializedHeap = Heap->Serialize();
            size_t index = 0;
            for (const THugeSlot& slot : SlotsInFlight) {
                Y_VERIFY_DEBUG_S(index < inLockedChunks.size(), VCtx->VDiskLogPrefix);
                Heap->OccupySlot(slot, inLockedChunks[index++]); // restore slot ownership
            }
            Y_VERIFY_DEBUG_S(index == inLockedChunks.size(), VCtx->VDiskLogPrefix);
            ui32 heapSize = serializedHeap.size();
            str.Write(&heapSize, sizeof(ui32));
            str.Write(serializedHeap.data(), heapSize);

            // chunks to free -- obsolete field
            const ui32 chunksSize = 0;
            Y_VERIFY_S(!chunksSize, VCtx->VDiskLogPrefix);
            str.Write(&chunksSize, sizeof(ui32));

            // allocated slots (we really never save them now, they're considered as free ones while serializing Heap)
            ui32 slotsSize = 0;
            str.Write(&slotsSize, sizeof(ui32));

            return str.Str();
        }

        void THullHugeKeeperPersState::ParseFromString(const TString &data) {
            ParseFromArray(data.data(), data.size());
        }

        void THullHugeKeeperPersState::ParseFromArray(const char* data, size_t size) {
            Y_UNUSED(size);
            SlotsInFlight.clear();

            const char *cur = data;
            cur += sizeof(ui32); // signature

            // log pos
            LogPos.ParseFromString(VCtx->VDiskLogPrefix, TString(cur, cur + THullHugeRecoveryLogPos::SerializedSize));
            cur += THullHugeRecoveryLogPos::SerializedSize; // log pos

            // heap
            ui32 heapSize = ReadUnaligned<ui32>(cur);
            cur += sizeof(ui32); // heap size
            Heap->ParseFromString(TString(cur, cur + heapSize));
            cur += heapSize;

            // chunks to free
            ui32 chunksSize = ReadUnaligned<ui32>(cur);
            cur += sizeof(ui32); // chunks size
            Y_VERIFY_S(!chunksSize, VCtx->VDiskLogPrefix);

            // allocated slots
            ui32 slotsSize = ReadUnaligned<ui32>(cur);
            cur += sizeof(ui32); // slots size
            for (ui32 i = 0; i < slotsSize; i++) {
                NHuge::THugeSlot hugeSlot;
                hugeSlot.Parse(cur, cur + NHuge::THugeSlot::SerializedSize);
                cur += NHuge::THugeSlot::SerializedSize;
                cur += sizeof(ui64); // refPointLsn (for backward compatibility, can be removed)
                AddSlotInFlight(hugeSlot);
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
            str << " SlotsInFlight:";
            if (!SlotsInFlight.empty()) {
                for (const auto &x : SlotsInFlight) {
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
            str << "SlotsInFlight:";
            if (!SlotsInFlight.empty()) {
                for (const auto &x : SlotsInFlight) {
                    str << " " << x.ToString();
                }
            } else {
                str << " empty<br>";
            }
            HTML(str) {
                COLLAPSED_BUTTON_CONTENT("chunkstoslotsizeid", "ChunksToSlotSize") {
                    TABLE_CLASS ("table table-condensed") {
                        TABLEHEAD() {
                            TABLER() {
                                TABLEH() {str << "ChunkId";}
                                TABLEH() {str << "RefCount";}
                                TABLEH() {str << "SlotSize";}
                            }
                        }
                        TABLEBODY() {
                            for (const auto& [key, value] : ChunkToSlotSize) {
                                TABLER() {
                                    const auto& [refcount, size] = value;
                                    TABLED() {str << key;}
                                    TABLED() {str << refcount;}
                                    TABLED() {str << size;}
                                }
                            }
                        }
                    }
                }
                str << "<br/>";
            }
            Heap->RenderHtml(str);
        }

        ui64 THullHugeKeeperPersState::FirstLsnToKeep(ui64 minInFlightLsn) const {
            const ui64 res = Min(minInFlightLsn, PersistentLsn);

            Y_VERIFY_S(FirstLsnToKeepReported <= res, VCtx->VDiskLogPrefix << "FirstLsnToKeepReported# " << FirstLsnToKeepReported
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
            Y_VERIFY_S(lsn > LogPos.EntryPointLsn, VCtx->VDiskLogPrefix);
            LogPos.EntryPointLsn = lsn;
            PersistentLsn = Min(lsn, minInFlightLsn);

            // these metabases never have huge blobs and we never care about them actually
            LogPos.BlocksDbSlotDelLsn = lsn;
            LogPos.BarriersDbSlotDelLsn = lsn;
        }

        // finish commit
        void THullHugeKeeperPersState::EntryPointCommitted(ui64 entryPointLsn) {
            Y_VERIFY_S(entryPointLsn == LogPos.EntryPointLsn, VCtx->VDiskLogPrefix);
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
                const TDiskPartVec& allocated,
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
                LOG_DEBUG(ctx, BS_HULLHUGE, VDISKP(VCtx->VDiskLogPrefix, "Recovery(guid# %" PRIu64 " lsn# %" PRIu64
                    " entryLsn# %" PRIu64 "): " "RmHugeBlobs apply: %s", Guid, lsn, LogPos.EntryPointLsn,
                    rec.ToString().data()));
                for (const auto &x : rec) {
                    Heap->RecoveryModeFree(x);
                }
                for (const auto& x : allocated) {
                    Heap->RecoveryModeAllocate(x);
                }

                *logPosDelLsn = lsn;
                PersistentLsn = Min(PersistentLsn, lsn);
                return TRlas(true, false);
            } else {
                // skip
                LOG_DEBUG(ctx, BS_HULLHUGE, VDISKP(VCtx->VDiskLogPrefix, "Recovery(guid# %" PRIu64 " lsn# %" PRIu64
                    " entryLsn# %" PRIu64 "): " "RmHugeBlobs skip: %s", Guid, lsn, LogPos.EntryPointLsn,
                    rec.ToString().data()));
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
                if (DeleteSlotInFlight(hugeSlot)) {
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
            logPos.ParseFromString(VCtx->VDiskLogPrefix, logPosSerialized);
            Y_VERIFY_S(logPos.EntryPointLsn == lsn, VCtx->VDiskLogPrefix);

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
            logPos.ParseFromArray(VCtx->VDiskLogPrefix, logPosSerialized.GetData(), logPosSerialized.GetSize());
            Y_VERIFY_S(logPos.EntryPointLsn == lsn, VCtx->VDiskLogPrefix);

            LOG_DEBUG(ctx, BS_HULLHUGE,
                    VDISKP(VCtx->VDiskLogPrefix,
                        "Recovery(guid# %" PRIu64 " lsn# %" PRIu64 " entryLsn# %" PRIu64 "): "
                        "EntryPoint: logPos# %s",
                        Guid, lsn, LogPos.EntryPointLsn, logPos.ToString().data()));

            return TRlas(true, false);
        }

        void THullHugeKeeperPersState::FinishRecovery(const TActorContext &ctx) {
            // handle SlotsInFlight
            for (const auto &x : SlotsInFlight) {
                Heap->RecoveryModeFree(x.GetDiskPart());
            }
            SlotsInFlight.clear();

            Recovered = true;
            LOG_DEBUG(ctx, BS_HULLHUGE,
                VDISKP(VCtx->VDiskLogPrefix, "Recovery(guid# %" PRIu64 ") finished", Guid));
        }

        void THullHugeKeeperPersState::GetOwnedChunks(TSet<TChunkIdx>& chunks) const {
            Heap->GetOwnedChunks(chunks);
        }

        void THullHugeKeeperPersState::AddSlotInFlight(THugeSlot hugeSlot) {
            const auto [it, inserted] = SlotsInFlight.insert(hugeSlot);
            Y_VERIFY_S(inserted, VCtx->VDiskLogPrefix);
        }

        bool THullHugeKeeperPersState::DeleteSlotInFlight(THugeSlot hugeSlot) {
            if (const auto it = SlotsInFlight.find(hugeSlot); it != SlotsInFlight.end()) {
                Y_VERIFY_S(it->GetSize() == hugeSlot.GetSize(), VCtx->VDiskLogPrefix);
                SlotsInFlight.erase(it);
                return true;
            } else {
                return false;
            }
        }

        void THullHugeKeeperPersState::AddChunkSize(THugeSlot hugeSlot) {
            const auto it = ChunkToSlotSize.emplace(hugeSlot.GetChunkId(), std::make_tuple(0, hugeSlot.GetSize())).first;
            auto& [refcount, size] = it->second;
            Y_VERIFY_DEBUG_S(size == hugeSlot.GetSize(), VCtx->VDiskLogPrefix << "HugeSlot# " << hugeSlot.ToString()
                << " Expected# " << size);
            if (size != hugeSlot.GetSize() && TlsActivationContext) {
                LOG_CRIT_S(*TlsActivationContext, NKikimrServices::BS_HULLHUGE, VCtx->VDiskLogPrefix
                    << "HugeSlot# " << hugeSlot.ToString() << " size is not as Expected# " << size);
            }
            ++refcount;
        }

        void THullHugeKeeperPersState::DeleteChunkSize(THugeSlot hugeSlot) {
            const auto jt = ChunkToSlotSize.find(hugeSlot.GetChunkId());
            Y_VERIFY_S(jt != ChunkToSlotSize.end(), VCtx->VDiskLogPrefix << "HugeSlot# " << hugeSlot.ToString());
            auto& [refcount, size] = jt->second;
            Y_VERIFY_DEBUG_S(size == hugeSlot.GetSize(), VCtx->VDiskLogPrefix << "HugeSlot# " << hugeSlot.ToString()
                << " Expected# " << size);
            if (size != hugeSlot.GetSize() && TlsActivationContext) {
                LOG_CRIT_S(*TlsActivationContext, NKikimrServices::BS_HULLHUGE, VCtx->VDiskLogPrefix
                    << "HugeSlot# " << hugeSlot.ToString() << " size is not as Expected# " << size);
            }
            if (!--refcount) {
                ChunkToSlotSize.erase(jt);
            }
        }

        void THullHugeKeeperPersState::RegisterBlob(TDiskPart diskPart) {
            AddChunkSize(Heap->ConvertDiskPartToHugeSlot(diskPart));
        }

    } // NHuge
} // NKikimr
