#include "blobstorage_hullhugerecovery.h"
#include "blobstorage_hullhugeheap.h"
#include <library/cpp/random_provider/random_provider.h>
#include <ydb/core/base/appdata.h>
#include <ydb/library/actors/core/actor.h>

#define YDB_LOG_THIS_FILE_COMPONENT BS_HULLHUGE

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

        void THullHugeRecoveryLogPos::SaveToProto(NKikimrVDiskData::THullHugeRecoveryLogPos& logPos) const {
            logPos.SetChunkAllocationLsn(ChunkAllocationLsn);
            logPos.SetChunkFreeingLsn(ChunkFreeingLsn);
            logPos.SetHugeBlobLoggedLsn(HugeBlobLoggedLsn);
            logPos.SetLogoBlobsDbSlotDelLsn(LogoBlobsDbSlotDelLsn);
            logPos.SetEntryPointLsn(EntryPointLsn);
        }

        void THullHugeRecoveryLogPos::LoadFromProto(const NKikimrVDiskData::THullHugeRecoveryLogPos& logPos) {
            ChunkAllocationLsn = logPos.GetChunkAllocationLsn();
            ChunkFreeingLsn = logPos.GetChunkFreeingLsn();
            HugeBlobLoggedLsn = logPos.GetHugeBlobLoggedLsn();
            LogoBlobsDbSlotDelLsn = logPos.GetLogoBlobsDbSlotDelLsn();
            EntryPointLsn = logPos.GetEntryPointLsn();
        }

        ////////////////////////////////////////////////////////////////////////////
        // THullHugeKeeperPersState
        ////////////////////////////////////////////////////////////////////////////
        const ui32 THullHugeKeeperPersState::Signature = 0x18A0CE62;
        const ui32 THullHugeKeeperPersState::SignatureV2 = 0x18A0CE63;

        THullHugeKeeperPersState::THullHugeKeeperPersState(TIntrusivePtr<TVDiskContext> vctx,
                                                           const ui32 chunkSize,
                                                           const ui32 appendBlockSize,
                                                           const ui32 minHugeBlobInBytes,
                                                           const ui32 milestoneHugeBlobInBytes,
                                                           const ui32 maxBlobInBytes,
                                                           const ui32 overhead,
                                                           const ui32 stepsBetweenPowersOf2,
                                                           const bool enableTinyDisks,
                                                           const ui32 freeChunksReservation,
                                                           TControlWrapper chunksSoftLocking,
                                                           std::function<void(const TString&)> logFunc)
            : VCtx(std::move(vctx))
            , Heap(new NHuge::THeap(VCtx->VDiskLogPrefix, chunkSize, appendBlockSize,
                                    minHugeBlobInBytes, milestoneHugeBlobInBytes,
                                    maxBlobInBytes, overhead, stepsBetweenPowersOf2,
                                    enableTinyDisks, freeChunksReservation, chunksSoftLocking))
            , StripeHeap(new NHuge::TStripeHeap(VCtx->VDiskLogPrefix, chunkSize, appendBlockSize))
            , Guid(TAppData::RandomProvider->GenRand64())
            , EnableTinyDisks(enableTinyDisks)
            , ChunksSoftLocking(chunksSoftLocking)
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
                                                           const ui32 stepsBetweenPowersOf2,
                                                           const bool enableTinyDisks,
                                                           const ui32 freeChunksReservation,
                                                           const ui64 entryPointLsn,
                                                           const TContiguousSpan &entryPointData,
                                                           TControlWrapper chunksSoftLocking,
                                                           std::function<void(const TString&)> logFunc)
            : VCtx(std::move(vctx))
            , Heap(new NHuge::THeap(VCtx->VDiskLogPrefix, chunkSize, appendBlockSize,
                                    minHugeBlobInBytes, milestoneHugeBlobInBytes,
                                    maxBlobInBytes, overhead, stepsBetweenPowersOf2,
                                    false, freeChunksReservation, chunksSoftLocking))
            , StripeHeap(new NHuge::TStripeHeap(VCtx->VDiskLogPrefix, chunkSize, appendBlockSize))
            , Guid(TAppData::RandomProvider->GenRand64())
            , PersistentLsn(entryPointLsn)
            , EnableTinyDisks(enableTinyDisks)
            , ChunksSoftLocking(chunksSoftLocking)
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
            if (EnableTinyDisks || LoadedFromProto || (StripeHeap && !StripeHeap->Empty())) {
                return SaveToProto();
            }

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

        void THullHugeKeeperPersState::ParseFromArray(const char* data, size_t size) {
            Y_UNUSED(size);
            SlotsInFlight.clear();

            const char *cur = data;

            ui32 signature = ReadUnaligned<ui32>(cur);
            cur += sizeof(ui32);
            if (signature == SignatureV2) {
                LoadFromProto(cur, size - sizeof(ui32));
                return;
            }

            // log pos
            LogPos.ParseFromArray(VCtx->VDiskLogPrefix, cur, THullHugeRecoveryLogPos::SerializedSize);
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

        TString THullHugeKeeperPersState::SaveToProto() const {
            NKikimrVDiskData::THugeKeeperEntryPoint entryPoint;
            LogPos.SaveToProto(*entryPoint.MutableLogPos());

            // The slot heap persists its occupancy, so an in-flight slot has to be serialized as free -- it is not
            // referenced by anything yet, and a crash here must leave it reusable. The stripe heap persists only its
            // chunk list, so its in-flight extents need no such treatment.
            std::vector<bool> inLockedChunks;
            inLockedChunks.reserve(SlotsInFlight.size());
            for (const THugeSlot& slot : SlotsInFlight) {
                if (!StripeHeap->ContainsChunk(slot.GetChunkId())) {
                    inLockedChunks.push_back(Heap->ReleaseSlot(slot));
                }
            }

            Heap->SaveToProto(*entryPoint.MutableHeap());
            StripeHeap->SaveToProto(*entryPoint.MutableStripeHeap());

            size_t index = 0;
            for (const THugeSlot& slot : SlotsInFlight) {
                if (!StripeHeap->ContainsChunk(slot.GetChunkId())) {
                    Y_VERIFY_DEBUG_S(index < inLockedChunks.size(), VCtx->VDiskLogPrefix);
                    Heap->OccupySlot(slot, inLockedChunks[index]);
                    ++index;
                }
            }
            Y_VERIFY_DEBUG_S(index == inLockedChunks.size(), VCtx->VDiskLogPrefix);

            TString result;
            TStringOutput str(result);
            str.Write(&SignatureV2, sizeof(ui32));

            auto size = entryPoint.ByteSize();
            result.resize(sizeof(ui32) + size);
            bool success = entryPoint.SerializeToArray(result.begin() + sizeof(ui32), size);
            Y_VERIFY_S(success, VCtx->VDiskLogPrefix);

            return result;
        }

        void THullHugeKeeperPersState::LoadFromProto(const char* data, size_t size) {
            NKikimrVDiskData::THugeKeeperEntryPoint entryPoint;
            bool success = entryPoint.ParseFromArray(data, size);
            Y_VERIFY_S(success, VCtx->VDiskLogPrefix);

            LogPos.LoadFromProto(entryPoint.GetLogPos());
            Heap.reset(new NHuge::THeap(VCtx->VDiskLogPrefix, entryPoint.GetHeap(), ChunksSoftLocking));
            if (entryPoint.HasStripeHeap()) {
                StripeHeap.reset(new NHuge::TStripeHeap(VCtx->VDiskLogPrefix, entryPoint.GetStripeHeap()));
            }

            LoadedFromProto = true;
        }

        bool THullHugeKeeperPersState::CheckEntryPoint(TContiguousSpan data) {
            const char *cur = data.data();
            const char *end = cur + data.size();

            if (size_t(end - cur) < sizeof(ui32))
                return false;

            // signature
            ui32 signature = ReadUnaligned<ui32>(cur);
            cur += sizeof(ui32); // signature
            if (signature == SignatureV2)
                return true;
            if (signature != Signature)
                return false;

            if (size_t(end - cur) < THullHugeRecoveryLogPos::SerializedSize + sizeof(ui32))
                return false;
            cur += THullHugeRecoveryLogPos::SerializedSize;

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
            if (StripeHeap) {
                str << " " << StripeHeap->ToString();
            }
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
            if (StripeHeap) {
                StripeHeap->RenderHtml(str);
            }
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
            return freeUpToLsn <= minInFlightLsn && (!PersistentLsn || PersistentLsn < freeUpToLsn || itemsAfterCommit > 10000);
        }

        // initiate commit
        void THullHugeKeeperPersState::InitiateNewEntryPointCommit(ui64 lsn, ui64 minInFlightLsn) {
            Y_VERIFY_S(lsn > LogPos.EntryPointLsn, VCtx->VDiskLogPrefix);
            LogPos.EntryPointLsn = lsn;
            PersistentLsn = Min(lsn, minInFlightLsn);
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
                YDB_LOG_DEBUG_CTX(ctx, VDISKP(VCtx->VDiskLogPrefix, "Recovery(guid# %" PRIu64 " lsn# %" PRIu64 " entryLsn# %" PRIu64 "): " "AllocChunk apply: %s", Guid, lsn, LogPos.EntryPointLsn, rec.ToString().data()));
                Heap->RecoveryModeAddChunk(rec.ChunkId);
                LogPos.ChunkAllocationLsn = lsn;
                PersistentLsn = Min(PersistentLsn, lsn);
                return TRlas(true, false);
            } else {
                // skip
                YDB_LOG_DEBUG_CTX(ctx, VDISKP(VCtx->VDiskLogPrefix, "Recovery(guid# %" PRIu64 " lsn# %" PRIu64 " entryLsn# %" PRIu64 "): " "AllocChunk skip: %s", Guid, lsn, LogPos.EntryPointLsn, rec.ToString().data()));
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
                YDB_LOG_DEBUG_CTX(ctx, VDISKP(VCtx->VDiskLogPrefix, "Recovery(guid# %" PRIu64 " lsn# %" PRIu64 " entryLsn# %" PRIu64 "): " "FreeChunk apply(remove): %s", Guid, lsn, LogPos.EntryPointLsn, rec.ToString().data()));
                for (ui32 chunkId : rec.ChunkIds) {
                    // Only an empty chunk is ever handed back, so one still claimed here emptied out earlier in the
                    // log and returned to the slot heap, which is where this record expects to find it.
                    RecoveryReleaseStripeChunk(chunkId);
                }
                Heap->RecoveryModeRemoveChunks(rec.ChunkIds);
                LogPos.ChunkFreeingLsn = lsn;
                PersistentLsn = Min(PersistentLsn, lsn);
                return TRlas(true, false);
            } else {
                // skip
                YDB_LOG_DEBUG_CTX(ctx, VDISKP(VCtx->VDiskLogPrefix, "Recovery(guid# %" PRIu64 " lsn# %" PRIu64 " entryLsn# %" PRIu64 "): " "FreeChunk skip: %s", Guid, lsn, LogPos.EntryPointLsn, rec.ToString().data()));
                return TRlas(true, true);
            }
        }

        // apply deleted slots
        TRlas THullHugeKeeperPersState::ApplySlotsDeletion(
                const TActorContext &ctx,
                ui64 lsn,
                const TDiskPartVec &rec,
                const TDiskPartVec& allocated,
                const TDiskPartVec& allocatedStripe,
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
                YDB_LOG_DEBUG_CTX(ctx, VDISKP(VCtx->VDiskLogPrefix, "Recovery(guid# %" PRIu64 " lsn# %" PRIu64 " entryLsn# %" PRIu64 "): " "RmHugeBlobs apply: %s", Guid, lsn, LogPos.EntryPointLsn, rec.ToString().data()));
                for (const auto &x : rec) {
                    // A stripe extent has nothing to release during replay: it is live only if the recovered hull
                    // still points at it, and this record is the very thing that stopped it doing so.
                    if (!IsStripeAddr(x)) {
                        FreeBlob(x);
                    }
                }
                for (const auto& x : allocated) {
                    RecoveryReleaseStripeChunk(x.ChunkIdx);
                    Heap->RecoveryModeAllocate(x);
                }
                for (const auto& x : allocatedStripe) {
                    RecoveryClaimStripeChunk(x.ChunkIdx);
                }

                *logPosDelLsn = lsn;
                PersistentLsn = Min(PersistentLsn, lsn);
                return TRlas(true, false);
            } else {
                // skip
                YDB_LOG_DEBUG_CTX(ctx, VDISKP(VCtx->VDiskLogPrefix, "Recovery(guid# %" PRIu64 " lsn# %" PRIu64 " entryLsn# %" PRIu64 "): " "RmHugeBlobs skip: %s", Guid, lsn, LogPos.EntryPointLsn, rec.ToString().data()));
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

            // The flag is written when the blob is allocated, so it says which heap owned the chunk at that moment.
            // The chunk's current claim must not be consulted instead: chunks migrate between the heaps whenever they
            // empty out, and a claim made earlier in the log may well have expired by now.
            const bool isStripe = rec.IsStripe;
            if (lsn > LogPos.HugeBlobLoggedLsn) {
                if (isStripe) {
                    // The record is what tells us this chunk left the slot heap; whether the blob it wrote is still
                    // live is decided later, by whether the recovered hull references it.
                    RecoveryClaimStripeChunk(rec.DiskAddr.ChunkIdx);
                    YDB_LOG_DEBUG_CTX(ctx, VDISKP(VCtx->VDiskLogPrefix, "Recovery(guid# %" PRIu64 " lsn# %" PRIu64 " entryLsn# %" PRIu64 "): " "HugeBlob apply(stripe): rec# %s", Guid, lsn, LogPos.EntryPointLsn, rec.ToString().data()));
                } else {
                    // this blob was cut out of the slot heap, so whatever the stripe heap thought it held here is stale
                    RecoveryReleaseStripeChunk(rec.DiskAddr.ChunkIdx);
                    NHuge::THugeSlot hugeSlot(Heap->ConvertDiskPartToHugeSlot(rec.DiskAddr));
                    if (DeleteSlotInFlight(hugeSlot)) {
                        YDB_LOG_DEBUG_CTX(ctx, VDISKP(VCtx->VDiskLogPrefix, "Recovery(guid# %" PRIu64 " lsn# %" PRIu64 " entryLsn# %" PRIu64 "): " "HugeBlob apply(1): rec# %s hugeSlot# %s", Guid, lsn, LogPos.EntryPointLsn, rec.ToString().data(), hugeSlot.ToString().data()));
                    } else {
                        YDB_LOG_DEBUG_CTX(ctx, VDISKP(VCtx->VDiskLogPrefix, "Recovery(guid# %" PRIu64 " lsn# %" PRIu64 " entryLsn# %" PRIu64 "): " "HugeBlob apply(2): rec# %s hugeSlot# %s", Guid, lsn, LogPos.EntryPointLsn, rec.ToString().data(), hugeSlot.ToString().data()));
                        Heap->RecoveryModeAllocate(rec.DiskAddr);
                    }
                }
                LogPos.HugeBlobLoggedLsn = lsn;
                PersistentLsn = Min(PersistentLsn, lsn);
                return TRlas(true, false);
            } else {
                YDB_LOG_DEBUG_CTX(ctx, VDISKP(VCtx->VDiskLogPrefix, "Recovery(guid# %" PRIu64 " lsn# %" PRIu64 " entryLsn# %" PRIu64 "): " "HugeBlob skip: rec# %s", Guid, lsn, LogPos.EntryPointLsn, rec.ToString().data()));
                return TRlas(true, true);
            }
        }

        TRlas THullHugeKeeperPersState::ApplyEntryPoint(
                const TActorContext &ctx,
                ui64 lsn,
                const TContiguousSpan &data)
        {
            if (!CheckEntryPoint(data))
                return TRlas(false, true);

            THullHugeRecoveryLogPos logPos;

            const char *cur = data.data();
            ui32 signature = ReadUnaligned<ui32>(cur);
            cur += sizeof(ui32);

            if (signature == SignatureV2) {
                NKikimrVDiskData::THugeKeeperEntryPoint entryPoint;
                bool success = entryPoint.ParseFromArray(cur, data.size() - sizeof(ui32));
                Y_VERIFY_S(success, VCtx->VDiskLogPrefix);
                logPos.LoadFromProto(entryPoint.GetLogPos());

            } else if (signature == Signature) {
                logPos.ParseFromArray(VCtx->VDiskLogPrefix, cur, THullHugeRecoveryLogPos::SerializedSize);
            }

            Y_VERIFY_S(logPos.EntryPointLsn == lsn, VCtx->VDiskLogPrefix);

            YDB_LOG_DEBUG_CTX(ctx, VDISKP(VCtx->VDiskLogPrefix, "Recovery(guid# %" PRIu64 " lsn# %" PRIu64 " entryLsn# %" PRIu64 "): " "EntryPoint: logPos# %s", Guid, lsn, LogPos.EntryPointLsn, logPos.ToString().data()));

            return TRlas(true, false);
        }

        void THullHugeKeeperPersState::FinishRecovery(const TActorContext &ctx) {
            // handle SlotsInFlight
            for (const auto &x : SlotsInFlight) {
                if (!IsStripeAddr(x.GetDiskPart())) {
                    FreeBlob(x.GetDiskPart());
                }
            }
            SlotsInFlight.clear();

            Recovered = true;
            YDB_LOG_DEBUG_CTX(ctx, VDISKP(VCtx->VDiskLogPrefix, "Recovery(guid# %" PRIu64 ") finished", Guid));
        }

        void THullHugeKeeperPersState::VerifyHeapsDisjoint() const {
            if (!StripeHeap) {
                return;
            }
            // Every allocation routing decision is keyed by which heap currently owns the chunk, so the two heaps must
            // never own the same one. This cannot be checked through the merged set built by GetOwnedChunks, because
            // chunks of either heap are legitimately listed there by the SSTs referencing blobs within them.
            TSet<TChunkIdx> slotChunks;
            Heap->GetOwnedChunks(slotChunks);
            THashSet<ui32> stripeChunks;
            StripeHeap->CollectChunkIds(stripeChunks);
            for (ui32 chunkIdx : stripeChunks) {
                Y_VERIFY_S(!slotChunks.contains(chunkIdx), VCtx->VDiskLogPrefix << "chunkIdx# " << chunkIdx
                    << " is owned by both the slot heap and the stripe heap");
            }
        }

        void THullHugeKeeperPersState::GetOwnedChunks(TSet<TChunkIdx>& chunks) const {
            VerifyHeapsDisjoint();
            Heap->GetOwnedChunks(chunks);
            if (StripeHeap) {
                StripeHeap->GetOwnedChunks(chunks);
            }
        }

        void THullHugeKeeperPersState::AddSlotInFlight(THugeSlot hugeSlot) {
            const auto [it, inserted] = SlotsInFlight.insert(hugeSlot);
            Y_VERIFY_S(inserted, VCtx->VDiskLogPrefix);
        }

        void THullHugeKeeperPersState::CollectStripeChunks(THashSet<TChunkIdx>& chunks) const {
            if (StripeHeap) {
                StripeHeap->CollectChunkIds(chunks);
            }
        }

        THugeSlot THullHugeKeeperPersState::ResolveSlotInFlight(const TDiskPart &addr) const {
            const auto it = SlotsInFlight.find(THugeSlot(addr.ChunkIdx, addr.Offset, 0));
            Y_VERIFY_S(it != SlotsInFlight.end(), VCtx->VDiskLogPrefix << " addr# " << addr.ToString());
            return *it;
        }

        void THullHugeKeeperPersState::ShrinkSlotInFlight(const TDiskPart &addr) {
            const auto it = SlotsInFlight.find(THugeSlot(addr.ChunkIdx, addr.Offset, 0));
            Y_VERIFY_S(it != SlotsInFlight.end(), VCtx->VDiskLogPrefix << " addr# " << addr.ToString());
            const THugeSlot inFlight = *it;
            const ui32 newSize = StripeHeap->AlignSize(addr.Size);
            if (newSize == inFlight.GetSize()) {
                return;
            }
            StripeHeap->ShrinkStripe(inFlight, newSize);
            SlotsInFlight.erase(it);
            const auto [_, inserted] = SlotsInFlight.insert(THugeSlot(addr.ChunkIdx, addr.Offset, newSize));
            Y_VERIFY_S(inserted, VCtx->VDiskLogPrefix << " addr# " << addr.ToString());
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
            if (StripeHeap && StripeHeap->ContainsChunk(hugeSlot.GetChunkId())) {
                return;
            }
            const auto it = ChunkToSlotSize.emplace(hugeSlot.GetChunkId(), std::make_tuple(0, hugeSlot.GetSize())).first;
            auto& [refcount, size] = it->second;
            Y_VERIFY_DEBUG_S(size == hugeSlot.GetSize(), VCtx->VDiskLogPrefix << "HugeSlot# " << hugeSlot.ToString()
                << " Expected# " << size);
            if (size != hugeSlot.GetSize() && TlsActivationContext) {
                YDB_LOG_CRIT("Size is not as",
                    {"VDiskLogPrefix", VCtx->VDiskLogPrefix},
                    {"hugeSlot", hugeSlot},
                    {"expected", size});
            }
            ++refcount;
        }

        void THullHugeKeeperPersState::DeleteChunkSize(THugeSlot hugeSlot) {
            if (StripeHeap && StripeHeap->ContainsChunk(hugeSlot.GetChunkId())) {
                return;
            }
            const auto jt = ChunkToSlotSize.find(hugeSlot.GetChunkId());
            Y_VERIFY_S(jt != ChunkToSlotSize.end(), VCtx->VDiskLogPrefix << "HugeSlot# " << hugeSlot.ToString());
            auto& [refcount, size] = jt->second;
            Y_VERIFY_DEBUG_S(size == hugeSlot.GetSize(), VCtx->VDiskLogPrefix << "HugeSlot# " << hugeSlot.ToString()
                << " Expected# " << size);
            if (size != hugeSlot.GetSize() && TlsActivationContext) {
                YDB_LOG_CRIT("Size is not as",
                    {"VDiskLogPrefix", VCtx->VDiskLogPrefix},
                    {"hugeSlot", hugeSlot},
                    {"expected", size});
            }
            if (!--refcount) {
                ChunkToSlotSize.erase(jt);
            }
        }

        void THullHugeKeeperPersState::RegisterBlob(TDiskPart diskPart) {
            if (IsStripeAddr(diskPart)) {
                return;
            }
            AddChunkSize(Heap->ConvertDiskPartToHugeSlot(diskPart));
        }

        bool THullHugeKeeperPersState::UseStripeAllocator() const {
            if (StripeAllocatorEnabled) {
                return true;
            }
            if (!TlsActivationContext) {
                return false;
            }
            return AppData()->FeatureFlags.GetEnableVDiskHeapAllocator();
        }

        bool THullHugeKeeperPersState::IsStripeAddr(const TDiskPart &addr) const {
            return StripeHeap && StripeHeap->ContainsChunk(addr.ChunkIdx);
        }

        bool THullHugeKeeperPersState::AllocateBlob(ui32 size, THugeSlot *hugeSlot, ui32 *allocKey) {
            if (UseStripeAllocator()) {
                *allocKey = Max<ui32>();
                if (StripeHeap->Allocate(size, hugeSlot)) {
                    return true;
                }
                if (const ui32 chunkId = Heap->TryStealFreeChunk()) {
                    StripeHeap->Allocate(size, hugeSlot, chunkId);
                    return true;
                }
                return false;
            }
            return Heap->Allocate(size, hugeSlot, allocKey);
        }

        TFreeRes THullHugeKeeperPersState::FreeBlob(const TDiskPart &addr) {
            if (IsStripeAddr(addr)) {
                TFreeRes res = StripeHeap->Free(addr);
                if (res.ChunkId) {
                    Heap->AddChunk(res.ChunkId);
                }
                return res;
            }
            return Heap->Free(addr);
        }

        THugeSlot THullHugeKeeperPersState::ConvertDiskPart(const TDiskPart &addr) const {
            if (IsStripeAddr(addr)) {
                return StripeHeap->ConvertDiskPart(addr);
            }
            return Heap->ConvertDiskPartToHugeSlot(addr);
        }

        THeapStat THullHugeKeeperPersState::GetHeapStat() const {
            THeapStat st = Heap->GetStat();
            if (StripeHeap) {
                st += StripeHeap->GetStat();
            }
            return st;
        }

        bool THullHugeKeeperPersState::LockChunkForAllocation(ui32 chunkId, ui32 slotSize) {
            if (StripeHeap && StripeHeap->LockChunk(chunkId)) {
                return true;
            }
            if (slotSize) {
                return Heap->LockChunkForAllocation(chunkId, slotSize);
            }
            return false;
        }

        void THullHugeKeeperPersState::ShredNotify(const std::vector<ui32>& chunksToShred) {
            Heap->ShredNotify(chunksToShred);
            if (StripeHeap) {
                StripeHeap->ShredNotify(chunksToShred);
            }
        }

        void THullHugeKeeperPersState::ListChunks(const THashSet<TChunkIdx>& chunksOfInterest, THashSet<TChunkIdx>& chunks) {
            Heap->ListChunks(chunksOfInterest, chunks);
            if (StripeHeap) {
                StripeHeap->ListChunks(chunksOfInterest, chunks);
            }
        }

        THashSet<TChunkIdx> THullHugeKeeperPersState::GetForbiddenChunks() const {
            THashSet<TChunkIdx> res = Heap->GetForbiddenChunks();
            if (StripeHeap) {
                auto extra = StripeHeap->GetForbiddenChunks();
                res.insert(extra.begin(), extra.end());
            }
            return res;
        }

        ui32 THullHugeKeeperPersState::RemoveChunk() {
            if (StripeHeap) {
                if (const ui32 chunkId = StripeHeap->RemoveChunk()) {
                    return chunkId;
                }
            }
            return Heap->RemoveChunk();
        }

        void THullHugeKeeperPersState::RecoveryClaimStripeChunk(ui32 chunkIdx) {
            if (!StripeHeap->ContainsChunk(chunkIdx)) {
                Heap->RecoveryModeRemoveChunks(TVector<ui32>{chunkIdx});
                StripeHeap->AddChunk(chunkIdx);
            }
        }

        void THullHugeKeeperPersState::RecoveryReleaseStripeChunk(ui32 chunkIdx) {
            if (StripeHeap && StripeHeap->ForgetChunk(chunkIdx)) {
                Heap->RecoveryModeAddChunk(chunkIdx);
            }
        }

        void THullHugeKeeperPersState::RecoveryOccupyDerived(const TDiskPart& addr) {
            StripeHeap->RecoveryOccupyDerived(addr);
        }

        void THullHugeKeeperPersState::FinishStripeDerivation() {
            for (ui32 chunkId : StripeHeap->DropUnreferencedChunks()) {
                Heap->AddChunk(chunkId);
            }
        }

    } // NHuge
} // NKikimr
