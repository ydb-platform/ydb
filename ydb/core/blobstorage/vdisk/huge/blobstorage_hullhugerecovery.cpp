#include "blobstorage_hullhugerecovery.h"
#include "blobstorage_hullhugeheap.h"

using namespace NKikimrServices;

namespace NKikimr {
    namespace NHuge {

        ////////////////////////////////////////////////////////////////////////////
        // THullHugeRecoveryLogPos
        ////////////////////////////////////////////////////////////////////////////
        ui64 THullHugeRecoveryLogPos::FirstLsnToKeep() const {
            return Min(LogoBlobsDbSlotDelLsn, BlocksDbSlotDelLsn, BarriersDbSlotDelLsn,
                EntryPointLsn, HugeBlobLoggedLsn);

            // NOTE: we do use HugeBlobLoggedLsn for LastKeepLsn calculation, because in
            //       run time we may have some in-fly messages to log, that are not reflected
            //       in HugeBlobLoggedLsn yet

            // NOTE: LogoBlobsDbSlotDelLsn, BlocksDbSlotDelLsn and BarriersDbSlotDelLsn are
            //       continiously increasing even if nobody writes to the database, because
            //       the hull component regularly writes its state into the log. This allows us
            //       to calculate LastKeepLsn based on these values and also progress continiously.
        }

        TString THullHugeRecoveryLogPos::FirstLsnToKeepDecomposed() const {
            TStringStream str;
            str << "{LogoBlobsDbSlotDelLsn# " << LogoBlobsDbSlotDelLsn
                << " BlocksDbSlotDelLsn# " << BlocksDbSlotDelLsn
                << " BarriersDbSlotDelLsn# " << BarriersDbSlotDelLsn
                << " EntryPointLsn# " << EntryPointLsn
                << " HugeBlobLoggedLsn# " << HugeBlobLoggedLsn
                << "}";
            return str.Str();
        }

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
                Y_VERIFY(static_cast<size_t>(end - cur) >= sizeof(*var));
                memcpy(var, cur, sizeof(*var));
                cur += sizeof(*var);
            }
            Y_VERIFY(cur == end);
        }

        bool THullHugeRecoveryLogPos::CheckEntryPoint(const TString &serialized) {
            return serialized.size() == SerializedSize;
        }


        ////////////////////////////////////////////////////////////////////////////
        // TLogTracker
        ////////////////////////////////////////////////////////////////////////////
        TLogTracker::TPosition::TPosition(const THullHugeRecoveryLogPos &logPos)
            : EntryPointLsn(logPos.EntryPointLsn)
            , HugeBlobLoggedLsn(logPos.HugeBlobLoggedLsn)
        {}

        void TLogTracker::TPosition::Output(IOutputStream &str) const {
            str << "{EntryPointLsn# " << EntryPointLsn << " HugeBlobLoggedLsn# " << HugeBlobLoggedLsn << "}";
        }

        TString TLogTracker::TPosition::ToString() const {
            TStringStream str;
            Output(str);
            return str.Str();
        }

        void TLogTracker::EntryPointFromRecoveryLog(TPosition pos) {
            PrivateNewLsn(pos);
        }

        void TLogTracker::FinishRecovery(ui64 entryPointLsn) {
            Y_VERIFY(entryPointLsn == 0 || Cur->EntryPointLsn == entryPointLsn);
        }

        // Prepare to commit
        void TLogTracker::InitiateNewEntryPointCommit(TPosition pos) {
            Y_VERIFY(InProgress.Empty());
            InProgress = pos;
        }

        // Committed
        void TLogTracker::EntryPointCommitted(ui64 lsn) {
            Y_VERIFY(InProgress.Defined() && InProgress->EntryPointLsn == lsn);
            Prev = Cur;
            Cur = InProgress;
            InProgress.Clear();
        }

        ui64 TLogTracker::FirstLsnToKeep() const {
            return Prev.Empty() ? 0 : Min(Prev->EntryPointLsn, Prev->HugeBlobLoggedLsn);
        }

        TString TLogTracker::FirstLsnToKeepDecomposed() const {
            TStringStream str;
            str << "{Prev# " << Prev << " Cur# " << Cur << "}";
            return str.Str();
        }

        bool TLogTracker::WouldNewEntryPointAdvanceLog(ui64 freeUpToLsn, bool inFlightWrites) const {
            Y_UNUSED(inFlightWrites);
            Y_VERIFY(InProgress.Empty());
            return FirstLsnToKeep() < freeUpToLsn;
        }

        void TLogTracker::PrivateNewLsn(TPosition pos) {
            Y_VERIFY(pos.EntryPointLsn != 0 &&
                    ((Prev.Empty() && Cur.Empty()) ||
                     (Prev.Empty() && Cur.Defined()) ||
                     (Prev.Defined() && Cur.Defined() &&
                      Prev->EntryPointLsn < Cur->EntryPointLsn)) &&
                    (Cur.Empty() || pos.EntryPointLsn > Cur->EntryPointLsn));

            Prev = Cur;
            Cur = pos;
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
                                                           const bool oldMapCompatible,
                                                           std::function<void(const TString&)> logFunc)
            : VCtx(std::move(vctx))
            , LogPos(THullHugeRecoveryLogPos::Default())
            , CommittedLogPos(LogPos)
            , Heap(new NHuge::THeap(VCtx->VDiskLogPrefix, chunkSize, appendBlockSize,
                                    minHugeBlobInBytes, milestoneHugeBlobInBytes,
                                    maxBlobInBytes, overhead, freeChunksReservation,
                                    oldMapCompatible))
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
                                                           const ui32 milestoneHugeBlobInBytes,
                                                           const ui32 maxBlobInBytes,
                                                           const ui32 overhead,
                                                           const ui32 freeChunksReservation,
                                                           const bool oldMapCompatible,
                                                           const ui64 entryPointLsn,
                                                           const TString &entryPointData,
                                                           std::function<void(const TString&)> logFunc)
            : VCtx(std::move(vctx))
            , LogPos(THullHugeRecoveryLogPos::Default())
            , CommittedLogPos(LogPos)
            , Heap(new NHuge::THeap(VCtx->VDiskLogPrefix, chunkSize, appendBlockSize,
                                    minHugeBlobInBytes, milestoneHugeBlobInBytes,
                                    maxBlobInBytes, overhead, freeChunksReservation,
                                    oldMapCompatible))
            , AllocatedSlots()
            , Guid(TAppData::RandomProvider->GenRand64())
        {
            ParseFromString(entryPointData);
            Y_VERIFY(entryPointLsn == LogPos.EntryPointLsn);
            logFunc(VDISKP(VCtx->VDiskLogPrefix,
                "Recovery started (guid# %" PRIu64 " entryLsn# %" PRIu64 "): State# %s",
                Guid, entryPointLsn, ToString().data()));
            CommittedLogPos = LogPos;
        }

        THullHugeKeeperPersState::THullHugeKeeperPersState(TIntrusivePtr<TVDiskContext> vctx,
                                                           const ui32 chunkSize,
                                                           const ui32 appendBlockSize,
                                                           const ui32 minHugeBlobInBytes,
                                                           const ui32 milestoneHugeBlobInBytes,
                                                           const ui32 maxBlobInBytes,
                                                           const ui32 overhead,
                                                           const ui32 freeChunksReservation,
                                                           const bool oldMapCompatible,
                                                           const ui64 entryPointLsn,
                                                           const TContiguousSpan &entryPointData,
                                                           std::function<void(const TString&)> logFunc)
            : VCtx(std::move(vctx))
            , LogPos(THullHugeRecoveryLogPos::Default())
            , CommittedLogPos(LogPos)
            , Heap(new NHuge::THeap(VCtx->VDiskLogPrefix, chunkSize, appendBlockSize,
                                    minHugeBlobInBytes, milestoneHugeBlobInBytes,
                                    maxBlobInBytes, overhead, freeChunksReservation,
                                    oldMapCompatible))
            , AllocatedSlots()
            , Guid(TAppData::RandomProvider->GenRand64())
        {
            ParseFromArray(entryPointData.GetData(), entryPointData.GetSize());
            Y_VERIFY(entryPointLsn == LogPos.EntryPointLsn);
            logFunc(VDISKP(VCtx->VDiskLogPrefix,
                "Recovery started (guid# %" PRIu64 " entryLsn# %" PRIu64 "): State# %s",
                Guid, entryPointLsn, ToString().data()));
            CommittedLogPos = LogPos;
        }

        THullHugeKeeperPersState::~THullHugeKeeperPersState() {
        }

        TString THullHugeKeeperPersState::Serialize() const {
            TStringStream str;
            // signature
            str.Write(&Signature, sizeof(ui32));

            // log pos
            TString serializedLogPos = LogPos.Serialize();
            Y_VERIFY_DEBUG(serializedLogPos.size() == THullHugeRecoveryLogPos::SerializedSize);
            str.Write(serializedLogPos.data(), THullHugeRecoveryLogPos::SerializedSize);

            // heap
            TString serializedHeap = Heap->Serialize();
            ui32 heapSize = serializedHeap.size();
            str.Write(&heapSize, sizeof(ui32));
            str.Write(serializedHeap.data(), heapSize);

            // chunks to free -- obsolete field
            const ui32 chunksSize = 0;
            Y_VERIFY(!chunksSize);
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
            Y_VERIFY(!chunksSize);

            // allocated slots
            ui32 slotsSize = ReadUnaligned<ui32>(cur);
            cur += sizeof(ui32); // slots size
            for (ui32 i = 0; i < slotsSize; i++) {
                NHuge::THugeSlot hugeSlot;
                hugeSlot.Parse(cur, cur + NHuge::THugeSlot::SerializedSize);
                cur += NHuge::THugeSlot::SerializedSize;
                cur += sizeof(ui64); // refPointLsn (for backward compatibility, can be removed)
                bool inserted = AllocatedSlots.insert(hugeSlot).second;
                Y_VERIFY(inserted);
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
            str << "LogPos: " << LogPos.ToString() << "<br>";
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

        ui32 THullHugeKeeperPersState::GetMinREALHugeBlobInBytes() const {
            return Heap->GetMinREALHugeBlobInBytes();
        }

        ui64 THullHugeKeeperPersState::FirstLsnToKeep() const {
            return Min(LogPos.FirstLsnToKeep(), LogTracker.FirstLsnToKeep(),
                // special case if these LSM tree entrypoints with deletions would be applied by the recovery code
                CommittedLogPos.LogoBlobsDbSlotDelLsn,
                CommittedLogPos.BlocksDbSlotDelLsn,
                CommittedLogPos.BarriersDbSlotDelLsn);
        }

        TString THullHugeKeeperPersState::FirstLsnToKeepDecomposed() const {
            TStringStream str;
            str << "{LogPos# " << LogPos.FirstLsnToKeepDecomposed()
                << " CommittedLogPos# " << CommittedLogPos.FirstLsnToKeepDecomposed()
                << " LogTracker# " << LogTracker.FirstLsnToKeepDecomposed()
                << "}";
            return str.Str();
        }

        bool THullHugeKeeperPersState::WouldNewEntryPointAdvanceLog(ui64 freeUpToLsn, bool inFlightWrites) const {
            return LogTracker.WouldNewEntryPointAdvanceLog(freeUpToLsn, inFlightWrites);
        }

        // initiate commit
        void THullHugeKeeperPersState::InitiateNewEntryPointCommit(ui64 lsn, bool inFlightWrites) {
            Y_VERIFY(lsn > LogPos.EntryPointLsn);
            // set up previous entry point position to prevent log from being occasionally cut and update new entry
            // point position in persistent state
            LogPos.EntryPointLsn = lsn;
            if (!inFlightWrites) {
                // no active writes are going on, we can promote HugeBlobLoggedLsn
                LogPos.HugeBlobLoggedLsn = lsn;
            }

            TLogTracker::TPosition pos;
            pos.EntryPointLsn = LogPos.EntryPointLsn;
            pos.HugeBlobLoggedLsn = LogPos.HugeBlobLoggedLsn;
            LogTracker.InitiateNewEntryPointCommit(pos);
            CommittedLogPos = LogPos;
        }

        // finish commit
        void THullHugeKeeperPersState::EntryPointCommitted(ui64 entryPointLsn) {
            Y_VERIFY(entryPointLsn == LogPos.EntryPointLsn);
            LogTracker.EntryPointCommitted(entryPointLsn);
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
                    Y_FAIL("Unexpected case");
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
            Y_VERIFY(logPos.EntryPointLsn == lsn);
            LogTracker.EntryPointFromRecoveryLog(logPos);

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
            Y_VERIFY(logPos.EntryPointLsn == lsn);
            LogTracker.EntryPointFromRecoveryLog(logPos);

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

            LogTracker.FinishRecovery(LogPos.EntryPointLsn);
            Recovered = true;
            LOG_DEBUG(ctx, BS_HULLHUGE,
                VDISKP(VCtx->VDiskLogPrefix, "Recovery(guid# %" PRIu64 ") finished", Guid));
        }

        void THullHugeKeeperPersState::GetOwnedChunks(TSet<TChunkIdx>& chunks) const {
            Heap->GetOwnedChunks(chunks);
        }

    } // NHuge
} // NKikimr

Y_DECLARE_OUT_SPEC(, NKikimr::NHuge::TLogTracker::TPosition, stream, value) {
    value.Output(stream);
}
