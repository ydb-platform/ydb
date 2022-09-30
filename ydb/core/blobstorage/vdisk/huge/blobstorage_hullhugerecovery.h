#pragma once

#include "defs.h"
#include "blobstorage_hullhugedefs.h"

#include <util/generic/hash_set.h>

namespace NKikimr {

    namespace NHuge {

        ////////////////////////////////////////////////////////////////////////////
        // THullHugeRecoveryLogPos
        ////////////////////////////////////////////////////////////////////////////
        struct THullHugeRecoveryLogPos {
            ui64 ChunkAllocationLsn = 0;
            ui64 ChunkFreeingLsn = 0;
            ui64 HugeBlobLoggedLsn = 0;
            ui64 LogoBlobsDbSlotDelLsn = 0;
            ui64 BlocksDbSlotDelLsn = 0;
            ui64 BarriersDbSlotDelLsn = 0;
            ui64 EntryPointLsn = 0;

            static const ui32 SerializedSize = sizeof(ui64) * 7;

            THullHugeRecoveryLogPos(ui64 allocLsn, ui64 freeLsn, ui64 blobLoggedLsn,
                                    ui64 logoBlobsDelLsn, ui64 blocksDelLsn,
                                    ui64 barriersDelLsn, ui64 entryLsn)
                : ChunkAllocationLsn(allocLsn)
                , ChunkFreeingLsn(freeLsn)
                , HugeBlobLoggedLsn(blobLoggedLsn)
                , LogoBlobsDbSlotDelLsn(logoBlobsDelLsn)
                , BlocksDbSlotDelLsn(blocksDelLsn)
                , BarriersDbSlotDelLsn(barriersDelLsn)
                , EntryPointLsn(entryLsn)
            {}

            THullHugeRecoveryLogPos(const THullHugeRecoveryLogPos &) = default;
            THullHugeRecoveryLogPos &operator=(const THullHugeRecoveryLogPos &) = default;

            static THullHugeRecoveryLogPos Default() {
                return THullHugeRecoveryLogPos(0, 0, 0, 0, 0, 0, 0);
            }

            ui64 FirstLsnToKeep() const;
            TString FirstLsnToKeepDecomposed() const;
            TString ToString() const;
            TString Serialize() const;
            void ParseFromString(const TString &serialized);
            void ParseFromArray(const char* data, size_t size);
            static bool CheckEntryPoint(const TString &serialized);
        };

        ////////////////////////////////////////////////////////////////////////////
        // TRlas - Recovery Log Application Status
        ////////////////////////////////////////////////////////////////////////////
        struct TRlas {
            bool Ok;    // record was treated correctly (ok=false -> can't recover)
            bool Skip;  // record was skipped, since it's already in state

            TRlas(bool ok, bool skip)
                : Ok(ok)
                , Skip(skip)
            {}
            TRlas(const TRlas &) = default;
            TRlas &operator=(const TRlas &) = default;
        };

        ////////////////////////////////////////////////////////////////////////////
        // TLogTracker
        // This class tracks two entry points: current and previous
        // It alse correctly implements recovery procedure.
        // The idea behind: we MUST keep all log records between Prev and Cur
        // entry points
        ////////////////////////////////////////////////////////////////////////////
        class TLogTracker {
        public:
            struct TPosition {
                ui64 EntryPointLsn = 0;
                ui64 HugeBlobLoggedLsn = 0;
                TPosition(const THullHugeRecoveryLogPos &logPos);
                TPosition() = default;
                void Output(IOutputStream &str) const;
                TString ToString() const;
            };

            // RecoveryMode: call on every entry point seen
            void EntryPointFromRecoveryLog(TPosition pos);
            // RecoveryMode: call when recovery is finished
            void FinishRecovery(ui64 entryPointLsn);

            // Prepare to commit
            void InitiateNewEntryPointCommit(TPosition pos);
            // Committed
            void EntryPointCommitted(ui64 lsn);
            // Calculate FirstLsnToKeep
            ui64 FirstLsnToKeep() const;
            TString FirstLsnToKeepDecomposed() const;
            bool WouldNewEntryPointAdvanceLog(ui64 freeUpToLsn, bool inFlightWrites) const;
        private:
            TMaybe<TPosition> Prev = {};
            TMaybe<TPosition> Cur = {};
            TMaybe<TPosition> InProgress = {};

            void PrivateNewLsn(TPosition pos);
        };

        ////////////////////////////////////////////////////////////////////////////
        // THullHugeKeeperPersState
        ////////////////////////////////////////////////////////////////////////////
        class THeap;

        struct THullHugeKeeperPersState {
            typedef THashSet<NHuge::THugeSlot> TAllocatedSlots;
            static const ui32 Signature;
            static const ui32 MilestoneHugeBlobInBytes;

            TIntrusivePtr<TVDiskContext> VCtx;
            // current pos
            THullHugeRecoveryLogPos LogPos;
            // last committed log pos
            THullHugeRecoveryLogPos CommittedLogPos;
            std::unique_ptr<NHuge::THeap> Heap;
            // slots that are already allocated, but not written to log
            TAllocatedSlots AllocatedSlots;
            // guard to avoid using structure before recovery has been completed
            bool Recovered = false;
            // manage last two entry points
            TLogTracker LogTracker;
            // guid for this instance of pers state
            const ui64 Guid;

            THullHugeKeeperPersState(TIntrusivePtr<TVDiskContext> vctx,
                                     const ui32 chunkSize,
                                     const ui32 appendBlockSize,
                                     const ui32 minHugeBlobInBytes,
                                     const ui32 milestoneHugeBlobInBytes,
                                     const ui32 maxBlobInBytes,
                                     const ui32 overhead,
                                     const ui32 freeChunksReservation,
                                     const bool oldMapCompatible,
                                     std::function<void(const TString&)> logFunc);
            THullHugeKeeperPersState(TIntrusivePtr<TVDiskContext> vctx,
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
                                     std::function<void(const TString&)> logFunc);
            THullHugeKeeperPersState(TIntrusivePtr<TVDiskContext> vctx,
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
                                     std::function<void(const TString&)> logFunc);
            ~THullHugeKeeperPersState();

            TString Serialize() const;
            void ParseFromString(const TString &data);
            void ParseFromArray(const char* data, size_t size);
            static TString ExtractLogPosition(const TString &data);
            static TContiguousSpan ExtractLogPosition(TContiguousSpan data);
            static bool CheckEntryPoint(const TString &data);
            static bool CheckEntryPoint(TContiguousSpan data);
            TString ToString() const;
            void RenderHtml(IOutputStream &str) const;
            ui32 GetMinREALHugeBlobInBytes() const;
            ui64 FirstLsnToKeep() const;
            TString FirstLsnToKeepDecomposed() const;
            bool WouldNewEntryPointAdvanceLog(ui64 freeUpToLsn, bool inFlightWrites) const;

            // initiate commit
            void InitiateNewEntryPointCommit(ui64 lsn, bool inFlightWrites);
            // finish commit
            void EntryPointCommitted(ui64 lsn);

            enum ESlotDelDbType {
                LogoBlobsDb,
                BlocksDb,
                BarriersDb
            };

            static const char *SlotDelDbTypeToStr(ESlotDelDbType dbType) {
                switch (dbType) {
                    case LogoBlobsDb:   return "LogoBlobsDb";
                    case BlocksDb:      return "BlocksDb";
                    case BarriersDb:    return "BarriersDb";
                    default:            return "UNKNOWN";
                }
            }

            // Recovery log application
            TRlas Apply(const TActorContext &ctx,
                        ui64 lsn,
                        const NHuge::TAllocChunkRecoveryLogRec &rec);
            TRlas Apply(const TActorContext &ctx,
                        ui64 lsn,
                        const NHuge::TFreeChunkRecoveryLogRec &rec);
            TRlas ApplySlotsDeletion(const TActorContext &ctx,
                        ui64 lsn,
                        const TDiskPartVec &rec,
                        ESlotDelDbType type);
            TRlas Apply(const TActorContext &ctx,
                        ui64 lsn,
                        const NHuge::TPutRecoveryLogRec &rec);
            TRlas ApplyEntryPoint(const TActorContext &ctx,
                        ui64 lsn,
                        const TString &data);
            TRlas ApplyEntryPoint(const TActorContext &ctx,
                        ui64 lsn,
                        const TContiguousSpan &data);

            void FinishRecovery(const TActorContext &ctx);

            void GetOwnedChunks(TSet<TChunkIdx>& chunks) const;
        };

    } // NHuge
} // NKikimr
