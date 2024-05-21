#pragma once

#include "defs.h"
#include "blobstorage_hullhugedefs.h"

#include <util/generic/hash_set.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_context.h>

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
        // THullHugeKeeperPersState
        ////////////////////////////////////////////////////////////////////////////
        class THeap;

        struct THullHugeKeeperPersState {
            typedef THashSet<NHuge::THugeSlot> TAllocatedSlots;
            static const ui32 Signature;

            TIntrusivePtr<TVDiskContext> VCtx;
            // current pos
            THullHugeRecoveryLogPos LogPos;
            std::unique_ptr<NHuge::THeap> Heap;
            // slots that are already allocated, but not written to log
            TAllocatedSlots AllocatedSlots;
            // guard to avoid using structure before recovery has been completed
            bool Recovered = false;
            // guid for this instance of pers state
            const ui64 Guid;
            // last reported FirstLsnToKeep; can't decrease
            mutable ui64 FirstLsnToKeepReported = 0;
            ui64 PersistentLsn = 0;

            THullHugeKeeperPersState(TIntrusivePtr<TVDiskContext> vctx,
                                     const ui32 chunkSize,
                                     const ui32 appendBlockSize,
                                     const ui32 minHugeBlobInBytes,
                                     const ui32 oldMinHugeBlobInBytes,
                                     const ui32 milestoneHugeBlobInBytes,
                                     const ui32 maxBlobInBytes,
                                     const ui32 overhead,
                                     const ui32 freeChunksReservation,
                                     std::function<void(const TString&)> logFunc);
            THullHugeKeeperPersState(TIntrusivePtr<TVDiskContext> vctx,
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
                                     std::function<void(const TString&)> logFunc);
            THullHugeKeeperPersState(TIntrusivePtr<TVDiskContext> vctx,
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
            ui64 FirstLsnToKeep(ui64 minInFlightLsn = Max<ui64>()) const;
            TString FirstLsnToKeepDecomposed() const;
            bool WouldNewEntryPointAdvanceLog(ui64 freeUpToLsn, ui64 minInFlightLsn, ui32 itemsAfterCommit) const;

            // initiate commit
            void InitiateNewEntryPointCommit(ui64 lsn, ui64 minInFlightLsn);
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
