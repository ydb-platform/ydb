#pragma once

#include "defs.h"
#include "blobstorage_hullhugedefs.h"
#include "blobstorage_hullhugestripe.h"

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

            THullHugeRecoveryLogPos() = default;

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

            TString ToString() const;

            TString Serialize() const;
            void ParseFromArray(const TString& prefix, const char* data, size_t size);

            void SaveToProto(NKikimrVDiskData::THullHugeRecoveryLogPos& logPos) const;
            void LoadFromProto(const NKikimrVDiskData::THullHugeRecoveryLogPos& logPos);
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
            static const ui32 Signature;
            static const ui32 SignatureV2;

            TIntrusivePtr<TVDiskContext> VCtx;
            // current pos
            THullHugeRecoveryLogPos LogPos;
            std::unique_ptr<NHuge::THeap> Heap;
            std::unique_ptr<NHuge::TStripeHeap> StripeHeap;
            // slots that are already allocated, but not written to log
            THashSet<THugeSlot> SlotsInFlight;
            THashMap<ui32, std::tuple<ui32, ui32>> ChunkToSlotSize;
            // guard to avoid using structure before recovery has been completed
            bool Recovered = false;
            // guid for this instance of pers state
            const ui64 Guid;
            // last reported FirstLsnToKeep; can't decrease
            mutable ui64 FirstLsnToKeepReported = 0;
            ui64 PersistentLsn = 0;
            bool LoadedFromProto = false;
            bool EnableTinyDisks = false;
            bool StripeAllocatorEnabled = false;
            TControlWrapper ChunksSoftLocking;

            THullHugeKeeperPersState(TIntrusivePtr<TVDiskContext> vctx,
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
                                     std::function<void(const TString&)> logFunc);

            THullHugeKeeperPersState(TIntrusivePtr<TVDiskContext> vctx,
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
                                     std::function<void(const TString&)> logFunc);

            ~THullHugeKeeperPersState();

            TString Serialize() const;
            void ParseFromArray(const char* data, size_t size);

            TString SaveToProto() const;
            void LoadFromProto(const char* data, size_t size);

            static bool CheckEntryPoint(TContiguousSpan data);
            TString ToString() const;
            void RenderHtml(IOutputStream &str) const;
            ui32 GetMinHugeBlobInBytes() const;
            ui64 FirstLsnToKeep(ui64 minInFlightLsn = Max<ui64>()) const;
            TString FirstLsnToKeepDecomposed() const;
            bool WouldNewEntryPointAdvanceLog(ui64 freeUpToLsn, ui64 minInFlightLsn, ui32 itemsAfterCommit) const;

            // initiate commit
            void InitiateNewEntryPointCommit(ui64 lsn, ui64 minInFlightLsn);
            // finish commit
            void EntryPointCommitted(ui64 lsn);

            void AddSlotInFlight(THugeSlot hugeSlot);
            bool DeleteSlotInFlight(THugeSlot hugeSlot);
            // The in-flight record is the authority on how much space an allocation actually reserved: an SST reserves
            // a worst-case stripe and only later learns how much of it it filled.
            THugeSlot ResolveSlotInFlight(const TDiskPart &addr) const;
            // Give back the unused tail of an in-flight stripe once the committed length is known.
            void ShrinkSlotInFlight(const TDiskPart &addr);

            void AddChunkSize(THugeSlot hugeSlot);
            void DeleteChunkSize(THugeSlot hugeSlot);
            void RegisterBlob(TDiskPart diskPart);

            bool UseStripeAllocator() const;
            // NOTE: ownership of a chunk migrates back to the slot heap as soon as its last stripe is freed, so this
            // has to be queried *before* freeing a blob when the answer is needed afterwards
            bool IsStripeAddr(const TDiskPart &addr) const;
            bool AllocateBlob(ui32 size, THugeSlot *hugeSlot, ui32 *allocKey);
            TFreeRes FreeBlob(const TDiskPart &addr);
            THugeSlot ConvertDiskPart(const TDiskPart &addr) const;
            THeapStat GetHeapStat() const;
            bool LockChunkForAllocation(ui32 chunkId, ui32 slotSize);
            void ShredNotify(const std::vector<ui32>& chunksToShred);
            void ListChunks(const THashSet<TChunkIdx>& chunksOfInterest, THashSet<TChunkIdx>& chunks);
            THashSet<TChunkIdx> GetForbiddenChunks() const;
            ui32 RemoveChunk();
            // Log replay establishes only which chunks belong to the stripe heap; the extents inside them come from
            // the hull's references once replay is over.
            void RecoveryClaimStripeChunk(ui32 chunkIdx);
            // Its mirror. Replay watches a chunk enter the stripe heap but cannot watch it leave: a chunk goes back
            // to the slot heap when its last stripe is freed, and stripes are not tracked until derivation. So the
            // records that put the chunk to slot use, or hand it back to PDisk, are what retire the claim.
            void RecoveryReleaseStripeChunk(ui32 chunkIdx);
            void RecoveryOccupyDerived(const TDiskPart& addr);
            void FinishStripeDerivation();
            void CollectStripeChunks(THashSet<TChunkIdx>& chunks) const;

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
                        const TDiskPartVec& allocated,
                        const TDiskPartVec& allocatedStripe,
                        ESlotDelDbType type);
            TRlas Apply(const TActorContext &ctx,
                        ui64 lsn,
                        const NHuge::TPutRecoveryLogRec &rec);
            TRlas ApplyEntryPoint(const TActorContext &ctx,
                        ui64 lsn,
                        const TContiguousSpan &data);

            void FinishRecovery(const TActorContext &ctx);

            // a chunk is owned either by the slot heap or by the stripe heap, but never by both
            void VerifyHeapsDisjoint() const;
            void GetOwnedChunks(TSet<TChunkIdx>& chunks) const;
        };

    } // NHuge
} // NKikimr
