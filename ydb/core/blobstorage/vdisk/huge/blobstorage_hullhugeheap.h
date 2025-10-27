#pragma once

#include "defs.h"
#include "blobstorage_hullhugedefs.h"

#include <ydb/core/blobstorage/vdisk/common/vdisk_log.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_hugeblobctx.h>
#include <ydb/core/control/lib/immediate_control_board_wrapper.h>
#include <ydb/core/util/bits.h>
#include <util/generic/set.h>
#include <util/ysaveload.h>

namespace NKikimr {

    namespace NHuge {

        // private structures
        namespace NPrivate {

            // TChain operates with TChunkSlot, it doesn't need to know about slot size
            struct TChunkSlot {
                ui32 ChunkId;
                ui32 SlotId;

                TChunkSlot()
                    : ChunkId(0)
                    , SlotId(0)
                {}

                TChunkSlot(ui32 chunkId, ui32 slotId)
                    : ChunkId(chunkId)
                    , SlotId(slotId)
                {}

                ui32 GetChunkId() const {
                    return ChunkId;
                }

                ui32 GetSlotId() const {
                    return SlotId;
                }

                TString ToString() const {
                    TStringStream str;
                    str << "[" << ChunkId << " " << SlotId << "]";
                    return str.Str();
                }

                bool operator ==(const TChunkSlot &id) const {
                    return ChunkId == id.ChunkId && SlotId == id.SlotId;
                }

                bool operator <(const TChunkSlot &id) const {
                    return ChunkId < id.ChunkId || (ChunkId == id.ChunkId && SlotId < id.SlotId);
                }
            };

            // represents one segment in form of (Left, Right]
            struct TLayoutSegment {
                ui32 Left; // not included
                ui32 Right; // included

                bool operator ==(const TLayoutSegment &s) const {
                    return Left == s.Left && Right == s.Right;
                }
            };

            using TLayout = TVector<TLayoutSegment>;

            ////////////////////////////////////////////////////////////////////////
            // TChainLayoutBuilder
            // Builds a map of slots in term of blocks (block=AppendBlockSize).
            ////////////////////////////////////////////////////////////////////////
            class TChainLayoutBuilder {
            public:
                TChainLayoutBuilder(const TString& prefix,
                    ui32 left, ui32 milestone, ui32 right, ui32 overhead);

                TLayout GetLayout() const { return std::move(Layout); }
                const TLayoutSegment &GetMilestoneSegment() const { return Layout.at(MilestoneId); }

                TString ToString(ui32 appendBlockSize = 0) const;
                void Output(IOutputStream &str, ui32 appendBlockSize = 0) const;

            private:
                void Check(const TString& prefix, ui32 left, ui32 right);
                void BuildDownward(ui32 left, ui32 right, ui32 overhead);
                void BuildUpward(ui32 left, ui32 right, ui32 overhead);

                TLayout Layout;
                // An index in Layout vector, where milestone segment starts
                size_t MilestoneId = Max<size_t>();
            };

            ////////////////////////////////////////////////////////////////////////
            // TChainLayoutBuilderV2
            // Next version of layout builder
            ////////////////////////////////////////////////////////////////////////

            class TChainLayoutBuilderV2 {
            public:
                TChainLayoutBuilderV2(const TString& prefix, ui32 blockSize,
                    ui32 blocksInChunk, ui32 left, ui32 right, ui32 stepsBetweenPowersOf2);

                TLayout GetLayout() const { return std::move(Layout); }

                TString ToString() const;
                void Output(IOutputStream &str) const;

            private:
                void Build();
                void Check();

                const TString VDiskLogPrefix;
                const ui32 BlockSize = 0;
                const ui32 BlocksInChunk = 0;
                const ui32 Left = 0;
                const ui32 Right = 0;
                const ui32 StepsBetweenPowersOf2 = 0;

                TLayout Layout;
            };

        } // NPrivate

        ////////////////////////////////////////////////////////////////////////////
        // TChain
        // It manages all slots of some fixed size.
        ////////////////////////////////////////////////////////////////////////////
        class TChain {
            struct TFreeSpaceItem {
                TMask FreeSlots;
                ui32 NumFreeSlots = 0;
            };

            using TChunkID = ui32;
            using TFreeSpace = TMap<TChunkID, TFreeSpaceItem>;

            static constexpr ui32 MaxNumberOfSlots = 32768; // it's not a good idea to have more slots than this
            TString VDiskLogPrefix;
            TMask ConstMask; // mask of 'all slots are free'
            TControlWrapper ChunksSoftLocking;
            TFreeSpace FreeSpace;
            TFreeSpace LockedChunks;
            ui32 AllocatedSlots = 0;
            ui32 FreeSlotsInFreeSpace = 0;

        public:
            ui32 SlotsInChunk;
            ui32 SlotSize; // may be adjusted during deserialization

        public:
            static TMask BuildConstMask(const TString &prefix, ui32 slotsInChunk);

        public:
            TChain(TString vdiskLogPrefix, ui32 slotsInChunk, ui32 slotSize, TControlWrapper chunksSoftLocking)
                : VDiskLogPrefix(std::move(vdiskLogPrefix))
                , ConstMask(BuildConstMask(vdiskLogPrefix, slotsInChunk))
                , ChunksSoftLocking(chunksSoftLocking)
                , SlotsInChunk(slotsInChunk)
                , SlotSize(slotSize)
            {}

            TChain(TString vdiskLogPrefix, const NKikimrVDiskData::THugeKeeperHeap::TChain& chain);

            TChain(TChain&&) = default;
            TChain(const TChain&) = delete;

            TChain& operator=(TChain&&) = default;
            TChain& operator=(const TChain&) = delete;

            void SaveToProto(NKikimrVDiskData::THugeKeeperHeap::TChain& chain) const;
            void LoadFromProto(const NKikimrVDiskData::THugeKeeperHeap::TChain& chain);

            THugeSlot Convert(const NPrivate::TChunkSlot& id) const;
            NPrivate::TChunkSlot Convert(const TDiskPart& addr) const;
            NPrivate::TChunkSlot Convert(const THugeSlot& slot) const;

            // returns true if allocated, false -- if no free slots
            bool Allocate(NPrivate::TChunkSlot *id);
            // allocate id, but we know that this chain doesn't have free slots, so add a chunk to it
            void Allocate(NPrivate::TChunkSlot *id, TChunkID chunkId);
            // returns freed ChunkID if any
            TFreeRes Free(const NPrivate::TChunkSlot &id);
            bool LockChunkForAllocation(TChunkID chunkId);
            THeapStat GetStat() const;
            // returns true is allocated, false otherwise
            bool RecoveryModeAllocate(const NPrivate::TChunkSlot &id);
            void RecoveryModeAllocate(const NPrivate::TChunkSlot &id, TChunkID chunkId, bool inLockedChunks);
            void Save(IOutputStream *s) const;
            bool HaveBeenUsed() const;
            TString ToString() const;
            void RenderHtml(IOutputStream &str) const;
            void RenderHtmlForUsage(IOutputStream &str) const;
            void GetOwnedChunks(TSet<TChunkIdx>& chunks) const;
            void ShredNotify(const std::vector<ui32>& chunksToShred);
            void ListChunks(const THashSet<TChunkIdx>& chunksOfInterest, THashSet<TChunkIdx>& chunks);

            static TChain Load(IInputStream *s, TString vdiskLogPrefix, ui32 appendBlockSize, ui32 blocksInChunk, TControlWrapper chunksSoftLocking);

            template<typename T>
            void ForEachFreeSpaceChunk(T&& callback) const {
                auto freeIt = FreeSpace.begin();
                const auto freeEnd = FreeSpace.end();
                auto lockedIt = LockedChunks.begin();
                const auto lockedEnd = LockedChunks.end();
                while (freeIt != freeEnd && lockedIt != lockedEnd) {
                    if (freeIt->first < lockedIt->first) {
                        callback(*freeIt++);
                    } else if (lockedIt->first < freeIt->first) {
                        callback(*lockedIt++);
                    } else {
                        Y_FAIL_S(VDiskLogPrefix << "intersecting sets of keys for FreeSpace and LockedChunks");
                    }
                }
                std::for_each(freeIt, freeEnd, callback);
                std::for_each(lockedIt, lockedEnd, callback);
            }
        };

        ////////////////////////////////////////////////////////////////////////////
        // TAllChains
        ////////////////////////////////////////////////////////////////////////////
        class TAllChains {
        public:
            TAllChains(const TString& vdiskLogPrefix,
                ui32 chunkSize,
                ui32 appendBlockSize,
                ui32 minHugeBlobInBytes,
                ui32 milestoneBlobInBytes,
                ui32 maxHugeBlobInBytes,
                ui32 overhead,
                ui32 stepsBetweenPowersOf2,
                bool useBucketsV2,
                TControlWrapper chunksSoftLocking);

            TAllChains(const TString& vdiskLogPrefix, const NKikimrVDiskData::THugeKeeperHeap& heap);

            // return a pointer to corresponding chain delegator by object byte size
            TChain *GetChain(ui32 size);
            const TChain *GetChain(ui32 size) const;
            THeapStat GetStat() const;

            void Save(IOutputStream *s) const;
            void Load(IInputStream *s);

            void SaveToProto(NKikimrVDiskData::THugeKeeperHeap& heap) const;
            void LoadFromProto(const NKikimrVDiskData::THugeKeeperHeap& heap);

            void GetOwnedChunks(TSet<TChunkIdx>& chunks) const;
            TString ToString() const;
            void RenderHtml(IOutputStream &str) const;
            void RenderHtmlForUsage(IOutputStream &str) const;
            // for testing purposes
            NPrivate::TLayout GetLayout() const;
            // Builds a map of BlobSize -> THugeSlotsMap::TSlotInfo for THugeBlobCtx
            std::shared_ptr<THugeSlotsMap> BuildHugeSlotsMap() const;

            void FinishRecovery();
            void ShredNotify(const std::vector<ui32>& chunksToShred);
            void ListChunks(const THashSet<TChunkIdx>& chunksOfInterest, THashSet<TChunkIdx>& chunks);

        private:
            void BuildChains(ui32 milestoneBlobInBytes, ui32 overhead);
            void BuildChainsV2(ui32 stepsBetweenPowersOf2);

            void BuildSearchTable();
            inline ui32 SizeToBlocks(ui32 size) const;

            const TString VDiskLogPrefix;
            ui32 ChunkSize = 0;
            ui32 AppendBlockSize = 0;
            ui32 MinHugeBlobInBytes = 0;
            ui32 MaxHugeBlobInBytes = 0;
            ui32 MinHugeBlobInBlocks = 0;
            ui32 MaxHugeBlobInBlocks = 0;

            TDynBitMap DeserializedChains; // a bit mask of chains that were deserialized from the origin stream
            std::vector<TChain> Chains;
            std::vector<ui16> SearchTable; // (NumFullBlocks - 1) -> Chain index
            TControlWrapper ChunksSoftLocking;
        };


        ////////////////////////////////////////////////////////////////////////////
        // THeap
        ////////////////////////////////////////////////////////////////////////////
        class THeap {

            using TChunkID = ui32;
            using TFreeChunks = TSet<TChunkID>;

            static const ui32 Signature;
            static const ui32 SignatureV2;

            const TString VDiskLogPrefix;
            ui32 FreeChunksReservation = 0;
            TFreeChunks FreeChunks;
            TAllChains Chains;
            THashSet<TChunkID> ForbiddenChunks; // chunks that are being shredded right now
            std::deque<TChunkID> ForceFreeChunks;

        public:
            THeap(const TString &vdiskLogPrefix,
                ui32 chunkSize,
                ui32 appendBlockSize,
                // min size of the huge blob
                ui32 minHugeBlobInBytes,
                // fixed point to calculate layout (for backward compatibility)
                ui32 mileStoneBlobInBytes,
                // max size of the blob
                ui32 maxHugeBlobInBytes,
                // difference between buckets is 1/overhead
                ui32 overhead,
                // new bucket scheme
                ui32 stepsBetweenPowersOf2,
                bool useBucketsV2,
                ui32 freeChunksReservation,
                TControlWrapper chunksSoftLocking);

            THeap(const TString& vdiskLogPrefix, const NKikimrVDiskData::THugeKeeperHeap& heap);

            ui32 SlotNumberOfThisSize(ui32 size) const {
                const TChain *chain = Chains.GetChain(size);
                return chain ? chain->SlotsInChunk : 0;
            }

            ui32 SlotSizeOfThisSize(ui32 size) const {
                const TChain *chain = Chains.GetChain(size);
                return chain ? chain->SlotSize : 0;
            }

            // Builds a map of BlobSize -> THugeSlotsMap::TSlotInfo for THugeBlobCtx
            std::shared_ptr<THugeSlotsMap> BuildHugeSlotsMap() const {
                return Chains.BuildHugeSlotsMap();
            }

            //////////////////////////////////////////////////////////////////////////////////////////
            // Main functions
            //////////////////////////////////////////////////////////////////////////////////////////
            THugeSlot ConvertDiskPartToHugeSlot(const TDiskPart &addr) const;
            bool Allocate(ui32 size, THugeSlot *hugeSlot, ui32 *slotSize);
            TFreeRes Free(const TDiskPart &addr);
            void AddChunk(ui32 chunkId);
            ui32 RemoveChunk();
            // make chunk not available for allocations, it is used for heap defragmentation
            bool LockChunkForAllocation(ui32 chunkId, ui32 slotSize);
            THeapStat GetStat() const;
            void ShredNotify(const std::vector<ui32>& chunksToShred);
            void ListChunks(const THashSet<TChunkIdx>& chunksOfInterest, THashSet<TChunkIdx>& chunks);
            THashSet<TChunkIdx> GetForbiddenChunks() const { return ForbiddenChunks; }

            //////////////////////////////////////////////////////////////////////////////////////////
            // RecoveryMode
            //////////////////////////////////////////////////////////////////////////////////////////
            TFreeRes RecoveryModeFree(const TDiskPart &addr);
            void RecoveryModeAllocate(const TDiskPart &addr);
            void RecoveryModeAddChunk(ui32 chunkId);
            void RecoveryModeRemoveChunks(const TVector<ui32> &chunkIds);
            bool ReleaseSlot(THugeSlot slot);
            void OccupySlot(THugeSlot slot, bool inLockedChunks);
            void FinishRecovery();

            //////////////////////////////////////////////////////////////////////////////////////////
            // Serialize/Parse/Check
            //////////////////////////////////////////////////////////////////////////////////////////
            TString Serialize();
            void ParseFromString(const TString &serialized);

            void SaveToProto(NKikimrVDiskData::THugeKeeperHeap& heap) const;
            void LoadFromProto(const NKikimrVDiskData::THugeKeeperHeap& heap);

            static bool CheckEntryPoint(const TString &serialized);
            void GetOwnedChunks(TSet<TChunkIdx>& chunks) const;

            //////////////////////////////////////////////////////////////////////////////////////////
            // Output
            //////////////////////////////////////////////////////////////////////////////////////////
            void RenderHtml(IOutputStream &str) const;
            TString ToString() const;

        private:
            inline ui32 GetChunkIdFromFreeChunks();
            inline void PutChunkIdToFreeChunks(ui32 chunkId);
        };

    } // NHuge

} // NKikimr
