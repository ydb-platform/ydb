#pragma once

#include "defs.h"
#include "blobstorage_hullhugedefs.h"

#include <ydb/core/blobstorage/vdisk/common/vdisk_log.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_hugeblobctx.h>
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


            ////////////////////////////////////////////////////////////////////////
            // TChainLayoutBuilder
            // Builds a map of slots in term of blocks (block=AppendBlockSize).
            ////////////////////////////////////////////////////////////////////////
            class TChainLayoutBuilder {
            public:
                // represents one segment in form of (Left, Right]
                struct TSeg {
                    ui32 Left; // not included
                    ui32 Right; // included
                    bool operator ==(const TSeg &s) const { return Left == s.Left && Right == s.Right; }
                };

                TChainLayoutBuilder(ui32 left, ui32 milestone, ui32 right, ui32 overhead);
                const TVector<TSeg> &GetLayout() const { return Layout; }
                const TSeg &GetMilestoneSegment() const { return Layout.at(MilesoneId); }
                TString ToString(ui32 appendBlockSize = 0) const;
                void Output(IOutputStream &str, ui32 appendBlockSize = 0) const;

            private:
                void Check(ui32 left, ui32 right);
                void BuildDownward(ui32 left, ui32 right, ui32 overhead);
                void BuildUpward(ui32 left, ui32 right, ui32 overhead);

                TVector<TSeg> Layout;
                // An index in Layout vector, where milestone segment starts
                size_t MilesoneId = Max<size_t>();
            };

        } // NPrivate

        ////////////////////////////////////////////////////////////////////////////
        // TChain
        // It manages all slots of some fixed size.
        ////////////////////////////////////////////////////////////////////////////
        class TChain : public TThrRefBase {
            using TChunkID = ui32;
            using TFreeSpace = TMap<TChunkID, TMask>;

            static constexpr ui32 MaxNumberOfSlots = 32768; // it's not a good idea to have more slots than this
            const TString VDiskLogPrefix;
            const ui32 SlotsInChunk;
            const TMask ConstMask; // mask of 'all slots are free'
            TFreeSpace FreeSpace;
            TFreeSpace LockedChunks;
            ui32 AllocatedSlots = 0;
            ui32 FreeSlotsInFreeSpace = 0;

        public:
            static TMask BuildConstMask(const TString &prefix, ui32 slotsInChunk);

        public:
            TChain(const TString &vdiskLogPrefix, const ui32 slotsInChunk)
                : VDiskLogPrefix(vdiskLogPrefix)
                , SlotsInChunk(slotsInChunk)
                , ConstMask(BuildConstMask(vdiskLogPrefix, slotsInChunk))
            {}

            // returns true if allocated, false -- if no free slots
            bool Allocate(NPrivate::TChunkSlot *id);
            // allocate id, but we know that this chain doesn't have free slots, so add a chunk to it
            void Allocate(NPrivate::TChunkSlot *id, TChunkID chunkId);
            // returns freed ChunkID if any
            TFreeRes Free(const NPrivate::TChunkSlot &id);
            bool LockChunkForAllocation(TChunkID chunkId);
            void UnlockChunk(TChunkID chunkId);
            THeapStat GetStat() const;
            // returns true is allocated, false otherwise
            bool RecoveryModeAllocate(const NPrivate::TChunkSlot &id);
            void RecoveryModeAllocate(const NPrivate::TChunkSlot &id, TChunkID chunkId);
            void Save(IOutputStream *s) const;
            void Load(IInputStream *s);
            bool HaveBeenUsed() const;
            TString ToString() const;
            void RenderHtml(IOutputStream &str) const;
            ui32 GetAllocatedSlots() const;
            void GetOwnedChunks(TSet<TChunkIdx>& chunks) const;
        };

        using TChainPtr = TIntrusivePtr<TChain>;

        ////////////////////////////////////////////////////////////////////////////
        // TChainDelegator
        ////////////////////////////////////////////////////////////////////////////
        struct TChainDelegator {
            TString VDiskLogPrefix;
            ui32 Blocks;
            ui32 ShiftInBlocks;
            ui32 SlotsInChunk;
            ui32 SlotSize;
            TChainPtr ChainPtr;

            TChainDelegator(const TString &vdiskLogPrefix,
                ui32 valBlocks,
                ui32 shiftBlocks,
                ui32 chunkSize,
                ui32 appendBlockSize);
            TChainDelegator(TChainDelegator &&) = default;
            TChainDelegator &operator =(TChainDelegator &&) = default;
            TChainDelegator(const TChainDelegator &) = delete;
            TChainDelegator &operator =(const TChainDelegator &) = delete;
            THugeSlot Convert(const NPrivate::TChunkSlot &id) const;
            NPrivate::TChunkSlot Convert(const TDiskPart &addr) const;
            void Save(IOutputStream *s) const;
            void Load(IInputStream *s);
            bool HaveBeenUsed() const;
            TString ToString() const;
            void GetOwnedChunks(TSet<TChunkIdx>& chunks) const;
            void RenderHtml(IOutputStream &str) const;
            void RenderHtmlForUsage(IOutputStream &str) const;
        };


        ////////////////////////////////////////////////////////////////////////////
        // TAllChains
        ////////////////////////////////////////////////////////////////////////////
        class TAllChains {
        public:
            using TAllChainDelegators = TVector<TChainDelegator>;
            using TSearchTable = TVector<TChainDelegator*>;

            TAllChains(const TString &vdiskLogPrefix,
                ui32 chunkSize,
                ui32 appendBlockSize,
                ui32 minHugeBlobInBytes,
                ui32 oldMinHugeBlobSizeInBytes,
                ui32 milestoneBlobInBytes,
                ui32 maxBlobInBytes,
                ui32 overhead);
            // return a pointer to corresponding chain delegator by object byte size
            TChainDelegator *GetChain(ui32 size);
            const TChainDelegator *GetChain(ui32 size) const;
            THeapStat GetStat() const;
            void PrintOutChains(IOutputStream &str) const;
            void PrintOutSearchTable(IOutputStream &str) const;
            void Save(IOutputStream *s) const;
            void Load(IInputStream *s);
            void GetOwnedChunks(TSet<TChunkIdx>& chunks) const;
            TString ToString() const;
            void RenderHtml(IOutputStream &str) const;
            void RenderHtmlForUsage(IOutputStream &str) const;
            // for testing purposes
            TVector<NPrivate::TChainLayoutBuilder::TSeg> GetLayout() const;
            // returns (ChainsSize, SearchTableSize)
            std::pair<ui32, ui32> GetTablesSize() const {
                return std::pair<ui32, ui32>(ChainDelegators.size(), SearchTable.size());
            }
            // Builds a map of BlobSize -> THugeSlotsMap::TSlotInfo for THugeBlobCtx
            std::shared_ptr<THugeSlotsMap> BuildHugeSlotsMap() const;

        private:

            TAllChainDelegators BuildChains(ui32 minHugeBlobInBytes) const;
            void BuildSearchTable();
            void BuildLayout();
            inline ui32 SizeToBlocks(ui32 size) const;
            inline ui32 GetEndBlocks() const;

            enum class EStartMode {
                Empty = 1,
                Loaded = 2,
                Migrated = 3,
            };

            const TString VDiskLogPrefix;
            const ui32 ChunkSize;
            const ui32 AppendBlockSize;
            const ui32 MinHugeBlobInBytes;
            const ui32 OldMinHugeBlobSizeInBytes;
            const ui32 MilestoneBlobInBytes;
            const ui32 MaxBlobInBytes;
            const ui32 Overhead;
            EStartMode StartMode = EStartMode::Empty;
            ui32 FirstLoadedSlotSize = 0;
            TAllChainDelegators ChainDelegators;
            TSearchTable SearchTable;
        };


        ////////////////////////////////////////////////////////////////////////////
        // THeap
        ////////////////////////////////////////////////////////////////////////////
        class THeap {

            using TChunkID = ui32;
            using TFreeChunks = TSet<TChunkID>;

            static const ui32 Signature;
            const TString VDiskLogPrefix;
            const ui32 FreeChunksReservation;
            TFreeChunks FreeChunks;
            TAllChains Chains;

        public:
            THeap(const TString &vdiskLogPrefix,
                ui32 chunkSize,
                ui32 appendBlockSize,
                // min size of the huge blob
                ui32 minHugeBlobInBytes,
                ui32 oldMinHugeBlobSizeInBytes,
                // fixed point to calculate layout (for backward compatibility)
                ui32 mileStoneBlobInBytes,
                // max size of the blob
                ui32 maxBlobInBytes,
                // difference between buckets is 1/overhead
                ui32 overhead,
                ui32 freeChunksReservation);


            ui32 SlotNumberOfThisSize(ui32 size) const {
                const TChainDelegator *chainD = Chains.GetChain(size);
                return chainD ? chainD->SlotsInChunk : 0;
            }

            ui32 SlotSizeOfThisSize(ui32 size) const {
                const TChainDelegator *chainD = Chains.GetChain(size);
                return chainD ? chainD->SlotSize : 0;
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
            void UnlockChunk(ui32 chunkId, ui32 slotSize);
            THeapStat GetStat() const;

            //////////////////////////////////////////////////////////////////////////////////////////
            // RecoveryMode
            //////////////////////////////////////////////////////////////////////////////////////////
            TFreeRes RecoveryModeFree(const TDiskPart &addr);
            void RecoveryModeAllocate(const TDiskPart &addr);
            void RecoveryModeAddChunk(ui32 chunkId);
            void RecoveryModeRemoveChunks(const TVector<ui32> &chunkIds);

            //////////////////////////////////////////////////////////////////////////////////////////
            // Serialize/Parse/Check
            //////////////////////////////////////////////////////////////////////////////////////////
            TString Serialize();
            void ParseFromString(const TString &serialized);
            static bool CheckEntryPoint(const TString &serialized);
            void GetOwnedChunks(TSet<TChunkIdx>& chunks) const;

            //////////////////////////////////////////////////////////////////////////////////////////
            // Output
            //////////////////////////////////////////////////////////////////////////////////////////
            void RenderHtml(IOutputStream &str) const;
            TString ToString() const;

            void PrintOutSearchTable(IOutputStream &str) {
                Chains.PrintOutSearchTable(str);
            }

        private:
            inline ui32 GetChunkIdFromFreeChunks();
            inline void PutChunkIdToFreeChunks(ui32 chunkId);
        };

    } // NHuge

} // NKikimr
