#include "blobstorage_hullhugeheap.h"

#include <library/cpp/monlib/service/pages/templates.h>

#include <ranges>

namespace NKikimr {
    namespace NHuge {

        namespace NPrivate {

            ////////////////////////////////////////////////////////////////////////
            // TChainLayoutBuilder
            // Builds a map of slots in term of blocks (block=AppendBlockSize).
            ////////////////////////////////////////////////////////////////////////
            TChainLayoutBuilder::TChainLayoutBuilder(const TString& prefix, ui32 left, ui32 milestone, ui32 right, ui32 overhead) {
                BuildDownward(left, milestone, overhead);
                BuildUpward(milestone, right, overhead);
                Check(prefix, left, right);
            }

            TString TChainLayoutBuilder::ToString(ui32 appendBlockSize) const {
               TStringStream str;
               Output(str, appendBlockSize);
               return str.Str();
            }

            void TChainLayoutBuilder::Output(IOutputStream &str, ui32 appendBlockSize) const {
                str << "CHAIN TABLE (MilestoneId=" << MilestoneId << " rows=" << Layout.size() << "):\n";
                for (const auto &x : Layout) {
                    str << "Blocks# (" << x.Left << ", " << x.Right << "]";
                    if (appendBlockSize) {
                        str << " Bytes# (" << x.Left * appendBlockSize << ", " << x.Right * appendBlockSize << "]\n";
                    } else {
                        str << "\n";
                    }
                }
            }

            void TChainLayoutBuilder::Check(const TString& prefix, ui32 left, ui32 right) {
                // check integrity of the built layout
                Y_VERIFY_S(Layout.size() > 1, prefix);
                Y_VERIFY_S(Layout.begin()->Left <= left, prefix);
                Y_VERIFY_S((Layout.end() - 1)->Right >= right, prefix);
                for (size_t i = 1, s = Layout.size(); i < s; ++i) {
                    Y_VERIFY_S(Layout[i - 1].Right == Layout[i].Left, prefix);
                }
            }

            void TChainLayoutBuilder::BuildDownward(ui32 left, ui32 right, ui32 overhead) {
                ui32 cur = right;
                while (cur > left) {
                    // Formula to build (prev, cur]:
                    // prev + prev / overhead = cur
                    // (overhead + 1) * prev = overhead * cur
                    // prev = overhead * cur / (overhead + 1)
                    ui32 prev = overhead * cur / (overhead + 1);
                    if (prev == cur) {
                        prev = cur - 1;
                    }

                    Layout.push_back({prev, cur});
                    cur = prev;
                }
                std::reverse(Layout.begin(), Layout.end());
            }

            void TChainLayoutBuilder::BuildUpward(ui32 left, ui32 right, ui32 overhead) {
                MilestoneId = Layout.size();

                ui32 valBlocks = left;
                ui32 shiftBlocks = 0;
                while (valBlocks < right) {
                    shiftBlocks = valBlocks / overhead;
                    if (shiftBlocks == 0)
                        shiftBlocks = 1;

                    Layout.push_back({valBlocks, valBlocks + shiftBlocks});
                    valBlocks += shiftBlocks;
                }
            }

        } // NPrivate

        ////////////////////////////////////////////////////////////////////////////
        // TChain
        ////////////////////////////////////////////////////////////////////////////
        THugeSlot TChain::Convert(const NPrivate::TChunkSlot& id) const {
            return THugeSlot(id.GetChunkId(), id.GetSlotId() * SlotSize, SlotSize);
        }

        NPrivate::TChunkSlot TChain::Convert(const TDiskPart& addr) const {
            ui32 slotId = addr.Offset / SlotSize;
            Y_VERIFY_S(slotId * SlotSize == addr.Offset, VDiskLogPrefix << "slotId# " << slotId
                << " addr# " << addr.ToString() << " State# " << ToString());
            return NPrivate::TChunkSlot(addr.ChunkIdx, slotId);
        }

        NPrivate::TChunkSlot TChain::Convert(const THugeSlot& slot) const {
            return Convert(slot.GetDiskPart());
        }

        TMask TChain::BuildConstMask(const TString &prefix, ui32 slotsInChunk) {
            Y_VERIFY_S(1 < slotsInChunk && slotsInChunk <= MaxNumberOfSlots,
                    prefix << "It's not a good idea to have so many slots in chunk;"
                    << " slotsInChunk# " << slotsInChunk);
            TMask mask;
            mask.Reserve(slotsInChunk);
            mask.Set(0, slotsInChunk);
            return mask;
        }

        // returns true if allocated, false -- if no free slots
        bool TChain::Allocate(NPrivate::TChunkSlot *id) {
            if (FreeSpace.empty()) {
                return false;
            }

            TFreeSpace::iterator it = FreeSpace.begin();
            TFreeSpaceItem& item = it->second;

            Y_VERIFY_S(item.NumFreeSlots, VDiskLogPrefix << "TChain::Allocate: id# " << id->ToString()
                << " State# " << ToString());

            *id = NPrivate::TChunkSlot(it->first, item.FreeSlots.FirstNonZeroBit());

            if (item.FreeSlots.Reset(id->GetSlotId()); !--item.NumFreeSlots) {
                FreeSpace.erase(it);
            }

            ++AllocatedSlots;
            --FreeSlotsInFreeSpace;
            return true;
        }

        // allocate id, but we know that this chain doesn't have free slots, so add a chunk to it
        void TChain::Allocate(NPrivate::TChunkSlot *id, TChunkID chunkId) {
            Y_VERIFY_S(FreeSpace.empty(), VDiskLogPrefix << " State# " << ToString());
            FreeSpace.emplace(chunkId, TFreeSpaceItem{ConstMask, SlotsInChunk});
            FreeSlotsInFreeSpace += SlotsInChunk;
            const bool res = Allocate(id);
            Y_VERIFY_S(res, VDiskLogPrefix << "TChain::Allocate: id# " << id->ToString() << " chunkId# "
                << chunkId << " State# " << ToString());
        }

        TFreeRes TChain::Free(const NPrivate::TChunkSlot &id) {
            Y_VERIFY_S(id.GetSlotId() < SlotsInChunk && AllocatedSlots > 0, VDiskLogPrefix
                    << " id# " << id.ToString() << " SlotsInChunk# " << SlotsInChunk
                    << " AllocatedSlots# " << AllocatedSlots << " State# " << ToString());

            --AllocatedSlots;
            ++FreeSlotsInFreeSpace;

            ui32 chunkId = id.GetChunkId();
            ui32 slotId = id.GetSlotId();
            Y_VERIFY_S(chunkId, VDiskLogPrefix << "chunkId# " << chunkId);
            TFreeSpace::iterator it;
            TFreeSpace *container = &LockedChunks;

            if (it = LockedChunks.find(chunkId); it == LockedChunks.end()) {
                bool inserted;
                std::tie(it, inserted) = FreeSpace.try_emplace(chunkId);
                if (inserted) {
                    it->second.FreeSlots.Reset(0, SlotsInChunk); // mark all slots as used ones
                }
                container = &FreeSpace;
            }

            TFreeSpaceItem& item = it->second;

            Y_VERIFY_S(!item.FreeSlots.Get(slotId), VDiskLogPrefix << "TChain::Free: containerName# " <<
                (container == &FreeSpace ? "FreeSpace" : "LockedChunks") << " id# " << id.ToString()
                << " State# " << ToString());

            if (item.FreeSlots.Set(slotId); ++item.NumFreeSlots == SlotsInChunk) {
                Y_VERIFY_DEBUG_S(item.FreeSlots == ConstMask, VDiskLogPrefix);
                container->erase(it);
                FreeSlotsInFreeSpace -= SlotsInChunk;
                return {chunkId, container == &LockedChunks};
            }

            Y_VERIFY_DEBUG_S(item.FreeSlots != ConstMask, VDiskLogPrefix);
            return {0u, false}; // no chunk freed
        }

        bool TChain::LockChunkForAllocation(TChunkID chunkId) {
            if (auto nh = FreeSpace.extract(chunkId)) {
                LockedChunks.insert(std::move(nh));
                return true;
            } else {
                return LockedChunks.contains(chunkId);
            }
        }

        THeapStat TChain::GetStat() const {
            // how many chunks are required to represent slotsNum
            auto slotsToChunks = [] (ui32 slotsNum, ui32 slotsInChunk) {
                return (slotsNum + slotsInChunk - 1) / slotsInChunk;
            };

            ui32 usedChunksInFreeSpace = FreeSpace.size() + LockedChunks.size();
            ui32 usedSlotsInFreeSpace = usedChunksInFreeSpace * SlotsInChunk - FreeSlotsInFreeSpace;
            ui32 chunksToStoreDefragmentedSlots = slotsToChunks(usedSlotsInFreeSpace, SlotsInChunk);
            Y_VERIFY_S(usedChunksInFreeSpace - chunksToStoreDefragmentedSlots >= 0, VDiskLogPrefix);
            ui32 canBeFreedChunks = usedChunksInFreeSpace - chunksToStoreDefragmentedSlots;
            ui32 fullyFilledChunks = slotsToChunks(AllocatedSlots - usedSlotsInFreeSpace, SlotsInChunk);
            ui32 currentlyUsedChunks = usedChunksInFreeSpace + fullyFilledChunks;

            std::vector<ui32> lockedChunks;
            for (auto& x : LockedChunks) {
                lockedChunks.push_back(x.first);
            }
            return THeapStat(currentlyUsedChunks, canBeFreedChunks, std::move(lockedChunks));
        }

        bool TChain::RecoveryModeAllocate(const NPrivate::TChunkSlot &id) {
            ui32 chunkId = id.GetChunkId();
            ui32 slotId = id.GetSlotId();

            TFreeSpace *map = &FreeSpace;
            TFreeSpace::iterator it = map->find(chunkId);
            if (it == map->end()) {
                map = &LockedChunks;
                it = map->find(chunkId);
            }
            if (it != map->end()) {
                TFreeSpaceItem& item = it->second;
                Y_VERIFY_S(item.FreeSlots.Get(slotId), VDiskLogPrefix << "RecoveryModeAllocate:"
                        << " id# " << id.ToString() << " State# " << ToString());
                if (item.FreeSlots.Reset(slotId); !--item.NumFreeSlots) {
                    map->erase(it);
                }

                --FreeSlotsInFreeSpace;
                ++AllocatedSlots;
                return true;
            } else {
                return false;
            }
        }

        void TChain::RecoveryModeAllocate(const NPrivate::TChunkSlot &id, TChunkID chunkId, bool inLockedChunks) {
            Y_VERIFY_S(id.GetChunkId() == chunkId && !FreeSpace.contains(chunkId) && !LockedChunks.contains(chunkId),
                    VDiskLogPrefix << " id# " << id.ToString() << " chunkId# " << chunkId << " State# " << ToString());

            (inLockedChunks ? LockedChunks : FreeSpace).emplace(chunkId, TFreeSpaceItem{ConstMask, SlotsInChunk});
            FreeSlotsInFreeSpace += SlotsInChunk;
            bool res = RecoveryModeAllocate(id);

            Y_VERIFY_S(res, VDiskLogPrefix << "RecoveryModeAllocate:"
                    << " id# " << id.ToString() << " chunkId# " << chunkId << " State# " << ToString());
        }

        void TChain::Save(IOutputStream *s) const {
            ::Save(s, SlotsInChunk);
            ::Save(s, AllocatedSlots);
            ::SaveSize(s, FreeSpace.size() + LockedChunks.size());
            ForEachFreeSpaceChunk([s](const auto& x) {
                const auto& [chunkId, item] = x;
                ::Save(s, chunkId);
                ::Save(s, item.FreeSlots);
            });
        }

        TChain TChain::Load(IInputStream *s, TString vdiskLogPrefix, ui32 appendBlockSize, ui32 blocksInChunk) {
            ui32 slotsInChunk;
            ::Load(s, slotsInChunk);

            // calculate optimal slot size for this number of slots per chunk; it may differ from builder's one,
            // this will be fixed in caller function
            const ui32 slotSizeInBlocks = blocksInChunk / slotsInChunk;
            Y_VERIFY_S(slotSizeInBlocks, vdiskLogPrefix);
            ui32 slotSize = slotSizeInBlocks * appendBlockSize;

            TChain res{
                std::move(vdiskLogPrefix),
                slotsInChunk,
                slotSize, // in bytes
            };

            ::Load(s, res.AllocatedSlots);

            for (size_t numItems = ::LoadSize(s); numItems; --numItems) {
                TChunkID chunkId;
                TFreeSpaceItem item;
                ::Load(s, chunkId);
                ::Load(s, item.FreeSlots);
                item.NumFreeSlots = item.FreeSlots.Count();
                res.FreeSlotsInFreeSpace += item.NumFreeSlots;
                res.FreeSpace.emplace(chunkId, std::move(item));
            }

            return res;
        }

        bool TChain::HaveBeenUsed() const {
            return AllocatedSlots; // chain is considered to be used if it contains any allocated slots
        }

        TString TChain::ToString() const {
            TStringStream str;

            str << "{" << SlotSize << '/' << SlotsInChunk << " AllocatedSlots# " << AllocatedSlots << " Free#";

            bool any = false;
            ForEachFreeSpaceChunk([&](const auto& value) {
                const auto& [chunk, item] = value;
                str << " {" << chunk;
                ui32 begin;
                ui32 prev = Max<ui32>();
                Y_FOR_EACH_BIT(i, item.FreeSlots) {
                    if (prev == Max<ui32>()) {
                        begin = i;
                    } else if (i != prev + 1) {
                        str << ' ' << begin;
                        if (begin != prev) {
                            str << '-' << prev;
                        }
                        begin = prev;
                    }
                    prev = i;
                }
                str << ' ' << begin;
                if (begin != prev) {
                    str << '-' << prev;
                }
                str << '}';

                any = true;
            });

            if (!any) {
                str << " none";
            }

            str << "}";
            return str.Str();
        }

        void TChain::RenderHtml(IOutputStream &str) const {
            HTML(str) {
                TABLER() {
                    TABLED() {
                        str << SlotSize << "/" << SlotsInChunk;
                    }
                    TABLED() {
                        ForEachFreeSpaceChunk([&](const auto& value) {
                            const auto& [chunk, item] = value;
                            str << " [" << chunk << " " << item.NumFreeSlots << "]";
                        });
                    }
                }
            }
        }

        void TChain::RenderHtmlForUsage(IOutputStream &str) const {
            HTML(str) {
                TABLER() {
                    TABLED() { str << SlotSize; }
                    TABLED() { str << SlotsInChunk; }
                    TABLED() { str << AllocatedSlots; }
                }
            }
        }

        void TChain::GetOwnedChunks(TSet<TChunkIdx>& chunks) const {
            auto traverse = [&chunks] (const TFreeSpace &c) {
                for (const auto& kv : c) {
                    chunks.insert(kv.first);
                }
            };
            traverse(FreeSpace);
            traverse(LockedChunks);
        }

        void TChain::ShredNotify(const std::vector<ui32>& chunksToShred) {
            auto chunksIt = chunksToShred.begin();
            for (auto it = FreeSpace.begin(); it != FreeSpace.end(); ) {
                const TChunkIdx chunkId = it->first;
                while (chunksIt != chunksToShred.end() && *chunksIt < chunkId) {
                    ++chunksIt;
                }
                if (chunksIt != chunksToShred.end() && *chunksIt == chunkId) {
                    LockedChunks.insert(FreeSpace.extract(it++));
                } else {
                    ++it;
                }
            }
        }

        void TChain::ListChunks(const THashSet<TChunkIdx>& chunksOfInterest, THashSet<TChunkIdx>& chunks) {
            for (auto& map : {FreeSpace, LockedChunks}) {
                for (const auto& [chunkIdx, freeSpace] : map) {
                    if (chunksOfInterest.contains(chunkIdx)) {
                        chunks.insert(chunkIdx);
                    }
                }
            }
        }

        ////////////////////////////////////////////////////////////////////////////
        // TAllChains
        ////////////////////////////////////////////////////////////////////////////
        TAllChains::TAllChains(
                const TString &vdiskLogPrefix,
                ui32 chunkSize,
                ui32 appendBlockSize,
                ui32 minHugeBlobInBytes,
                ui32 milestoneBlobInBytes,
                ui32 maxBlobInBytes,
                ui32 overhead)
            : VDiskLogPrefix(vdiskLogPrefix)
            , ChunkSize(chunkSize)
            , AppendBlockSize(appendBlockSize)
            , MinHugeBlobInBytes(minHugeBlobInBytes)
            , MilestoneBlobInBytes(milestoneBlobInBytes)
            , Overhead(overhead)
            , MinHugeBlobInBlocks(MinHugeBlobInBytes / AppendBlockSize)
            , MaxHugeBlobInBlocks(SizeToBlocks(maxBlobInBytes))
        {
            Y_VERIFY_S(MinHugeBlobInBytes &&
                    MinHugeBlobInBytes <= MilestoneBlobInBytes &&
                    MilestoneBlobInBytes < maxBlobInBytes,
                            VDiskLogPrefix << "INVALID CONFIGURATION! (SETTINGS ARE:"
                            << " MaxBlobInBytes# " << maxBlobInBytes << " MinHugeBlobInBytes# " << MinHugeBlobInBytes
                            << " MilestoneBlobInBytes# " << MilestoneBlobInBytes << " ChunkSize# " << ChunkSize
                            << " AppendBlockSize# " << AppendBlockSize << ")");

            BuildChains();
        }

        TChain *TAllChains::GetChain(ui32 size) {
            if (size < MinHugeBlobInBytes || MaxHugeBlobInBlocks * AppendBlockSize < size) {
                return nullptr;
            }
            const size_t index = SizeToBlocks(size) - MinHugeBlobInBlocks;
            Y_VERIFY_DEBUG_S(index < SearchTable.size(), VDiskLogPrefix);
            const size_t chainIndex = SearchTable[index];
            Y_VERIFY_DEBUG_S(chainIndex < Chains.size(), VDiskLogPrefix);
            return &Chains[chainIndex];
        }

        const TChain *TAllChains::GetChain(ui32 size) const {
            if (size < MinHugeBlobInBytes || MaxHugeBlobInBlocks * AppendBlockSize < size) {
                return nullptr;
            }
            Y_VERIFY_S(MinHugeBlobInBytes <= size, VDiskLogPrefix);
            const size_t index = SizeToBlocks(size) - MinHugeBlobInBlocks;
            Y_VERIFY_DEBUG_S(index < SearchTable.size(), VDiskLogPrefix);
            const size_t chainIndex = SearchTable[index];
            Y_VERIFY_DEBUG_S(chainIndex < Chains.size(), VDiskLogPrefix);
            return &Chains[chainIndex];
        }

        THeapStat TAllChains::GetStat() const {
            THeapStat stat;
            for (const auto& chain : Chains) {
                stat += chain.GetStat();
            }
            return stat;
        }

        void TAllChains::Save(IOutputStream *s) const {
            // check if we can write compatible entrypoint (with exactly the same set of chains)
            bool writeCompatible = true;
            ui32 numChains = 0;
            if (DeserializedChains.Empty()) { // if this was initially empty heap, write it fully
                writeCompatible = false;
            } else {
                for (size_t i = 0; i < Chains.size(); ++i) {
                    if (DeserializedChains[i]) {
                        ++numChains;
                    } else if (Chains[i].HaveBeenUsed()) {
                        writeCompatible = false;
                        break;
                    }
                }
            }

            if (!writeCompatible) { // we can't, so we serialize all our chains anyway
                numChains = Chains.size();
            }
            ::Save(s, numChains);

            // serialize selected chains
            for (size_t i = 0; i < Chains.size(); ++i) {
                if (!writeCompatible || DeserializedChains[i]) {
                    ::Save(s, Chains[i]);
                }
            }
        }

        void TAllChains::Load(IInputStream *s) {
            std::vector<TChain> newChains;
            newChains.reserve(Chains.size());

            Y_VERIFY_DEBUG_S(ChunkSize % AppendBlockSize == 0, VDiskLogPrefix);
            Y_VERIFY_DEBUG_S(AppendBlockSize <= ChunkSize, VDiskLogPrefix);
            const ui32 blocksInChunk = ChunkSize / AppendBlockSize;

            auto chainsIt = Chains.begin();
            const auto chainsEnd = Chains.end();

            ui32 prevSlotSize = 0;
            ui32 numChains;
            for (::Load(s, numChains); numChains; --numChains) {
                auto chain = TChain::Load(s, VDiskLogPrefix, AppendBlockSize, blocksInChunk);

                // merge new item with originating ones from TChainLayoutBuilder -- we may have not every one of them
                // serialized
                for (; chainsIt != chainsEnd && chain.SlotsInChunk < chainsIt->SlotsInChunk; ++chainsIt) {
                    newChains.push_back(std::move(*chainsIt));
                }
                if (chainsIt != chainsEnd && chainsIt->SlotsInChunk == chain.SlotsInChunk) {
                    // reuse slot size from the builder's one to retain compatibility
                    chain.SlotSize = chainsIt->SlotSize;
                    ++chainsIt;
                }

                // assert SlotSize-s are coming in strictly increasing order
                Y_VERIFY_S(std::exchange(prevSlotSize, chain.SlotSize) < chain.SlotSize, VDiskLogPrefix);

                DeserializedChains.Set(newChains.size());
                newChains.push_back(std::move(chain));
            }

            std::ranges::move(chainsIt, chainsEnd, std::back_inserter(newChains));

            Chains = std::move(newChains);
        }

        void TAllChains::GetOwnedChunks(TSet<TChunkIdx>& chunks) const {
            for (const TChain& chain : Chains) {
                chain.GetOwnedChunks(chunks);
            }
        }

        TString TAllChains::ToString() const {
            TStringStream str;
            str << "{ChunkSize# " << ChunkSize
                << " AppendBlockSize# " << AppendBlockSize
                << " MinHugeBlobInBytes# " << MinHugeBlobInBytes
                << " MinHugeBlobInBlocks# " << MinHugeBlobInBlocks
                << " MaxHugeBlobInBlocks# " << MaxHugeBlobInBlocks;
            for (const auto& chain : Chains) {
                str << " {CHAIN " << chain.ToString() << "}";
            }
            str << "}";
            return str.Str();
        }

        void TAllChains::RenderHtml(IOutputStream &str) const {
            HTML(str) {
                TABLE_CLASS ("table table-condensed") {
                    TABLEHEAD() {
                        TABLER() {
                            TABLEH() {str << "Chain";}
                            TABLEH() {str << "Reserved: [ChunkIdx, FreeSlotsInChunk]";}
                        }
                    }
                    TABLEBODY() {
                        for (const auto& chain : Chains) {
                            chain.RenderHtml(str);
                        }
                    }
                }
            }
        }

        void TAllChains::RenderHtmlForUsage(IOutputStream &str) const {
            HTML(str) {
                TABLE_CLASS ("table table-condensed") {
                    TABLEHEAD() {
                        TABLER() {
                            TABLEH() {str << "Slot Size";}
                            TABLEH() {str << "Slots in Chunk";}
                            TABLEH() {str << "Allocated";}
                        }
                    }
                    TABLEBODY() {
                        for (const auto& chain : Chains) {
                            chain.RenderHtmlForUsage(str);
                        }
                    }
                }
            }
        }

        TVector<NPrivate::TChainLayoutBuilder::TSeg> TAllChains::GetLayout() const {
            TVector<NPrivate::TChainLayoutBuilder::TSeg> res;
            res.reserve(Chains.size());
            ui32 prevSlotSizeInBlocks = MinHugeBlobInBlocks;
            for (const auto& chain : Chains) {
                const ui32 slotSizeInBlocks = chain.SlotSize / AppendBlockSize;
                res.push_back({
                    prevSlotSizeInBlocks,
                    slotSizeInBlocks - prevSlotSizeInBlocks,
                });
                prevSlotSizeInBlocks = slotSizeInBlocks;
            }
            return res;
        }

        std::shared_ptr<THugeSlotsMap> TAllChains::BuildHugeSlotsMap() const {
            THugeSlotsMap::TAllSlotsInfo allSlotsInfo;
            for (const auto& chain : Chains) {
                allSlotsInfo.emplace_back(chain.SlotSize, chain.SlotsInChunk);
            }
            return std::make_shared<THugeSlotsMap>(AppendBlockSize, MinHugeBlobInBlocks, std::move(allSlotsInfo),
                THugeSlotsMap::TSearchTable(SearchTable));
        }

        ////////////////////////////////////////////////////////////////////////////
        // TAllChains: Private
        ////////////////////////////////////////////////////////////////////////////
        void TAllChains::BuildChains() {
            const ui32 startBlocks = MinHugeBlobInBlocks;
            const ui32 milestoneBlocks = MilestoneBlobInBytes / AppendBlockSize;
            const ui32 endBlocks = MaxHugeBlobInBlocks;

            NPrivate::TChainLayoutBuilder builder(VDiskLogPrefix, startBlocks, milestoneBlocks, endBlocks, Overhead);
            const ui32 blocksInChunk = ChunkSize / AppendBlockSize;

            for (auto x : builder.GetLayout()) {
                const ui32 slotSizeInBlocks = x.Right;
                const ui32 slotSize = slotSizeInBlocks * AppendBlockSize;
                const ui32 slotsInChunk = blocksInChunk / slotSizeInBlocks;
                Chains.emplace_back(VDiskLogPrefix, slotsInChunk, slotSize);
            }

            Y_VERIFY_S(!Chains.empty(), VDiskLogPrefix);
        }

        void TAllChains::BuildSearchTable() {
            Y_VERIFY_S(SearchTable.empty(), VDiskLogPrefix);
            Y_VERIFY_S(!Chains.empty(), VDiskLogPrefix);

            const ui32 startBlocks = MinHugeBlobInBlocks;
            const ui32 minSize = startBlocks * AppendBlockSize;
            const ui32 endBlocks = MaxHugeBlobInBlocks; // maximum possible number of blocks per huge blob
            auto it = Chains.begin();
            ui16 index = 0;

            SearchTable.reserve(endBlocks - startBlocks + 1);
            for (ui32 i = startBlocks, size = minSize; i <= endBlocks; ++i, size += AppendBlockSize) {
                if (it->SlotSize < size) { // size doesn't fit in current chain, but it must fit into next one
                    ++it;
                    Y_VERIFY_S(it != Chains.end(), VDiskLogPrefix);
                    Y_VERIFY_S(size <= it->SlotSize, VDiskLogPrefix);
                    Y_VERIFY_S(index != Max<ui16>(), VDiskLogPrefix);
                    ++index;
                }
                SearchTable.push_back(index);
            }
        }

        ui32 TAllChains::SizeToBlocks(ui32 size) const {
            return (size + AppendBlockSize - 1) / AppendBlockSize;
        }

        void TAllChains::FinishRecovery() {
            ui32 prevSlotSize = 0;
            for (const TChain& chain : Chains) {
                Y_VERIFY_S(prevSlotSize < chain.SlotSize, VDiskLogPrefix);
                prevSlotSize = chain.SlotSize;
            }
            BuildSearchTable();
        }

        void TAllChains::ShredNotify(const std::vector<ui32>& chunksToShred) {
            for (TChain& chain : Chains) {
                chain.ShredNotify(chunksToShred);
            }
        }

        void TAllChains::ListChunks(const THashSet<TChunkIdx>& chunksOfInterest, THashSet<TChunkIdx>& chunks) {
            for (TChain& chain : Chains) {
                chain.ListChunks(chunksOfInterest, chunks);
            }
        }

        ////////////////////////////////////////////////////////////////////////////
        // THeap
        ////////////////////////////////////////////////////////////////////////////
        const ui32 THeap::Signature = 0x2FA8C240;

        THeap::THeap(const TString &vdiskLogPrefix,
                ui32 chunkSize,
                ui32 appendBlockSize,
                ui32 minHugeBlobInBytes,
                ui32 mileStoneBlobInBytes,
                ui32 maxBlobInBytes,
                ui32 overhead,
                ui32 freeChunksReservation)
            : VDiskLogPrefix(vdiskLogPrefix)
            , FreeChunksReservation(freeChunksReservation)
            , FreeChunks()
            , Chains(vdiskLogPrefix, chunkSize, appendBlockSize, minHugeBlobInBytes, mileStoneBlobInBytes,
                maxBlobInBytes, overhead)
        {}

        //////////////////////////////////////////////////////////////////////////////////////////
        // THeap: main functions
        //////////////////////////////////////////////////////////////////////////////////////////
        THugeSlot THeap::ConvertDiskPartToHugeSlot(const TDiskPart& addr) const {
            const TChain *chain = Chains.GetChain(addr.Size);
            Y_VERIFY_S(chain, VDiskLogPrefix);
            return chain->Convert(chain->Convert(addr));
        }

        bool THeap::Allocate(ui32 size, THugeSlot *hugeSlot, ui32 *slotSize) {
            TChain *chain = Chains.GetChain(size);
            Y_VERIFY_S(chain, VDiskLogPrefix << "size# " << size << " Heap# " << ToString());
            *slotSize = chain->SlotSize;

            NPrivate::TChunkSlot id;
            if (!chain->Allocate(&id)) { // no available slot in free space of the chain
                if (FreeChunks.empty()) { // no free chunks left for reuse -- request a new chunk
                    return false;
                }
                chain->Allocate(&id, GetChunkIdFromFreeChunks()); // reuse free chunk for this chain
            }
            *hugeSlot = chain->Convert(id);
            return true;
        }

        TFreeRes THeap::Free(const TDiskPart &addr) {
            ui32 size = addr.Size;
            TChain *chain = Chains.GetChain(size);
            Y_VERIFY_S(chain, VDiskLogPrefix);

            TFreeRes res = chain->Free(chain->Convert(addr));
            if (res.ChunkId) {
                PutChunkIdToFreeChunks(res.ChunkId);
            }
            return res;
        }

        void THeap::AddChunk(ui32 chunkId) {
            ForbiddenChunks.erase(chunkId); // may be this was forbidden chunk from other subsystem, but now it is clean
            PutChunkIdToFreeChunks(chunkId);
        }

        ui32 THeap::RemoveChunk() {
            if (!ForceFreeChunks.empty()) {
                const ui32 chunkId = ForceFreeChunks.front();
                ForceFreeChunks.pop_front();
                return chunkId;
            } else if (FreeChunks.size() > FreeChunksReservation) {
                ui32 chunkId = GetChunkIdFromFreeChunks();
                Y_VERIFY_S(chunkId, VDiskLogPrefix << "State# " << ToString());
                return chunkId;
            } else {
                return 0;
            }
        }

        bool THeap::LockChunkForAllocation(ui32 chunkId, ui32 slotSize) {
            TChain *chain = Chains.GetChain(slotSize);
            Y_VERIFY_S(chain, VDiskLogPrefix);
            return chain->LockChunkForAllocation(chunkId);
        }

        void THeap::FinishRecovery() {
            Chains.FinishRecovery();
        }

        THeapStat THeap::GetStat() const {
            return Chains.GetStat();
        }

        void THeap::ShredNotify(const std::vector<ui32>& chunksToShred) {
            Chains.ShredNotify(chunksToShred);

            ForbiddenChunks.insert(chunksToShred.begin(), chunksToShred.end());

            for (auto it = FreeChunks.begin(); it != FreeChunks.end(); ) {
                if (ForbiddenChunks.erase(*it)) {
                    ForceFreeChunks.push_back(*it);
                    FreeChunks.erase(it++);
                } else {
                    ++it;
                }
            }
        }

        void THeap::ListChunks(const THashSet<TChunkIdx>& chunksOfInterest, THashSet<TChunkIdx>& chunks) {
            for (const TChunkIdx chunkIdx : FreeChunks) {
                if (chunksOfInterest.contains(chunkIdx)) {
                    chunks.insert(chunkIdx);
                }
            }
            Chains.ListChunks(chunksOfInterest, chunks);
        }

        //////////////////////////////////////////////////////////////////////////////////////////
        // THeap: RecoveryMode
        //////////////////////////////////////////////////////////////////////////////////////////
        TFreeRes THeap::RecoveryModeFree(const TDiskPart &addr) {
            return Free(addr);
        }

        void THeap::RecoveryModeAllocate(const TDiskPart &addr) {
            ui32 size = addr.Size;
            TChain *chain = Chains.GetChain(size);
            Y_VERIFY_S(chain, VDiskLogPrefix << "State# " << ToString());

            NPrivate::TChunkSlot id(chain->Convert(addr));
            bool allocated = chain->RecoveryModeAllocate(id);
            if (allocated) {
                return;
            } else {
                ui32 chunkId = addr.ChunkIdx;
                TFreeChunks::iterator it = FreeChunks.find(chunkId);
                Y_VERIFY_S(it != FreeChunks.end(), VDiskLogPrefix << "addr# " << addr.ToString() << " State# " << ToString());
                FreeChunks.erase(it);
                chain->RecoveryModeAllocate(id, chunkId, false);
            }
        }

        void THeap::RecoveryModeAddChunk(ui32 chunkId) {
            PutChunkIdToFreeChunks(chunkId);
        }

        void THeap::RecoveryModeRemoveChunks(const TVector<ui32> &chunkIds) {
            for (ui32 x : chunkIds) {
                TFreeChunks::iterator it = FreeChunks.find(x);
                Y_VERIFY_S(it != FreeChunks.end(), VDiskLogPrefix << "chunkId# " << x << " State# " << ToString());
                FreeChunks.erase(it);
            }
        }

        bool THeap::ReleaseSlot(THugeSlot slot) {
            TChain* const chain = Chains.GetChain(slot.GetSize());
            Y_VERIFY_S(chain, VDiskLogPrefix << "State# " << ToString() << " slot# " << slot.ToString());
            if (TFreeRes res = chain->Free(chain->Convert(slot)); res.ChunkId) {
                PutChunkIdToFreeChunks(res.ChunkId);
                return res.InLockedChunks;
            }
            return false;
        }

        void THeap::OccupySlot(THugeSlot slot, bool inLockedChunks) {
            TChain* const chain = Chains.GetChain(slot.GetSize());
            Y_VERIFY_S(chain, VDiskLogPrefix << "State# " << ToString() << " slot# " << slot.ToString());
            if (!chain->RecoveryModeAllocate(chain->Convert(slot))) {
                const size_t numErased = FreeChunks.erase(slot.GetChunkId());
                Y_VERIFY_S(numErased, VDiskLogPrefix << "State# " << ToString() << " slot# " << slot.ToString());
                chain->RecoveryModeAllocate(chain->Convert(slot), slot.GetChunkId(), inLockedChunks);
            }
        }

        //////////////////////////////////////////////////////////////////////////////////////////
        // THeap: Serialize/Parse/Check
        //////////////////////////////////////////////////////////////////////////////////////////
        TString THeap::Serialize() {
            TStringStream str;
            ::Save(&str, Signature);
            ::Save(&str, FreeChunks);
            ::Save(&str, Chains);
            return str.Str();
        }

        void THeap::ParseFromString(const TString &serialized) {
            FreeChunks.clear();
            TStringInput str(serialized);
            ui32 signature = 0;
            ::Load(&str, signature);
            ::Load(&str, FreeChunks);
            ::Load(&str, Chains);
        }

        bool THeap::CheckEntryPoint(const TString &serialized) {
            TStringInput str(serialized);
            ui32 signature = 0;
            ::Load(&str, signature);
            return Signature == signature;
        }

        void THeap::GetOwnedChunks(TSet<TChunkIdx>& chunks) const {
            for (TChunkIdx chunk : FreeChunks) {
                const bool inserted = chunks.insert(chunk).second;
                Y_VERIFY_S(inserted, VDiskLogPrefix); // this chunk should be unique to the set
            }
            Chains.GetOwnedChunks(chunks);
        }

        //////////////////////////////////////////////////////////////////////////////////////////
        // THeap: Output
        //////////////////////////////////////////////////////////////////////////////////////////
        void THeap::RenderHtml(IOutputStream &str) const {
            str << "\n";
            HTML(str) {
                COLLAPSED_BUTTON_CONTENT("hugeheapusageid", "Heap Usage") {
                    Chains.RenderHtmlForUsage(str);
                }
                str << "<br/>";
                COLLAPSED_BUTTON_CONTENT("hugeheapstateid", "Heap State") {
                    str << "FreeChunks: ";
                    if (FreeChunks.empty()) {
                        str << "empty";
                    } else {
                        for (const auto &x : FreeChunks) {
                            str << x;
                        }
                    }
                    str << "<br>";
                    Chains.RenderHtml(str);
                }
            }
            str << "\n";
        }

        TString THeap::ToString() const {
            TStringStream str;
            str << "FreeChunks# " << FormatList(FreeChunks)
                << " Chains# {" << Chains.ToString() << '}';
            return str.Str();
        }

        //////////////////////////////////////////////////////////////////////////////////////////
        // THeap: Private
        //////////////////////////////////////////////////////////////////////////////////////////
        inline ui32 THeap::GetChunkIdFromFreeChunks() {
            Y_VERIFY_S(!FreeChunks.empty(), VDiskLogPrefix << "State# " << ToString());
            TFreeChunks::iterator it = FreeChunks.begin();
            ui32 chunkId = *it;
            FreeChunks.erase(it);
            return chunkId;
        }

        inline void THeap::PutChunkIdToFreeChunks(ui32 chunkId) {
            if (ForbiddenChunks.erase(chunkId)) {
                ForceFreeChunks.push_back(chunkId);
            } else {
                bool res = FreeChunks.insert(chunkId).second;
                Y_VERIFY_S(res, VDiskLogPrefix << "State# " << ToString());
            }
        }

    } // NHuge
} // NKikimr
