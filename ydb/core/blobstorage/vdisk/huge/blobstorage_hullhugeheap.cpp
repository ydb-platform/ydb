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
            TChainLayoutBuilder::TChainLayoutBuilder(ui32 left, ui32 milestone, ui32 right, ui32 overhead) {
                BuildDownward(left, milestone, overhead);
                BuildUpward(milestone, right, overhead);
                Check(left, right);
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

            void TChainLayoutBuilder::Check(ui32 left, ui32 right) {
                // check integrity of the built layout
                Y_ABORT_UNLESS(Layout.size() > 1);
                Y_ABORT_UNLESS(Layout.begin()->Left <= left);
                Y_ABORT_UNLESS((Layout.end() - 1)->Right >= right);
                for (size_t i = 1, s = Layout.size(); i < s; ++i) {
                    Y_ABORT_UNLESS(Layout[i - 1].Right == Layout[i].Left);
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
            if (FreeSpace.empty())
                return false;

            TFreeSpace::iterator it = FreeSpace.begin();
            TMask &mask = it->second;

            Y_VERIFY_S(!mask.Empty(), VDiskLogPrefix << "TChain::Allocate1:"
                    << " id# " << id->ToString() << " State# " << ToString());
            ui32 slot = mask.FirstNonZeroBit();
            mask.Reset(slot);

            *id = NPrivate::TChunkSlot(it->first, slot);

            if (mask.Empty())
                FreeSpace.erase(it);

            AllocatedSlots++;
            --FreeSlotsInFreeSpace;
            return true;
        }

        // allocate id, but we know that this chain doesn't have free slots, so add a chunk to it
        void TChain::Allocate(NPrivate::TChunkSlot *id, TChunkID chunkId) {
            Y_VERIFY_S(FreeSpace.empty(), VDiskLogPrefix << "Empty");

            FreeSpace.emplace(chunkId, ConstMask);
            FreeSlotsInFreeSpace += SlotsInChunk;
            bool res = Allocate(id);
            Y_VERIFY_S(res, VDiskLogPrefix << "TChain::Allocate2:"
                    << " id# " << id->ToString() << " chunkId# " << chunkId << " State# " << ToString());
        }

        TFreeRes TChain::Free(const NPrivate::TChunkSlot &id) {
            Y_VERIFY_S(id.GetSlotId() < SlotsInChunk && AllocatedSlots > 0, VDiskLogPrefix
                    << " id# " << id.ToString() << " SlotsInChunk# " << SlotsInChunk
                    << " AllocatedSlots# " << AllocatedSlots << " State# " << ToString());
            AllocatedSlots--;

            ui32 chunkId = id.GetChunkId();
            ui32 slotId = id.GetSlotId();
            Y_VERIFY_S(chunkId, VDiskLogPrefix << "chunkId# " << chunkId);
            TFreeSpace::iterator it;

            auto freeFoundSlot = [&] (TFreeSpace &container, const char *containerName, bool inLockedChunks) {
                TMask &mask = it->second;
                Y_VERIFY_S(!mask.Get(slotId), VDiskLogPrefix << "TChain::Free: containerName# " << containerName
                        << " id# " << id.ToString() << " State# " << ToString());
                mask.Set(slotId);
                ++FreeSlotsInFreeSpace;
                if (mask == ConstMask) {
                    // free chunk
                    container.erase(it);
                    FreeSlotsInFreeSpace -= SlotsInChunk;
                    return TFreeRes(chunkId, ConstMask, SlotsInChunk, inLockedChunks);
                } else
                    return TFreeRes(0, mask, SlotsInChunk, false);
            };

            if ((it = FreeSpace.find(chunkId)) != FreeSpace.end()) {
                return freeFoundSlot(FreeSpace, "FreeSpace", false);
            } else if ((it = LockedChunks.find(chunkId)) != LockedChunks.end()) {
                return freeFoundSlot(LockedChunks, "LockedChunks", true);
            } else {
                // chunk is neither in FreeSpace nor in LockedChunks
                TDynBitMap mask;
                mask.Reserve(SlotsInChunk);
                mask.Reset(0, SlotsInChunk);
                mask.Set(slotId);
                ++FreeSlotsInFreeSpace;

                FreeSpace.emplace(chunkId, mask);
                return TFreeRes(0, mask, SlotsInChunk, false); // no empty chunk
            }
        }

        bool TChain::LockChunkForAllocation(TChunkID chunkId) {
            if (auto nh = FreeSpace.extract(chunkId)) {
                LockedChunks.insert(std::move(nh));
                return true;
            } else {
                // chunk is already freed
                return false;
            }
        }

        void TChain::UnlockChunk(TChunkID chunkId) {
            if (auto nh = LockedChunks.extract(chunkId)) {
                FreeSpace.insert(std::move(nh));
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
            Y_ABORT_UNLESS(usedChunksInFreeSpace - chunksToStoreDefragmentedSlots >= 0);
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
                TMask &mask = it->second;
                Y_VERIFY_S(mask.Get(slotId), VDiskLogPrefix << "RecoveryModeAllocate:"
                        << " id# " << id.ToString() << " State# " << ToString());
                mask.Reset(slotId);

                if (mask.Empty()) {
                    map->erase(it);
                }

                --FreeSlotsInFreeSpace;
                AllocatedSlots++;
                return true;
            } else {
                return false;
            }
        }

        void TChain::RecoveryModeAllocate(const NPrivate::TChunkSlot &id, TChunkID chunkId, bool inLockedChunks) {
            Y_VERIFY_S(id.GetChunkId() == chunkId && !FreeSpace.contains(chunkId) && !LockedChunks.contains(chunkId),
                    VDiskLogPrefix << " id# " << id.ToString() << " chunkId# " << chunkId << " State# " << ToString());

            (inLockedChunks ? LockedChunks : FreeSpace).emplace(chunkId, ConstMask);
            FreeSlotsInFreeSpace += SlotsInChunk;
            bool res = RecoveryModeAllocate(id);

            Y_VERIFY_S(res, VDiskLogPrefix << "RecoveryModeAllocate:"
                    << " id# " << id.ToString() << " chunkId# " << chunkId << " State# " << ToString());
        }

        void TChain::Save(IOutputStream *s) const {
            ::Save(s, SlotsInChunk);
            ::Save(s, AllocatedSlots);
            ::SaveSize(s, FreeSpace.size() + LockedChunks.size());
            ForEachFreeSpaceChunk(std::bind(&::Save<TFreeSpace::value_type>, s, std::placeholders::_1));
        }

        TChain TChain::Load(IInputStream *s, TString vdiskLogPrefix, ui32 appendBlockSize, ui32 blocksInChunk,
                std::span<TChain> chains, bool *compatible) {
            ui32 slotsInChunk;
            ::Load(s, slotsInChunk);

            const ui32 slotSizeInBlocks = blocksInChunk / slotsInChunk;
            Y_ABORT_UNLESS(slotSizeInBlocks);
            ui32 slotSize = slotSizeInBlocks * appendBlockSize; // assume optimal slot size for specific slots per chunk

            // check if this goes with compatible chain
            for (const TChain& chainFromBuilder : chains) {
                if (chainFromBuilder.SlotsInChunk < slotsInChunk) { // no such chain from builder
                    *compatible = false;
                } else if (chainFromBuilder.SlotsInChunk == slotsInChunk) {
                    slotSize = chainFromBuilder.SlotSize;
                } else {
                    continue;
                }
                break;
            }

            TChain res{
                std::move(vdiskLogPrefix),
                slotsInChunk,
                slotSize, // in bytes
            };

            ::Load(s, res.AllocatedSlots);
            ::Load(s, res.FreeSpace);
            for (const auto& [chunkId, mask] : res.FreeSpace) {
                res.FreeSlotsInFreeSpace += mask.Count();
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
                const auto& [chunk, bitmap] = value;
                str << " {" << chunk;
                ui32 begin;
                ui32 prev = Max<ui32>();
                Y_FOR_EACH_BIT(i, bitmap) {
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
                            const auto& [chunk, bitmap] = value;
                            str << " [" << chunk << " " << bitmap.Count() << "]";
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
            , MaxBlobInBytes(maxBlobInBytes)
            , Overhead(overhead)
            , MinHugeBlobInBlocks(MinHugeBlobInBytes / AppendBlockSize)
        {
            Y_VERIFY_S(MinHugeBlobInBytes &&
                    MinHugeBlobInBytes <= MilestoneBlobInBytes &&
                    MilestoneBlobInBytes < MaxBlobInBytes, "INVALID CONFIGURATION! (SETTINGS ARE:"
                            << " MaxBlobInBytes# " << MaxBlobInBytes << " MinHugeBlobInBytes# " << MinHugeBlobInBytes
                            << " MilestoneBlobInBytes# " << MilestoneBlobInBytes << " ChunkSize# " << ChunkSize
                            << " AppendBlockSize# " << AppendBlockSize << ")");

            BuildChains();
        }

        TChain *TAllChains::GetChain(ui32 size) {
            if (size < MinHugeBlobInBytes || GetEndBlocks() * AppendBlockSize < size) {
                return nullptr;
            }
            const size_t index = SizeToBlocks(size) - MinHugeBlobInBlocks;
            Y_DEBUG_ABORT_UNLESS(index < SearchTable.size());
            const size_t chainIndex = SearchTable[index];
            Y_DEBUG_ABORT_UNLESS(chainIndex < Chains.size());
            return &Chains[chainIndex];
        }

        const TChain *TAllChains::GetChain(ui32 size) const {
            if (size < MinHugeBlobInBytes || GetEndBlocks() * AppendBlockSize < size) {
                return nullptr;
            }
            Y_ABORT_UNLESS(MinHugeBlobInBytes <= size);
            const size_t index = SizeToBlocks(size) - MinHugeBlobInBlocks;
            Y_DEBUG_ABORT_UNLESS(index < SearchTable.size());
            const size_t chainIndex = SearchTable[index];
            Y_DEBUG_ABORT_UNLESS(chainIndex < Chains.size());
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
            if (DeserializedSlotSizes.empty()) { // if this was initially empty heap, write it fully
                writeCompatible = false;
            } else {
                for (auto& chain : Chains) {
                    if (DeserializedSlotSizes.contains(chain.SlotSize)) {
                        ++numChains;
                    } else if (chain.HaveBeenUsed()) {
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
            auto chains = Chains | std::views::filter([&](const auto& chain) {
                return !writeCompatible || DeserializedSlotSizes.contains(chain.SlotSize);
            });
            ::SaveRange(s, chains.begin(), chains.end());
        }

        void TAllChains::Load(IInputStream *s) {
            std::vector<TChain> newChains;
            newChains.reserve(Chains.size());

            auto chainsIt = Chains.begin();
            const auto chainsEnd = Chains.end();

            Y_DEBUG_ABORT_UNLESS(ChunkSize % AppendBlockSize == 0);
            Y_DEBUG_ABORT_UNLESS(AppendBlockSize <= ChunkSize);
            const ui32 blocksInChunk = ChunkSize / AppendBlockSize;

            bool compatible = true; // if the entrypoint is compatible with chain layout builder
            ui32 prevSlotSize = 0;

            ui32 size;
            for (::Load(s, size); size; --size) {
                auto chain = TChain::Load(s, VDiskLogPrefix, AppendBlockSize, blocksInChunk, {chainsIt, chainsEnd},
                    &compatible);
                DeserializedSlotSizes.insert(chain.SlotSize);

                Y_ABORT_UNLESS(chain.SlotSize > std::exchange(prevSlotSize, chain.SlotSize));

                for (; chainsIt != chainsEnd && chainsIt->SlotSize <= chain.SlotSize; ++chainsIt) {
                    if (chainsIt->SlotSize == chain.SlotSize) {
                        Y_ABORT_UNLESS(chain.SlotsInChunk == chainsIt->SlotsInChunk);
                        Y_ABORT_UNLESS(!chainsIt->HaveBeenUsed());
                    } else {
                        newChains.push_back(std::move(*chainsIt));
                    }
                }

                newChains.push_back(std::move(chain));
            }

            std::ranges::move(chainsIt, chainsEnd, std::back_inserter(newChains));

            Chains = std::move(newChains);
            if (!compatible) { // deserialized slot sizes can't be stored in compatible way
                DeserializedSlotSizes.clear();
            }
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
                << " MaxBlobInBytes# " << MaxBlobInBytes;
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
            const ui32 endBlocks = GetEndBlocks();

            NPrivate::TChainLayoutBuilder builder(startBlocks, milestoneBlocks, endBlocks, Overhead);
            const ui32 blocksInChunk = ChunkSize / AppendBlockSize;

            for (auto x : builder.GetLayout()) {
                const ui32 slotSizeInBlocks = x.Right;
                const ui32 slotSize = slotSizeInBlocks * AppendBlockSize;
                const ui32 slotsInChunk = blocksInChunk / slotSizeInBlocks;
                Chains.emplace_back(VDiskLogPrefix, slotsInChunk, slotSize);
            }

            Y_ABORT_UNLESS(!Chains.empty());
        }

        void TAllChains::BuildSearchTable() {
            Y_ABORT_UNLESS(SearchTable.empty());
            Y_ABORT_UNLESS(!Chains.empty());

            const ui32 startBlocks = MinHugeBlobInBlocks;
            const ui32 minSize = startBlocks * AppendBlockSize;
            const ui32 endBlocks = GetEndBlocks(); // maximum possible number of blocks per huge blob
            auto it = Chains.begin();
            ui16 index = 0;

            SearchTable.reserve(endBlocks - startBlocks + 1);
            for (ui32 i = startBlocks, size = minSize; i <= endBlocks; ++i, size += AppendBlockSize) {
                if (it->SlotSize < size) { // size doesn't fit in current chain, but it must fit into next one
                    ++it;
                    Y_ABORT_UNLESS(it != Chains.end());
                    Y_ABORT_UNLESS(size <= it->SlotSize);
                    Y_ABORT_UNLESS(index != Max<ui16>());
                    ++index;
                }
                SearchTable.push_back(index);
            }
        }

        ui32 TAllChains::SizeToBlocks(ui32 size) const {
            return (size + AppendBlockSize - 1) / AppendBlockSize;
        }

        ui32 TAllChains::GetEndBlocks() const {
            return SizeToBlocks(MaxBlobInBytes);
        }

        void TAllChains::FinishRecovery() {
            ui32 prevSlotSize = 0;
            for (const TChain& chain : Chains) {
                Y_ABORT_UNLESS(prevSlotSize < chain.SlotSize);
                prevSlotSize = chain.SlotSize;
            }
            BuildSearchTable();
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
            Y_ABORT_UNLESS(chain);
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
            Y_ABORT_UNLESS(chain);

            TFreeRes res = chain->Free(chain->Convert(addr));
            if (res.ChunkId) {
                PutChunkIdToFreeChunks(res.ChunkId);
            }
            return res;
        }

        void THeap::AddChunk(ui32 chunkId) {
            PutChunkIdToFreeChunks(chunkId);
        }

        ui32 THeap::RemoveChunk() {
            if (FreeChunks.size() > FreeChunksReservation) {
                ui32 chunkId = GetChunkIdFromFreeChunks();
                Y_VERIFY_S(chunkId, VDiskLogPrefix << "State# " << ToString());
                return chunkId;
            } else {
                return 0;
            }
        }

        bool THeap::LockChunkForAllocation(ui32 chunkId, ui32 slotSize) {
            TChain *chain = Chains.GetChain(slotSize);
            Y_ABORT_UNLESS(chain);
            return chain->LockChunkForAllocation(chunkId);
        }

        void THeap::UnlockChunk(ui32 chunkId, ui32 slotSize) {
            TChain *chain = Chains.GetChain(slotSize);
            Y_ABORT_UNLESS(chain);
            chain->UnlockChunk(chunkId);
        }

        void THeap::FinishRecovery() {
            Chains.FinishRecovery();
        }

        THeapStat THeap::GetStat() const {
            return Chains.GetStat();
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
                Y_ABORT_UNLESS(inserted); // this chunk should be unique to the set
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
            bool res = FreeChunks.insert(chunkId).second;
            Y_VERIFY_S(res, VDiskLogPrefix << "State# " << ToString());
        }

    } // NHuge
} // NKikimr

