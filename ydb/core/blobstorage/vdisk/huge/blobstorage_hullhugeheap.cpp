#include "blobstorage_hullhugeheap.h"

#include <library/cpp/monlib/service/pages/templates.h>

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
                str << "CHAIN TABLE (MilesoneId=" << MilesoneId << " rows=" << Layout.size() << "):\n";
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
                MilesoneId = Layout.size();

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

            auto freeFoundSlot = [&] (TFreeSpace &container, const char *containerName) {
                TMask &mask = it->second;
                Y_VERIFY_S(!mask.Get(slotId), VDiskLogPrefix << "TChain::Free: containerName# " << containerName
                        << " id# " << id.ToString() << " State# " << ToString());
                mask.Set(slotId);
                ++FreeSlotsInFreeSpace;
                if (mask == ConstMask) {
                    // free chunk
                    container.erase(it);
                    FreeSlotsInFreeSpace -= SlotsInChunk;
                    return TFreeRes(chunkId, ConstMask, SlotsInChunk);
                } else
                    return TFreeRes(0, mask, SlotsInChunk);
            };

            if ((it = FreeSpace.find(chunkId)) != FreeSpace.end()) {
                return freeFoundSlot(FreeSpace, "FreeSpace");
            } else if ((it = LockedChunks.find(chunkId)) != LockedChunks.end()) {
                return freeFoundSlot(LockedChunks, "LockedChunks");
            } else {
                // chunk is neither in FreeSpace nor in LockedChunks
                TDynBitMap mask;
                mask.Reserve(SlotsInChunk);
                mask.Reset(0, SlotsInChunk);
                mask.Set(slotId);
                ++FreeSlotsInFreeSpace;

                FreeSpace.emplace(chunkId, mask);
                return TFreeRes(0, mask, SlotsInChunk); // no empty chunk
            }
        }

        bool TChain::LockChunkForAllocation(TChunkID chunkId) {
            if (TFreeSpace::iterator it = FreeSpace.find(chunkId); it != FreeSpace.end()) {
                LockedChunks.insert(FreeSpace.extract(it));
                return true;
            } else {
                // chunk is already freed
                return false;
            }
        }

        void TChain::UnlockChunk(TChunkID chunkId) {
            if (auto it = LockedChunks.find(chunkId); it != LockedChunks.end()) {
                FreeSpace.insert(LockedChunks.extract(it));
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

            TFreeSpace::iterator it = FreeSpace.find(chunkId);
            if (it != FreeSpace.end()) {
                TMask &mask = it->second;
                Y_VERIFY_S(mask.Get(slotId), VDiskLogPrefix << "RecoveryModeAllocate:"
                        << " id# " << id.ToString() << " State# " << ToString());
                mask.Reset(slotId);

                if (mask.Empty()) {
                    FreeSpace.erase(it);
                }

                --FreeSlotsInFreeSpace;
                AllocatedSlots++;
                return true;
            } else {
                return false;
            }
        }

        void TChain::RecoveryModeAllocate(const NPrivate::TChunkSlot &id, TChunkID chunkId) {
            Y_VERIFY_S(id.GetChunkId() == chunkId && FreeSpace.find(chunkId) == FreeSpace.end(),
                    VDiskLogPrefix << " id# " << id.ToString() << " chunkId# " << chunkId << " State# " << ToString());

            FreeSpace.emplace(chunkId, ConstMask);
            FreeSlotsInFreeSpace += SlotsInChunk;
            bool res = RecoveryModeAllocate(id);

            Y_VERIFY_S(res, VDiskLogPrefix << "RecoveryModeAllocate:"
                    << " id# " << id.ToString() << " chunkId# " << chunkId << " State# " << ToString());
        }

        void TChain::Save(IOutputStream *s) const {
            ::Save(s, SlotsInChunk);
            ::Save(s, AllocatedSlots);
            if (LockedChunks) {
                TFreeSpace temp(FreeSpace);
                temp.insert(LockedChunks.begin(), LockedChunks.end());
                ::Save(s, temp);
            } else {
                ::Save(s, FreeSpace);
            }
        }

        void TChain::Load(IInputStream *s) {
            FreeSpace.clear();
            ui32 slotsInChunk = 0;
            ::Load(s, slotsInChunk);
            Y_VERIFY_S(slotsInChunk == SlotsInChunk, VDiskLogPrefix
                    << "slotsInChunk# " << slotsInChunk << " SlotsInChunk# " << SlotsInChunk);
            ::Load(s, AllocatedSlots);
            ::Load(s, FreeSpace);
            FreeSlotsInFreeSpace = 0;
            for (const auto &[chunkId, mask] : FreeSpace) {
                // all 1 in mask -- free slots
                // 0 - slot is in use
                FreeSlotsInFreeSpace += mask.Count();
            }
        }

        bool TChain::HaveBeenUsed() const {
            return AllocatedSlots != 0 || !FreeSpace.empty();
        }

        TString TChain::ToString() const {
            TStringStream str;
            auto output = [&str] (const TFreeSpace &c) {
                for (const auto &x : c) {
                    for (size_t i = 0; i < x.second.Size(); i++) {
                        if (x.second.Test(i))
                            str << " [" << x.first << " " << i << "]";
                    }
                }
            };

            str << "{AllocatedSlots# " << AllocatedSlots << " [ChunkId FreeSlot]:";
            output(FreeSpace);
            output(LockedChunks);
            str << "}";
            return str.Str();
        }

        void TChain::RenderHtml(IOutputStream &str) const {
            auto output = [&str] (const TFreeSpace &c) {
                for (const auto &x : c) {
                    size_t freeSlots = 0;
                    for (size_t i = 0; i < x.second.Size(); i++) {
                        if (x.second.Test(i))
                            ++freeSlots;
                    }
                    if (freeSlots) {
                        str << " [" << x.first << " " << freeSlots << "]";
                    }
                }
            };

            output(FreeSpace);
            output(LockedChunks);
        }

        ui32 TChain::GetAllocatedSlots() const {
            return AllocatedSlots;
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
        // TChainDelegator
        ////////////////////////////////////////////////////////////////////////////
        TChainDelegator::TChainDelegator(const TString &vdiskLogPrefix, ui32 valBlocks, ui32 shiftBlocks,
                ui32 chunkSize, ui32 appendBlockSize)
            : VDiskLogPrefix(vdiskLogPrefix)
            , Blocks(valBlocks)
            , ShiftInBlocks(shiftBlocks)
            , SlotsInChunk(0)
            , SlotSize(0)
        {
            ui32 slotSizeInBlocks = Blocks + ShiftInBlocks;
            ui32 blocksInChunk = chunkSize / appendBlockSize;
            Y_VERIFY_S(appendBlockSize * blocksInChunk == chunkSize, VDiskLogPrefix
                    << "Blocks# " << Blocks << " ShiftInBlocks# " << ShiftInBlocks
                    << " chunkSize# " << chunkSize << " appendBlockSize# " << appendBlockSize);

            SlotsInChunk = blocksInChunk / slotSizeInBlocks;
            SlotSize = slotSizeInBlocks * appendBlockSize;

            ChainPtr = MakeIntrusive<TChain>(vdiskLogPrefix, SlotsInChunk);
        }

        THugeSlot TChainDelegator::Convert(const NPrivate::TChunkSlot &id) const {
            return THugeSlot(id.GetChunkId(), id.GetSlotId() * SlotSize, SlotSize);
        }

        NPrivate::TChunkSlot TChainDelegator::Convert(const TDiskPart &addr) const {
            ui32 slotId = addr.Offset / SlotSize;
            Y_VERIFY_S(slotId * SlotSize == addr.Offset, VDiskLogPrefix
                    << "slotId# " << slotId << " addr# " << addr.ToString() << " State# " << ToString());
            return NPrivate::TChunkSlot(addr.ChunkIdx, slotId);
        }

        void TChainDelegator::Save(IOutputStream *s) const {
            ::Save(s, *ChainPtr);
        }

        void TChainDelegator::Load(IInputStream *s) {
            ::Load(s, *ChainPtr);
        }

        bool TChainDelegator::HaveBeenUsed() const {
            return ChainPtr->HaveBeenUsed();
        }

        TString TChainDelegator::ToString() const {
            TStringStream str;
            str << "{[SlotSize, SlotsInChunk]: [" << SlotSize << ", " << SlotsInChunk << "] "
                << ChainPtr->ToString() << "}";
            return str.Str();
        }

        void TChainDelegator::GetOwnedChunks(TSet<TChunkIdx>& chunks) const {
            ChainPtr->GetOwnedChunks(chunks);
        }

        void TChainDelegator::RenderHtml(IOutputStream &str) const {
            HTML(str) {
                TABLER() {
                    TABLED() {str << SlotSize << " / " << SlotsInChunk;}
                    TABLED() {ChainPtr->RenderHtml(str);}
                }
            }
        }

        void TChainDelegator::RenderHtmlForUsage(IOutputStream &str) const {
            HTML(str) {
                TABLER() {
                    TABLED() {str << SlotSize;}
                    TABLED() {str << SlotsInChunk;}
                    TABLED() {str << ChainPtr->GetAllocatedSlots();}
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
                ui32 overhead,
                bool oldMapCompatible)
            : VDiskLogPrefix(vdiskLogPrefix)
            , ChunkSize(chunkSize)
            , AppendBlockSize(appendBlockSize)
            , MinHugeBlobInBytes(minHugeBlobInBytes)
            , MilestoneBlobInBytes(milestoneBlobInBytes)
            , MaxBlobInBytes(maxBlobInBytes)
            , Overhead(overhead)
            , OldMapCompatible(oldMapCompatible)
        {
            Y_VERIFY_S(MinHugeBlobInBytes != 0 &&
                    MinHugeBlobInBytes <= MilestoneBlobInBytes &&
                    MilestoneBlobInBytes < MaxBlobInBytes, "INVALID CONFIGURATION! (SETTINGS ARE:"
                            << " MaxBlobInBytes# " << MaxBlobInBytes << " MinHugeBlobInBytes# " << MinHugeBlobInBytes
                            << " MilestoneBlobInBytes# " << MilestoneBlobInBytes << " ChunkSize# " << ChunkSize
                            << " AppendBlockSize# " << AppendBlockSize << ")");
            BuildLayout(OldMapCompatible);
        }

        ui32 TAllChains::GetMinREALHugeBlobInBytes() const {
            Y_ABORT_UNLESS(MinREALHugeBlobInBlocks);
            return MinREALHugeBlobInBlocks * AppendBlockSize + 1;
        }

        TChainDelegator *TAllChains::GetChain(ui32 size) {
            return SearchTable.at(SizeToBlocks(size));
        }

        const TChainDelegator *TAllChains::GetChain(ui32 size) const {
            return SearchTable.at(SizeToBlocks(size));
        }

        THeapStat TAllChains::GetStat() const {
            THeapStat stat;
            for (const auto &x : ChainDelegators) {
                stat += x.ChainPtr->GetStat();
            }
            return stat;
        }

        void TAllChains::PrintOutChains(IOutputStream &str) const {
            str << "CHAIN TABLE (rows=" << ChainDelegators.size() << "):\n";
            for (const auto &x : ChainDelegators) {
                str << "Blocks# (" << x.Blocks << ", " << (x.Blocks + x.ShiftInBlocks) << "] "
                    << "Bytes# (" << (x.Blocks * AppendBlockSize) << ", "
                    << ((x.Blocks + x.ShiftInBlocks) * AppendBlockSize) << "] "
                    << " SlotSize# " << x.SlotSize << " SlotsInChunk# " << x.SlotsInChunk << "\n";
            }
        }

        void TAllChains::PrintOutSearchTable(IOutputStream &str) const {
            str << "SEARCH TABLE:\n";
            for (const auto &x : SearchTable) {
                str << (&x - &SearchTable[0]) << " idx: ";
                if (x) {
                    str << x - &ChainDelegators[0];
                } else {
                    str << "null";
                }
                str << "\n";
            }
        }

        void TAllChains::Save(IOutputStream *s) const {
            if (OldMapCompatible && (StartMode == EStartMode::Empty || StartMode == EStartMode::Migrated)) {
                // this branch takes place when:
                // 1. OldMapCompatible = true, i.e. we are in 19-1 stable branch
                // 2. we didn't rollback from 19-2, i.e. we read empty db or migrated on start
                // => save only second part of data
                TBuiltChainDelegators b = BuildChains(MilestoneBlobInBytes);
                ui32 size = b.ChainDelegators.size();
                ::Save(s, size);
                ui32 skip = ChainDelegators.size() - size;
                for (auto &x : ChainDelegators) {
                    if (skip > 0) {
                        Y_ABORT_UNLESS(!x.HaveBeenUsed());
                        --skip;
                        continue;
                    }
                    ::Save(s, x);
                }
            } else {
                // save all
                ui32 size = ChainDelegators.size();
                ::Save(s, size);
                for (auto &x : ChainDelegators) {
                    ::Save(s, x);
                }
            }
        }

        void TAllChains::Load(IInputStream *s) {
            ui32 size = 0;
            // load array size
            ::Load(s, size);
            if (size == ChainDelegators.size()) {
                StartMode = EStartMode::Loaded;
                // load map and current map are of the same size, just load it
                for (auto &x : ChainDelegators) {
                    ::Load(s, x);
                }
            } else if (size < ChainDelegators.size()) {
                // map size has been changed, run migration
                StartMode = EStartMode::Migrated;
                TBuiltChainDelegators b = BuildChains(MilestoneBlobInBytes);
                Y_VERIFY_S(size == b.ChainDelegators.size(), "size# " << size
                        << " b.ChainDelegators.size()# " << b.ChainDelegators.size());

                // load into temporary delegators
                for (auto &x : b.ChainDelegators) {
                    ::Load(s, x);
                }

                // migrate
                using TIt = TAllChainDelegators::iterator;
                TIt loadedIt = b.ChainDelegators.begin();
                TIt loadedEnd = b.ChainDelegators.end();
                for (TIt it = ChainDelegators.begin(); it != ChainDelegators.end(); ++it) {
                    Y_ABORT_UNLESS(loadedIt != loadedEnd);
                    if (loadedIt->SlotSize == it->SlotSize) {
                        *it = std::move(*loadedIt);
                        ++loadedIt;
                    }
                }
                Y_ABORT_UNLESS(loadedIt == loadedEnd);
            } else {
                Y_ABORT_UNLESS(size > ChainDelegators.size());

                // skip first delegators, which must not be used
                for (size_t i = ChainDelegators.size(); i < size; ++i) {
                    ui32 slotsInChunk;
                    ::Load(s, slotsInChunk);
                    ui32 allocatedSlots;
                    ::Load(s, allocatedSlots);
                    TMap<ui32, TMask> freeSpace;
                    ::Load(s, freeSpace);
                    Y_ABORT_UNLESS(slotsInChunk > ChainDelegators.front().SlotsInChunk, "incompatible format");
                    Y_ABORT_UNLESS(!allocatedSlots, "incompatible format");
                    Y_ABORT_UNLESS(freeSpace.empty(), "incompatible format");
                }

                // load the rest as usual
                StartMode = EStartMode::Loaded;
                for (TChainDelegator& delegator : ChainDelegators) {
                    ::Load(s, delegator);
                }
            }
        }

        void TAllChains::GetOwnedChunks(TSet<TChunkIdx>& chunks) const {
            for (const TChainDelegator& delegator : ChainDelegators) {
                delegator.GetOwnedChunks(chunks);
            }
        }

        TString TAllChains::ToString() const {
            TStringStream str;
            str << "{ChunkSize# " << ChunkSize << " AppendBlockSize# " << AppendBlockSize
                << " MinHugeBlobInBytes# " << MinHugeBlobInBytes << " MaxBlobInBytes# " << MaxBlobInBytes;
            for (const auto & x : ChainDelegators) {
                str << " {CHAIN " << x.ToString() << "}";
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
                        for (const auto & x : ChainDelegators)
                            x.RenderHtml(str);
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
                        for (const auto & x : ChainDelegators)
                            x.RenderHtmlForUsage(str);
                    }
                }
            }
        }

        TVector<NPrivate::TChainLayoutBuilder::TSeg> TAllChains::GetLayout() const {
            TVector<NPrivate::TChainLayoutBuilder::TSeg> res;
            res.reserve(ChainDelegators.size());
            for (const auto &x : ChainDelegators) {
                res.push_back(NPrivate::TChainLayoutBuilder::TSeg {x.Blocks, x.Blocks + x.ShiftInBlocks} );
            }
            return res;
        }

        std::shared_ptr<THugeSlotsMap> TAllChains::BuildHugeSlotsMap() const {
            THugeSlotsMap::TAllSlotsInfo targetAllSlotsInfo;
            THugeSlotsMap::TSearchTable targetSearchTable;

            for (const auto &x : SearchTable) {
                if (!x) {
                    // first records in SearchTable are equal to nullptr
                    targetSearchTable.push_back(THugeSlotsMap::NoOpIdx);
                    continue;
                }

                if (targetAllSlotsInfo.empty() || targetAllSlotsInfo.back().SlotSize != x->SlotSize) {
                    targetAllSlotsInfo.emplace_back(x->SlotSize, x->SlotsInChunk);
                }
                targetSearchTable.push_back(THugeSlotsMap::TIndex(targetAllSlotsInfo.size() - 1));
            }

            return std::make_shared<THugeSlotsMap>(AppendBlockSize, std::move(targetAllSlotsInfo),
                std::move(targetSearchTable));
        }

        ////////////////////////////////////////////////////////////////////////////
        // TAllChains: Private
        ////////////////////////////////////////////////////////////////////////////
        TAllChains::TBuiltChainDelegators TAllChains::BuildChains(ui32 minHugeBlobInBytes) const {
            // minHugeBlobInBytes -- is the only variable parameter, used for migration
            const ui32 startBlocks = minHugeBlobInBytes / AppendBlockSize;
            const ui32 mileStoneBlocks = MilestoneBlobInBytes / AppendBlockSize;
            const ui32 endBlocks = GetEndBlocks();

            NPrivate::TChainLayoutBuilder builder(startBlocks, mileStoneBlocks, endBlocks, Overhead);
            Y_ABORT_UNLESS(!builder.GetLayout().empty());

            TBuiltChainDelegators result;
            for (auto x : builder.GetLayout()) {
                result.ChainDelegators.emplace_back(VDiskLogPrefix, x.Left, x.Right - x.Left,
                    ChunkSize, AppendBlockSize);
            }

            result.MinREALHugeBlobInBlocks = builder.GetLayout()[0].Left;
            result.MilestoneREALHugeBlobInBlocks = builder.GetMilestoneSegment().Left;
            return result;
        }

        void TAllChains::BuildSearchTable() {
            const ui32 endBlocks = GetEndBlocks();
            Y_DEBUG_ABORT_UNLESS(!ChainDelegators.empty());
            TAllChainDelegators::iterator it = ChainDelegators.begin();
            TChainDelegator *ptr = nullptr;
            ui32 blocks = it->Blocks;
            for (ui32 i = 0; i <= endBlocks; i++) {
                if (i <= blocks) {
                } else {
                    ptr = &(*it);
                    ++it;
                    if (it == ChainDelegators.end())
                        blocks = ui32(-1);
                    else
                        blocks = it->Blocks;
                }
                SearchTable.push_back(ptr);
            }
        }

        void TAllChains::BuildLayout(bool oldMapCompatible)
        {
            TBuiltChainDelegators b = BuildChains(MinHugeBlobInBytes);
            ChainDelegators = std::move(b.ChainDelegators);
            MinREALHugeBlobInBlocks = oldMapCompatible ? b.MilestoneREALHugeBlobInBlocks : b.MinREALHugeBlobInBlocks;

            Y_ABORT_UNLESS(!ChainDelegators.empty());
            BuildSearchTable();

            Y_VERIFY_S(GetMinREALHugeBlobInBytes() != 0, "INVALID CONFIGURATION: MinREALHugeBlobInBytes IS 0"
                    << " (SETTINGS ARE: MaxBlobInBytes# " << MaxBlobInBytes
                    << " MinHugeBlobInBytes# " << MinHugeBlobInBytes
                    << " ChunkSize# " << ChunkSize
                    << " AppendBlockSize# " << AppendBlockSize << ')');
        }

        inline ui32 TAllChains::SizeToBlocks(ui32 size) const {
            ui32 sizeInBlocks = size / AppendBlockSize;
            sizeInBlocks += !(sizeInBlocks * AppendBlockSize == size);
            return sizeInBlocks;
        }

        inline ui32 TAllChains::GetEndBlocks() const {
            ui32 endBlocks = MaxBlobInBytes / AppendBlockSize;
            endBlocks += !(endBlocks * AppendBlockSize == MaxBlobInBytes);
            return endBlocks;
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
                ui32 freeChunksReservation,
                bool oldMapCompatible)
            : VDiskLogPrefix(vdiskLogPrefix)
            , FreeChunksReservation(freeChunksReservation)
            , FreeChunks()
            , Chains(vdiskLogPrefix, chunkSize, appendBlockSize, minHugeBlobInBytes, mileStoneBlobInBytes,
                    maxBlobInBytes, overhead, oldMapCompatible)
        {}

        //////////////////////////////////////////////////////////////////////////////////////////
        // THeap: main functions
        //////////////////////////////////////////////////////////////////////////////////////////
        THugeSlot THeap::ConvertDiskPartToHugeSlot(const TDiskPart &addr) const {
            const TChainDelegator *chainD = Chains.GetChain(addr.Size);
            Y_VERIFY_S(chainD && (addr.Offset / chainD->SlotSize * chainD->SlotSize == addr.Offset), VDiskLogPrefix
                    << "chainD# " <<  (chainD ? chainD->ToString() : "nullptr") << " addr# " << addr.ToString());
            return THugeSlot(addr.ChunkIdx, addr.Offset, chainD->SlotSize);
        }

        bool THeap::Allocate(ui32 size, THugeSlot *hugeSlot, ui32 *slotSize) {
            TChainDelegator *chainD = Chains.GetChain(size);
            Y_VERIFY_S(chainD, VDiskLogPrefix << "size# " << size << " Heap# " << ToString());
            *slotSize = chainD->SlotSize;

            NPrivate::TChunkSlot id;
            if (!chainD->ChainPtr->Allocate(&id)) { // no available slot in free space of the chain
                if (FreeChunks.empty()) { // no free chunks left for reuse -- request a new chunk
                    return false;
                }
                chainD->ChainPtr->Allocate(&id, GetChunkIdFromFreeChunks()); // reuse free chunk for this chain
            }
            *hugeSlot = chainD->Convert(id);
            return true;
        }

        TFreeRes THeap::Free(const TDiskPart &addr) {
            ui32 size = addr.Size;
            TChainDelegator *chainD = Chains.GetChain(size);
            Y_ABORT_UNLESS(chainD);

            TFreeRes res = chainD->ChainPtr->Free(chainD->Convert(addr));
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
            TChainDelegator *cd = Chains.GetChain(slotSize);
            return cd->ChainPtr->LockChunkForAllocation(chunkId);
        }

        void THeap::UnlockChunk(ui32 chunkId, ui32 slotSize) {
            TChainDelegator *cd = Chains.GetChain(slotSize);
            cd->ChainPtr->UnlockChunk(chunkId);
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
            TChainDelegator *chainD = Chains.GetChain(size);
            Y_VERIFY_S(chainD, VDiskLogPrefix << "State# " << ToString());

            NPrivate::TChunkSlot id(chainD->Convert(addr));
            bool allocated = chainD->ChainPtr->RecoveryModeAllocate(id);
            if (allocated) {
                return;
            } else {
                ui32 chunkId = addr.ChunkIdx;
                TFreeChunks::iterator it = FreeChunks.find(chunkId);
                Y_VERIFY_S(it != FreeChunks.end(), VDiskLogPrefix << "addr# " << addr.ToString() << " State# " << ToString());
                FreeChunks.erase(it);
                chainD->ChainPtr->RecoveryModeAllocate(id, chunkId);
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
            str << "FreeChunks: ";
            str << FormatList(FreeChunks);
            str << " CHAINS: " << Chains.ToString();
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

