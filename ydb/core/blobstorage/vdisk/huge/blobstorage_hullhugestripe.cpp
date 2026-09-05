#include "blobstorage_hullhugestripe.h"

#include <library/cpp/monlib/service/pages/templates.h>
#include <ydb/core/blobstorage/vdisk/common/align.h>

namespace NKikimr {
    namespace NHuge {

        TStripeHeap::TStripeHeap(TString vdiskLogPrefix, ui32 chunkSize, ui32 appendBlockSize)
            : VDiskLogPrefix(std::move(vdiskLogPrefix))
            , ChunkSize(chunkSize)
            , AppendBlockSize(appendBlockSize)
        {
            Y_VERIFY_S(AppendBlockSize > 0, VDiskLogPrefix);
            Y_VERIFY_S(ChunkSize >= AppendBlockSize, VDiskLogPrefix);
            Y_VERIFY_S(ChunkSize % AppendBlockSize == 0, VDiskLogPrefix);
        }

        TStripeHeap::TStripeHeap(TString vdiskLogPrefix, const NKikimrVDiskData::THugeKeeperStripeHeap& proto)
            : VDiskLogPrefix(std::move(vdiskLogPrefix))
        {
            LoadFromProto(proto);
        }

        ui32 TStripeHeap::AlignSize(ui32 size) const {
            return AlignUpLocal(size);
        }

        bool TStripeHeap::Empty() const {
            return Chunks.empty() && ForceFreeChunks.empty();
        }

        bool TStripeHeap::ContainsChunk(ui32 chunkId) const {
            return Chunks.contains(chunkId);
        }

        ui32 TStripeHeap::AlignUpLocal(ui32 size) const {
            Y_VERIFY_S(size > 0, VDiskLogPrefix);
            return AlignUpAppendBlockSize(size, AppendBlockSize);
        }

        void TStripeHeap::IndexInsert(ui32 length, ui32 chunkId, ui32 offset) {
            FreeBySize[length].emplace(chunkId, offset);
        }

        void TStripeHeap::IndexErase(ui32 length, ui32 chunkId, ui32 offset) {
            auto it = FreeBySize.find(length);
            Y_VERIFY_S(it != FreeBySize.end(), VDiskLogPrefix);
            const size_t erased = it->second.erase(THole(chunkId, offset));
            Y_VERIFY_S(erased == 1, VDiskLogPrefix);
            if (it->second.empty()) {
                FreeBySize.erase(it);
            }
        }

        void TStripeHeap::InsertFree(TChunkState& st, ui32 chunkId, ui32 offset, ui32 length) {
            if (!length) {
                return;
            }
            Y_VERIFY_S(offset % AppendBlockSize == 0 && length % AppendBlockSize == 0, VDiskLogPrefix);

            ui32 start = offset;
            ui32 len = length;

            auto it = st.Free.lower_bound(offset);
            if (it != st.Free.begin()) {
                auto prev = std::prev(it);
                if (prev->first + prev->second == offset) {
                    start = prev->first;
                    len += prev->second;
                    IndexErase(prev->second, chunkId, prev->first);
                    st.Free.erase(prev);
                    it = st.Free.lower_bound(offset);
                }
            }
            if (it != st.Free.end() && start + len == it->first) {
                len += it->second;
                IndexErase(it->second, chunkId, it->first);
                st.Free.erase(it);
            }

            st.Free[start] = len;
            IndexInsert(len, chunkId, start);
        }

        void TStripeHeap::EraseFree(TChunkState& st, ui32 chunkId, ui32 offset) {
            auto it = st.Free.find(offset);
            Y_VERIFY_S(it != st.Free.end(), VDiskLogPrefix);
            IndexErase(it->second, chunkId, offset);
            st.Free.erase(it);
        }

        bool TStripeHeap::TryAllocateInChunk(TChunkState& st, ui32 chunkId, ui32 offset, ui32 length, THugeSlot *hugeSlot) {
            auto it = st.Free.find(offset);
            Y_VERIFY_S(it != st.Free.end(), VDiskLogPrefix);
            Y_VERIFY_S(it->second >= length, VDiskLogPrefix);
            const ui32 holeLen = it->second;
            EraseFree(st, chunkId, offset);
            OccupyRange(st, offset, length);
            if (holeLen > length) {
                InsertFree(st, chunkId, offset + length, holeLen - length);
            }
            *hugeSlot = THugeSlot(chunkId, offset, length);
            return true;
        }

        bool TStripeHeap::RangeIsOccupied(const TChunkState& st, ui32 offset, ui32 length) {
            auto it = st.Free.upper_bound(offset);
            if (it != st.Free.begin()) {
                auto prev = std::prev(it);
                if (prev->first + prev->second > offset) {
                    return false; // a hole starting at or before offset reaches into the range
                }
            }
            return it == st.Free.end() || it->first >= offset + length;
        }

        void TStripeHeap::OccupyRange(TChunkState& st, ui32 offset, ui32 length) {
            Y_VERIFY_S(RangeIsOccupied(st, offset, length), VDiskLogPrefix);
            st.UsedBytes += length;
        }

        bool TStripeHeap::FindBestFit(ui32 length, ui32 *chunkId, ui32 *offset) const {
            ui32 bestRemaining = Max<ui32>();
            bool found = false;
            for (auto it = FreeBySize.lower_bound(length); it != FreeBySize.end(); ++it) {
                for (const auto& [id, off] : it->second) {
                    auto cit = Chunks.find(id);
                    Y_VERIFY_S(cit != Chunks.end(), VDiskLogPrefix);
                    const TChunkState& st = cit->second;
                    if (st.Locked || st.Forbidden) {
                        continue;
                    }
                    const ui32 remaining = ChunkSize - st.UsedBytes;
                    if (!found || remaining < bestRemaining ||
                            (remaining == bestRemaining && (id < *chunkId || (id == *chunkId && off < *offset))))
                    {
                        found = true;
                        bestRemaining = remaining;
                        *chunkId = id;
                        *offset = off;
                    }
                }
                if (found) {
                    // smallest hole class that fits; tie-break among holes of this size only
                    return true;
                }
            }
            return false;
        }

        bool TStripeHeap::Allocate(ui32 size, THugeSlot *hugeSlot) {
            const ui32 length = AlignUpLocal(size);
            Y_VERIFY_S(length <= ChunkSize, VDiskLogPrefix << " size# " << size << " length# " << length
                << " ChunkSize# " << ChunkSize);
            ui32 chunkId = 0;
            ui32 offset = 0;
            if (!FindBestFit(length, &chunkId, &offset)) {
                return false;
            }
            auto it = Chunks.find(chunkId);
            Y_VERIFY_S(it != Chunks.end(), VDiskLogPrefix);
            TryAllocateInChunk(it->second, chunkId, offset, length, hugeSlot);
            return true;
        }

        void TStripeHeap::Allocate(ui32 size, THugeSlot *hugeSlot, ui32 chunkId) {
            AddChunk(chunkId);
            const ui32 length = AlignUpLocal(size);
            auto it = Chunks.find(chunkId);
            Y_VERIFY_S(it != Chunks.end(), VDiskLogPrefix << " chunkId# " << chunkId);
            const auto fit = it->second.Free.find(0);
            Y_VERIFY_S(fit != it->second.Free.end() && fit->second >= length,
                VDiskLogPrefix << " chunkId# " << chunkId << " size# " << size);
            TryAllocateInChunk(it->second, chunkId, 0, length, hugeSlot);
        }

        void TStripeHeap::AddChunk(ui32 chunkId) {
            Y_VERIFY_S(chunkId, VDiskLogPrefix);
            Y_VERIFY_S(!Chunks.contains(chunkId), VDiskLogPrefix << " chunkId# " << chunkId);
            ForbiddenChunks.erase(chunkId);
            TChunkState& st = Chunks[chunkId];
            InsertFree(st, chunkId, 0, ChunkSize);
        }

        TFreeRes TStripeHeap::Free(const TDiskPart &addr) {
            return DoFree(addr.ChunkIdx, addr.Offset, AlignUpLocal(addr.Size));
        }

        TFreeRes TStripeHeap::DoFree(ui32 chunkId, ui32 offset, ui32 length) {
            auto cit = Chunks.find(chunkId);
            Y_VERIFY_S(cit != Chunks.end(), VDiskLogPrefix << " chunkId# " << chunkId << " offset# " << offset);
            TChunkState& st = cit->second;
            Y_VERIFY_S(RangeIsOccupied(st, offset, length), VDiskLogPrefix << " chunkId# " << chunkId
                << " offset# " << offset << " length# " << length);
            Y_VERIFY_S(st.UsedBytes >= length, VDiskLogPrefix);
            st.UsedBytes -= length;
            InsertFree(st, chunkId, offset, length);

            TFreeRes res;
            if (!st.UsedBytes) {
                DropEmptyChunk(chunkId, st, &res);
            }
            return res;
        }

        void TStripeHeap::DropEmptyChunk(ui32 chunkId, TChunkState& st, TFreeRes *res) {
            Y_VERIFY_S(st.UsedBytes == 0, VDiskLogPrefix);
            // one free interval covering the whole chunk
            Y_VERIFY_S(st.Free.size() == 1 && st.Free.begin()->first == 0 &&
                st.Free.begin()->second == ChunkSize, VDiskLogPrefix);
            // st does not survive erasing the chunk from the map
            const bool inLocked = st.Locked;
            const bool forbidden = st.Forbidden;
            EraseFree(st, chunkId, 0);
            Chunks.erase(chunkId);

            res->InLockedChunks = inLocked;
            if (ForbiddenChunks.erase(chunkId) || forbidden) {
                ForceFreeChunks.push_back(chunkId);
                res->ChunkId = 0;
            } else {
                res->ChunkId = chunkId;
            }
        }

        ui32 TStripeHeap::RemoveChunk() {
            if (ForceFreeChunks.empty()) {
                return 0;
            }
            const ui32 chunkId = ForceFreeChunks.front();
            ForceFreeChunks.pop_front();
            return chunkId;
        }

        bool TStripeHeap::ForgetChunk(ui32 chunkId) {
            const auto it = Chunks.find(chunkId);
            if (it == Chunks.end()) {
                return false;
            }
            TChunkState& st = it->second;
            Y_VERIFY_S(!st.UsedBytes, VDiskLogPrefix << " chunkId# " << chunkId << " UsedBytes# " << st.UsedBytes);
            for (const auto& [offset, length] : st.Free) {
                IndexErase(length, chunkId, offset);
            }
            Chunks.erase(it);
            ForbiddenChunks.erase(chunkId);
            return true;
        }

        bool TStripeHeap::LockChunk(ui32 chunkId) {
            auto it = Chunks.find(chunkId);
            if (it == Chunks.end()) {
                return false;
            }
            it->second.Locked = true;
            return true;
        }

        THeapStat TStripeHeap::GetStat() const {
            ui32 usedChunks = Chunks.size();
            ui64 liveBytes = 0;
            std::vector<ui32> locked;
            for (const auto& [chunkId, st] : Chunks) {
                liveBytes += st.UsedBytes;
                if (st.Locked) {
                    locked.push_back(chunkId);
                }
            }
            ui32 minChunks = 0;
            if (liveBytes) {
                minChunks = (liveBytes + ChunkSize - 1) / ChunkSize;
            }
            ui32 canBeFreed = usedChunks > minChunks ? usedChunks - minChunks : 0;
            return THeapStat(usedChunks, canBeFreed, std::move(locked));
        }

        void TStripeHeap::ShredNotify(const std::vector<ui32>& chunksToShred) {
            ForbiddenChunks.insert(chunksToShred.begin(), chunksToShred.end());
            for (ui32 chunkId : chunksToShred) {
                auto it = Chunks.find(chunkId);
                if (it == Chunks.end()) {
                    continue;
                }
                it->second.Forbidden = true;
                it->second.Locked = true;
            }
        }

        void TStripeHeap::ListChunks(const THashSet<TChunkIdx>& chunksOfInterest, THashSet<TChunkIdx>& chunks) const {
            for (const auto& [chunkId, _] : Chunks) {
                if (chunksOfInterest.contains(chunkId)) {
                    chunks.insert(chunkId);
                }
            }
        }

        THashSet<TChunkIdx> TStripeHeap::GetForbiddenChunks() const {
            THashSet<TChunkIdx> res = ForbiddenChunks;
            for (const auto& [chunkId, st] : Chunks) {
                if (st.Forbidden) {
                    res.insert(chunkId);
                }
            }
            return res;
        }

        THugeSlot TStripeHeap::ConvertDiskPart(const TDiskPart &addr) const {
            auto cit = Chunks.find(addr.ChunkIdx);
            Y_VERIFY_S(cit != Chunks.end(), VDiskLogPrefix << " addr# " << addr.ToString());
            const ui32 length = AlignUpLocal(addr.Size);
            Y_VERIFY_S(RangeIsOccupied(cit->second, addr.Offset, length), VDiskLogPrefix
                << " addr# " << addr.ToString());
            return THugeSlot(addr.ChunkIdx, addr.Offset, length);
        }

        TFreeRes TStripeHeap::RecoveryModeFree(const TDiskPart &addr) {
            return Free(addr);
        }

        void TStripeHeap::RecoveryModeAllocate(const TDiskPart &addr) {
            const ui32 length = AlignUpLocal(addr.Size);
            auto cit = Chunks.find(addr.ChunkIdx);
            Y_VERIFY_S(cit != Chunks.end(), VDiskLogPrefix << " addr# " << addr.ToString());
            TChunkState& st = cit->second;
            if (RangeIsOccupied(st, addr.Offset, length)) {
                return; // already occupied (in-flight)
            }

            auto fit = st.Free.upper_bound(addr.Offset);
            Y_VERIFY_S(fit != st.Free.begin(), VDiskLogPrefix << " addr# " << addr.ToString());
            --fit;
            Y_VERIFY_S(fit->first <= addr.Offset, VDiskLogPrefix);
            Y_VERIFY_S(fit->first + fit->second >= addr.Offset + length, VDiskLogPrefix
                << " addr# " << addr.ToString() << " hole# " << fit->first << ":" << fit->second);

            const ui32 holeOff = fit->first;
            const ui32 holeLen = fit->second;
            EraseFree(st, addr.ChunkIdx, holeOff);
            if (holeOff < addr.Offset) {
                InsertFree(st, addr.ChunkIdx, holeOff, addr.Offset - holeOff);
            }
            OccupyRange(st, addr.Offset, length);
            const ui32 holeEnd = holeOff + holeLen;
            const ui32 allocEnd = addr.Offset + length;
            if (allocEnd < holeEnd) {
                InsertFree(st, addr.ChunkIdx, allocEnd, holeEnd - allocEnd);
            }
        }

        TFreeRes TStripeHeap::ReleaseStripe(THugeSlot slot) {
            return DoFree(slot.GetChunkId(), slot.GetOffset(), AlignUpLocal(slot.GetSize()));
        }

        void TStripeHeap::ShrinkStripe(THugeSlot slot, ui32 newSize) {
            const ui32 oldLength = AlignUpLocal(slot.GetSize());
            const ui32 newLength = AlignUpLocal(newSize);
            Y_VERIFY_S(newLength <= oldLength, VDiskLogPrefix << " slot# " << slot.ToString()
                << " newSize# " << newSize);
            if (newLength == oldLength) {
                return;
            }
            const ui32 chunkId = slot.GetChunkId();
            auto cit = Chunks.find(chunkId);
            Y_VERIFY_S(cit != Chunks.end(), VDiskLogPrefix << " slot# " << slot.ToString());
            TChunkState& st = cit->second;
            Y_VERIFY_S(RangeIsOccupied(st, slot.GetOffset(), oldLength), VDiskLogPrefix
                << " slot# " << slot.ToString());
            const ui32 tail = oldLength - newLength;
            Y_VERIFY_S(st.UsedBytes >= tail, VDiskLogPrefix);
            st.UsedBytes -= tail;
            InsertFree(st, chunkId, slot.GetOffset() + newLength, tail);
        }

        void TStripeHeap::OccupyStripe(THugeSlot slot, bool inLockedChunks) {
            TDiskPart addr(slot.GetChunkId(), slot.GetOffset(), slot.GetSize());
            if (!ContainsChunk(slot.GetChunkId())) {
                AddChunk(slot.GetChunkId());
            }
            RecoveryModeAllocate(addr);
            if (inLockedChunks) {
                LockChunk(slot.GetChunkId());
            }
        }

        void TStripeHeap::SaveToProto(NKikimrVDiskData::THugeKeeperStripeHeap& proto) const {
            proto.SetChunkSize(ChunkSize);
            proto.SetAppendBlockSize(AppendBlockSize);
            std::vector<ui32> chunkIds;
            chunkIds.reserve(Chunks.size());
            for (const auto& [chunkId, st] : Chunks) {
                chunkIds.push_back(chunkId);
            }
            std::sort(chunkIds.begin(), chunkIds.end());
            for (ui32 chunkId : chunkIds) {
                proto.AddChunkIds(chunkId);
            }
        }

        void TStripeHeap::LoadFromProto(const NKikimrVDiskData::THugeKeeperStripeHeap& proto) {
            Chunks.clear();
            FreeBySize.clear();
            ForbiddenChunks.clear();
            ForceFreeChunks.clear();

            ChunkSize = proto.GetChunkSize();
            AppendBlockSize = proto.GetAppendBlockSize();
            Y_VERIFY_S(AppendBlockSize > 0, VDiskLogPrefix);
            Y_VERIFY_S(ChunkSize % AppendBlockSize == 0, VDiskLogPrefix);

            // Chunks come back empty; RecoveryOccupyDerived fills them in from the hull's references.
            for (ui32 chunkId : proto.GetChunkIds()) {
                AddChunk(chunkId);
            }
        }

        void TStripeHeap::RecoveryOccupyDerived(const TDiskPart &addr) {
            Y_VERIFY_S(Chunks.contains(addr.ChunkIdx), VDiskLogPrefix << " addr# " << addr.ToString()
                << " refers to a chunk the stripe heap does not own");
            RecoveryModeAllocate(addr);
        }

        std::vector<ui32> TStripeHeap::DropUnreferencedChunks() {
            std::vector<ui32> empty;
            for (const auto& [chunkId, st] : Chunks) {
                if (!st.UsedBytes) {
                    empty.push_back(chunkId);
                }
            }
            std::sort(empty.begin(), empty.end());

            std::vector<ui32> res;
            for (ui32 chunkId : empty) {
                const auto it = Chunks.find(chunkId);
                Y_VERIFY_S(it != Chunks.end(), VDiskLogPrefix << " chunkId# " << chunkId);
                TChunkState& st = it->second;
                // st does not survive erasing the chunk from the map
                const bool forbidden = st.Forbidden;
                EraseFree(st, chunkId, 0);
                Chunks.erase(it);
                if (ForbiddenChunks.erase(chunkId) || forbidden) {
                    ForceFreeChunks.push_back(chunkId);
                } else {
                    res.push_back(chunkId);
                }
            }
            return res;
        }

        void TStripeHeap::GetOwnedChunks(TSet<TChunkIdx>& chunks) const {
            for (const auto& [chunkId, _] : Chunks) {
                // May already be listed by an SST that lives on this heap chunk.
                chunks.insert(chunkId);
            }
        }

        void TStripeHeap::CollectChunkIds(THashSet<ui32>& chunks) const {
            for (const auto& [chunkId, _] : Chunks) {
                chunks.insert(chunkId);
            }
        }

        void TStripeHeap::RenderHtml(IOutputStream &str) const {
            HTML(str) {
                COLLAPSED_BUTTON_CONTENT("stripeheapstateid", "Stripe Heap") {
                    str << "Chunks: " << Chunks.size()
                        << " Stat: " << GetStat().ToString() << "<br/>";
                    TABLE_CLASS("table table-condensed") {
                        TABLEHEAD() {
                            TABLER() {
                                TABLEH() { str << "ChunkId"; }
                                TABLEH() { str << "UsedBytes"; }
                                TABLEH() { str << "FreeHoles"; }
                                TABLEH() { str << "Locked"; }
                                TABLEH() { str << "Forbidden"; }
                            }
                        }
                        TABLEBODY() {
                            for (const auto& [chunkId, st] : Chunks) {
                                TABLER() {
                                    TABLED() { str << chunkId; }
                                    TABLED() { str << st.UsedBytes; }
                                    TABLED() { str << st.Free.size(); }
                                    TABLED() { str << st.Locked; }
                                    TABLED() { str << st.Forbidden; }
                                }
                            }
                        }
                    }
                }
            }
        }

        TString TStripeHeap::ToString() const {
            TStringStream str;
            str << "{StripeHeap Chunks# " << Chunks.size()
                << " " << GetStat().ToString() << "}";
            return str.Str();
        }

    } // NHuge
} // NKikimr
