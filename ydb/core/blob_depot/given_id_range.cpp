#include "blob_depot_tablet.h"

namespace NKikimr::NBlobDepot {

    TGivenIdRange::TGivenIdRange(const NKikimrBlobDepot::TGivenIdRange& proto) {
        for (const auto& range : proto.GetRanges()) {
            const ui64 begin = range.GetBegin();
            const ui32 len = range.GetLen();
            TRange& r = InsertNewRange(begin, begin + len);
            ui64 *chunks = const_cast<ui64*>(r.Bits.GetChunks());
            const ui32 count = r.Bits.GetChunkCount();
            Y_VERIFY(range.OffsetsSize() == range.BitMasksSize());
            for (ui32 i = 0; i < range.OffsetsSize(); ++i) {
                const ui32 offset = range.GetOffsets(i);
                const ui64 bitMask = range.GetBitMasks(i);
                NumAvailableItems += PopCount(bitMask) - 64;
                Y_VERIFY(offset < count);
                chunks[offset] = bitMask;
            }
        }
    }

    TGivenIdRange::TRange& TGivenIdRange::InsertNewRange(ui64 begin, ui64 end) {
        const ui64 len = end - begin;
        Y_VERIFY(len);
        const auto it = Ranges.lower_bound(begin);
        if (it != Ranges.begin()) {
            const auto& [prevBegin, prev] = *std::prev(it);
            Y_VERIFY(prevBegin + prev.Len <= begin);
        }
        if (it != Ranges.end()) {
            Y_VERIFY(end <= it->first);
        }
        NumAvailableItems += len;
        const auto newIt = Ranges.emplace_hint(it, begin, len);
        return newIt->second;
    }

    void TGivenIdRange::IssueNewRange(ui64 begin, ui64 end) {
        InsertNewRange(begin, end);
    }

    void TGivenIdRange::Join(TGivenIdRange&& other) {
        Ranges.merge(std::move(other.Ranges));
        if (Ranges.size() > 1) {
            for (auto it = std::next(Ranges.begin()); it != Ranges.end(); ++it) {
                const auto& [prevBegin, prev] = *std::prev(it);
                const auto& [begin, range] = *it;
                Y_VERIFY(prevBegin + prev.Len <= begin);
            }
        }
        NumAvailableItems += std::exchange(other.NumAvailableItems, 0);
    }

    void TGivenIdRange::ToProto(NKikimrBlobDepot::TGivenIdRange *proto) {
        for (const auto& [begin, range] : Ranges) {
            auto *r = proto->AddRanges();
            r->SetBegin(begin);
            r->SetLen(range.Len);
            const ui64 *chunks = range.Bits.GetChunks();
            const ui32 count = range.Bits.GetChunkCount();
            for (ui32 i = 0; i < count; ++i) {
                if (chunks[i] != ~ui64(0)) {
                    r->AddOffsets(i);
                    r->AddBitMasks(chunks[i]);
                }
            }
        }
    }

    void TGivenIdRange::RemovePoint(ui64 value) {
        auto it = Ranges.upper_bound(value);
        Y_VERIFY(it != Ranges.begin());
        auto& [begin, range] = *--it;
        Y_VERIFY(begin <= value);
        const ui64 offset = value - begin;
        Y_VERIFY(offset < range.Len);
        Y_VERIFY(range.Bits[offset]);
        range.Bits.Reset(offset);
        --NumAvailableItems;
        // FIXME: reduce range (maybe)?
    }

    void TGivenIdRange::Output(IOutputStream& s) const {
        s << "{";
        for (auto it = Ranges.begin(); it != Ranges.end(); ++it) {
            if (it != Ranges.begin()) {
                s << " ";
            }
            const auto& [begin, range] = *it;
            s << begin << ":" << range.Len << "[";
            for (ui32 i = 0; i < range.Len; ++i) {
                s << static_cast<int>(range.Bits[i]);
            }
            s << "]";
        }
    }

    TString TGivenIdRange::ToString() const {
        TStringStream s;
        Output(s);
        return s.Str();
    }

    bool TGivenIdRange::IsEmpty() const {
        return Ranges.empty();
    }

    ui32 TGivenIdRange::GetNumAvailableItems() const {
        return NumAvailableItems;
    }

    ui64 TGivenIdRange::Allocate() {
        Y_VERIFY(!Ranges.empty());
        const auto it = Ranges.begin();
        auto& [begin, range] = *it;
        const size_t offset = range.Bits.FirstNonZeroBit();
        const ui64 res = begin + offset;
        range.Bits.Reset(offset);
        if (range.Bits.NextNonZeroBit(offset) == range.Bits.Size()) {
            Ranges.erase(it);
        }
        --NumAvailableItems;
        return res;
    }

} // NKikimr::NBlobDepot
