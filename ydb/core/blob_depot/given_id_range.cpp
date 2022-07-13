#include "blob_depot_tablet.h"

namespace NKikimr::NBlobDepot {

    void TGivenIdRange::IssueNewRange(ui64 begin, ui64 end) {
        Y_VERIFY(begin < end);

        const auto [it, inserted] = Ranges.emplace(begin, end);
        Y_VERIFY(inserted);

        if (it != Ranges.begin()) {
            const auto& prev = *std::prev(it);
            Y_VERIFY(prev.End <= begin);
        }
        if (std::next(it) != Ranges.end()) {
            const auto& next = *std::next(it);
            Y_VERIFY(end <= next.Begin);
        }

        NumAvailableItems += end - begin;
    }

    void TGivenIdRange::AddPoint(ui64 value) {
        IssueNewRange(value, value + 1);
    }

    void TGivenIdRange::RemovePoint(ui64 value) {
        auto it = Ranges.upper_bound(value);
        Y_VERIFY(it != Ranges.begin());
        Pop(std::prev(it), value);
    }

    bool TGivenIdRange::IsEmpty() const {
        return Ranges.empty();
    }
    
    ui32 TGivenIdRange::GetNumAvailableItems() const {
        return NumAvailableItems;
    }

    ui64 TGivenIdRange::GetMinimumValue() const {
        Y_VERIFY(!Ranges.empty());
        const auto& range = *Ranges.begin();
        Y_VERIFY(range.NumSetBits);
        size_t offset = range.Bits.FirstNonZeroBit();
        Y_VERIFY(offset != range.Bits.Size());
        return range.Begin + offset;
    }

    ui64 TGivenIdRange::Allocate() {
        const ui64 value = GetMinimumValue();
        Pop(Ranges.begin(), value);
        return value;
    }

    void TGivenIdRange::Trim(ui8 channel, ui32 generation, ui32 invalidatedStep) {
        const ui64 validSince = 1 + TBlobSeqId{channel, generation, invalidatedStep, TBlobSeqId::MaxIndex}.ToSequentialNumber();

        while (!Ranges.empty()) {
            const auto it = Ranges.begin();
            auto& range = const_cast<TRange&>(*it);
            if (range.End <= validSince) {
                NumAvailableItems -= range.Bits.Count();
                Ranges.erase(it);
            } else if (range.Begin < validSince) {
                const ui32 len = validSince - range.Begin;
                for (ui32 i = 0; i < len; ++i) {
                    range.NumSetBits -= range.Bits[i];
                    NumAvailableItems -= range.Bits[i];
                }
                range.Bits.Reset(0, len);
            } else {
                break;
            }
        }
    }

    void TGivenIdRange::Subtract(const TGivenIdRange& other) {
        for (const TRange& range : other.Ranges) {
            const auto it = Ranges.find(range.Begin);
            Y_VERIFY(it != Ranges.end());
            Y_VERIFY(range.End == it->End);
            Y_VERIFY(range.NumSetBits == it->NumSetBits);
            Y_VERIFY(range.Bits == it->Bits);
            NumAvailableItems -= range.NumSetBits;
            Ranges.erase(it);
        }
    }

    void TGivenIdRange::Output(IOutputStream& s) const {
        s << "{";
        for (auto it = Ranges.begin(); it != Ranges.end(); ++it) {
            if (it != Ranges.begin()) {
                s << " ";
            }
            s << "[" << it->Begin << "," << it->End << "):";
            for (ui32 i = 0, count = it->End - it->Begin; i < count; ++i) {
                s << int(it->Bits[i]);
            }
        }
        s << "}";
    }

    TString TGivenIdRange::ToString() const {
        TStringStream s;
        Output(s);
        return s.Str();
    }

    void TGivenIdRange::Pop(TRanges::iterator it, ui64 value) {
        TRange& range = const_cast<TRange&>(*it);
        Y_VERIFY(range.Begin <= value && value < range.End);
        const size_t offset = value - range.Begin;
        Y_VERIFY(range.Bits[offset]);
        range.Bits.Reset(offset);
        --range.NumSetBits;
        if (!range.NumSetBits) {
            Ranges.erase(it);
        }
        --NumAvailableItems;
    }

} // NKikimr::NBlobDepot
