#include "blob_depot_tablet.h"

namespace NKikimr::NBlobDepot {

    void TGivenIdRange::IssueNewRange(ui64 begin, ui64 end) {
        Y_ABORT_UNLESS(begin < end);
        NumAvailableItems += end - begin;

        // obtain insertion point
        auto it = Ranges.lower_bound(begin / BitsPerChunk);

        while (begin < end) {
            const ui64 key = begin / BitsPerChunk;
            const size_t offset = begin % BitsPerChunk;
            const size_t count = Min(end - begin, BitsPerChunk - offset);

            if (it == Ranges.end() || it->first != key) {
                Y_DEBUG_ABORT_UNLESS(it == Ranges.end() || key < it->first);
                it = Ranges.emplace_hint(it, key, TChunk());
            }

            TChunk& chunk = it->second;
            Y_DEBUG_ABORT_UNLESS((chunk >> offset).FirstNonZeroBit() >= count);
            chunk.Set(offset, offset + count);

            ++it;
            begin += count;
        }
    }

    void TGivenIdRange::AddPoint(ui64 value) {
        const ui64 key = value / BitsPerChunk;
        TChunk& chunk = Ranges[key];
        const size_t offset = value % BitsPerChunk;
        Y_DEBUG_ABORT_UNLESS(!chunk[offset]);
        chunk.Set(offset);
        ++NumAvailableItems;
    }

    void TGivenIdRange::RemovePoint(ui64 value) {
        const ui64 key = value / BitsPerChunk;
        const auto it = Ranges.find(key);
        Y_ABORT_UNLESS(it != Ranges.end());
        TChunk& chunk = it->second;
        const size_t offset = value % BitsPerChunk;
        Y_DEBUG_ABORT_UNLESS(chunk[offset]);
        chunk.Reset(offset);
        --NumAvailableItems;
        if (chunk.Empty()) {
            Ranges.erase(it);
        }
    }

    bool TGivenIdRange::GetPoint(ui64 value) const {
        const ui64 key = value / BitsPerChunk;
        const auto it = Ranges.find(key);
        return it == Ranges.end() ? false : it->second[value % BitsPerChunk];
    }

    bool TGivenIdRange::IsEmpty() const {
        return Ranges.empty();
    }
    
    ui32 TGivenIdRange::GetNumAvailableItems() const {
        return NumAvailableItems;
    }

    ui64 TGivenIdRange::GetMinimumValue() const {
        const auto it = Ranges.begin();
        Y_ABORT_UNLESS(it != Ranges.end());
        const TChunk& chunk = it->second;
        return chunk.FirstNonZeroBit() + it->first * BitsPerChunk;
    }

    ui64 TGivenIdRange::Allocate() {
        const auto it = Ranges.begin();
        Y_ABORT_UNLESS(it != Ranges.end());
        TChunk& chunk = it->second;
        const size_t offset = chunk.FirstNonZeroBit();
        const ui64 value = offset + it->first * BitsPerChunk;
        chunk.Reset(offset);
        --NumAvailableItems;
        if (chunk.Empty()) {
            Ranges.erase(it);
        }
        return value;
    }

    TGivenIdRange TGivenIdRange::Trim(ui64 validSince) {
        TGivenIdRange result;

        auto it = Ranges.begin();
        while (it != Ranges.end()) {
            TChunk& chunk = it->second;

            if (validSince <= it->first * BitsPerChunk) {
                break;
            } else if (validSince < (it->first + 1) * BitsPerChunk) {
                const size_t numBits = validSince - it->first * BitsPerChunk;
                Y_DEBUG_ABORT_UNLESS(0 < numBits && numBits < BitsPerChunk);

                TChunk mask;
                mask.Set(0, numBits);

                const TChunk cut = chunk & mask;
                chunk -= mask;

                if (!cut.Empty()) {
                    result.Ranges.emplace_hint(result.Ranges.end(), it->first, cut);
                }
                if (chunk.Empty()) {
                    Ranges.erase(it);
                }

                const size_t count = cut.Count();
                NumAvailableItems -= count;
                result.NumAvailableItems += count;
                break;
            } else {
                const size_t count = chunk.Count();
                result.Ranges.insert(result.Ranges.end(), Ranges.extract(it++));
                NumAvailableItems -= count;
                result.NumAvailableItems += count;
            }
        }

        return result;
    }

    void TGivenIdRange::Subtract(const TGivenIdRange& other) {
        auto myIt = Ranges.begin();
        auto otherIt = other.Ranges.begin();

        while (myIt != Ranges.end() && otherIt != other.Ranges.end()) {
            if (myIt->first < otherIt->first) {
                ++myIt;
            } else if (otherIt->first < myIt->first) {
                Y_ABORT();
            } else {
                TChunk& myChunk = myIt->second;
                const TChunk& otherChunk = otherIt->second;
                Y_DEBUG_ABORT_UNLESS((myChunk & otherChunk) == otherChunk);
                myChunk -= otherChunk;
                NumAvailableItems -= otherChunk.Count();

                if (myChunk.Count()) {
                    ++myIt;
                } else {
                    myIt = Ranges.erase(myIt);
                }

                ++otherIt;
            }
        }

        Y_ABORT_UNLESS(otherIt == other.Ranges.end());
    }

    void TGivenIdRange::Output(IOutputStream& s) const {
        s << "{";
        for (auto it = Ranges.begin(); it != Ranges.end(); ++it) {
            const auto& [key, chunk] = *it;
            if (it != Ranges.begin()) {
                s << " ";
            }
            s << key << ":";
            for (ui32 i = 0; i < chunk.Size(); ++i) {
                s << int(chunk[i]);
            }
        }
        s << "}";
    }

    TString TGivenIdRange::ToString() const {
        TStringStream s;
        Output(s);
        return s.Str();
    }

    std::vector<bool> TGivenIdRange::ToDebugArray(size_t numItems) const {
        std::vector<bool> res(numItems, false);
        for (const auto& [key, chunk] : Ranges) {
            const ui64 base = key * BitsPerChunk;
            Y_FOR_EACH_BIT(i, chunk) {
                res[base + i] = true;
            }
        }
        return res;
    }

    void TGivenIdRange::CheckConsistency() const {
        ui32 count = 0;
        for (const auto& [key, chunk] : Ranges) {
            Y_ABORT_UNLESS(!chunk.Empty());
            count += chunk.Count();
        }
        Y_ABORT_UNLESS(count == NumAvailableItems);
    }

} // NKikimr::NBlobDepot
