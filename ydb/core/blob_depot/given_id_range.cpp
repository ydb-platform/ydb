#include "blob_depot_tablet.h"

namespace NKikimr::NBlobDepot {

    namespace {
        TDynBitMap Compact(TDynBitMap value) {
            const size_t valueBitCount = value.ValueBitCount();
            TDynBitMap result;
            result.Reserve(valueBitCount);
            Y_VERIFY(result.GetChunkCount() <= value.GetChunkCount());
            Y_VERIFY_DEBUG(result.GetChunkCount() < value.GetChunkCount());
            const size_t numBytes = sizeof(TDynBitMap::TChunk) * result.GetChunkCount();
            memcpy(const_cast<TDynBitMap::TChunk*>(result.GetChunks()), value.GetChunks(), numBytes);
            return result;
        }
    }

    void TGivenIdRange::IssueNewRange(ui64 begin, ui64 end) {
        Y_VERIFY(begin < end);
        NumAvailableItems += end - begin;

        const auto it = Ranges.upper_bound(begin);
        bool adjacentWithNext = false, adjacentWithPrev = false;

        if (it != Ranges.end()) {
            TRange& next = const_cast<TRange&>(*it);
            if (next.Begin < end) {
                Y_VERIFY(next.Bits.FirstNonZeroBit() >= end - next.Begin);
                next.Bits.Set(0, end - next.Begin);
                next.NumSetBits += end - next.Begin;
                end = next.Begin;
            }
            Y_VERIFY(end <= next.Begin);
            adjacentWithNext = end == next.Begin;
        }

        if (it != Ranges.begin()) {
            TRange& prev = const_cast<TRange&>(*std::prev(it));
            if (prev.End <= begin) {
                // nothing interesting here -- no any kind overlap
            } else if (end <= prev.End) {
                // the [begin, end) pair is just a subrange of 'prev' item
                Y_VERIFY(prev.Begin <= begin);
                Y_VERIFY(!adjacentWithNext);
                prev.Bits.Set(begin - prev.Begin, end - prev.Begin);
                prev.NumSetBits += end - begin;
                return;
            } else {
                Y_VERIFY(prev.Begin < begin);
                prev.Bits.Set(begin - prev.Begin, prev.End - prev.Begin);
                prev.NumSetBits += prev.End - begin;
                begin = prev.End;
            }
            Y_VERIFY_DEBUG(prev.End <= begin);
            adjacentWithPrev = prev.End == begin;
        }

        if (adjacentWithNext && adjacentWithPrev) {
            // [ prev )     [ next )
            //        ^     ^
            //      begin  end
            TRange& next = const_cast<TRange&>(*it);
            TRange& prev = const_cast<TRange&>(*std::prev(it));
            prev.Bits |= next.Bits << end - prev.Begin;
            prev.Bits.Set(begin - prev.Begin, end - prev.Begin);
            prev.NumSetBits += next.NumSetBits + end - begin;
            prev.End = next.End;
            Ranges.erase(it);
        } else if (adjacentWithNext) {
            TRange& next = const_cast<TRange&>(*it);
            const ui32 shift = end - begin;
            next.Bits <<= shift;
            next.Bits.Set(0, shift);
            next.NumSetBits += shift;
            next.Begin = begin;
        } else if (adjacentWithPrev) {
            TRange& prev = const_cast<TRange&>(*std::prev(it));
            prev.Bits.Set(prev.End - prev.Begin, end - prev.Begin);
            prev.NumSetBits += end - begin;
            prev.End = end;
        } else {
            Ranges.emplace_hint(it, begin, end);
        }
    }

    void TGivenIdRange::AddPoint(ui64 value) {
        IssueNewRange(value, value + 1);
    }

    void TGivenIdRange::RemovePoint(ui64 value, bool *wasLeast) {
        auto it = Ranges.upper_bound(value);
        Y_VERIFY(it != Ranges.begin());
        --it;
        if (wasLeast) {
            *wasLeast = it == Ranges.begin() && it->Bits.FirstNonZeroBit() + it->Begin == value;
        }
        Pop(it, value);
    }

    bool TGivenIdRange::GetPoint(ui64 value) const {
        const auto it = Ranges.upper_bound(value);
        if (it == Ranges.begin()) {
            return false;
        }
        const TRange& range = *std::prev(it);
        return range.Begin <= value && value < range.End && range.Bits[value - range.Begin];
    }

    bool TGivenIdRange::IsEmpty() const {
        return Ranges.empty();
    }
    
    ui32 TGivenIdRange::GetNumAvailableItems() const {
#ifndef NDEBUG
        ui32 count = 0;
        for (const auto& range : Ranges) {
            Y_VERIFY(range.NumSetBits == range.Bits.Count());
            count += range.NumSetBits;
        }
        Y_VERIFY(count == NumAvailableItems);
#endif
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

    TGivenIdRange TGivenIdRange::Trim(ui64 validSince) {
        TGivenIdRange result;

        while (!Ranges.empty()) {
            auto it = Ranges.begin();
            auto& range = const_cast<TRange&>(*it);
            if (range.End <= validSince) {
                NumAvailableItems -= range.NumSetBits;
                result.NumAvailableItems += range.NumSetBits;
                result.Ranges.insert(result.Ranges.end(), Ranges.extract(it++));
                continue;
            } else if (range.Begin < validSince) {
                const ui32 len = validSince - range.Begin;
                TDynBitMap mask;
                mask.Set(0, len);

                const TDynBitMap setBits = range.Bits & mask;
                if (const size_t numSetBits = setBits.Count()) {
                    // clear set bits in the origin chunk
                    range.Bits -= setBits;
                    range.NumSetBits -= numSetBits;
                    NumAvailableItems -= numSetBits;
                    if (!range.NumSetBits) {
                        Ranges.erase(it);
                    }

                    // put 'em into the result
                    TRange& r = const_cast<TRange&>(*result.Ranges.emplace_hint(result.Ranges.end(), range.Begin, range.End, TRange::Zero));
                    r.Bits |= setBits;
                    r.NumSetBits += numSetBits;
                    result.NumAvailableItems += numSetBits;
                }
            }
            break;
        }

        return result;
    }

    void TGivenIdRange::Subtract(const TGivenIdRange& other) {
        auto it = Ranges.begin();
        auto otherIt = other.Ranges.begin();

        while (it != Ranges.end()) {
            if (it->End <= otherIt->Begin) {
                ++it;
            } else if (otherIt->End <= it->Begin) {
                Y_FAIL();
            } else {
                const ui64 begin = Max(it->Begin, otherIt->Begin);
                const ui64 end = Min(it->End, otherIt->End);
                Y_VERIFY(begin < end);

                TDynBitMap subtractedMask = otherIt->Bits;
                if (otherIt->Begin < begin) {
                    subtractedMask >>= begin - otherIt->Begin;
                }
                subtractedMask.Reset(end - begin, subtractedMask.Size());
                if (it->Begin < begin) {
                    subtractedMask <<= begin - it->Begin;
                }
                Y_VERIFY_DEBUG((it->Bits & subtractedMask) == subtractedMask);

                TRange& r = const_cast<TRange&>(*it);
                r.Bits -= subtractedMask;
                r.NumSetBits -= subtractedMask.Count();
                if (!r.NumSetBits) {
                    it = Ranges.erase(it);
                } else if (it->End == end) {
                    ++it;
                }

                if (otherIt->End == end) {
                    ++otherIt;
                }

                NumAvailableItems -= subtractedMask.Count();
            }
        }

        // ensure we have processed all other range
        Y_VERIFY(otherIt == other.Ranges.end());
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
        static constexpr size_t bitsPerChunk = CHAR_BIT * sizeof(TDynBitMap::TChunk);

        TRange& range = const_cast<TRange&>(*it);
        Y_VERIFY_S(range.Begin <= value && value < range.End, "value# " << value << " this# " << ToString());
        const size_t offset = value - range.Begin;
        Y_VERIFY(range.Bits[offset]);
        range.Bits.Reset(offset);
        --range.NumSetBits;
        if (!range.NumSetBits) {
            Ranges.erase(it);
        } else if (const size_t index = range.Bits.FirstNonZeroBit(); index >= 2 * bitsPerChunk) {
            // make its representation more compact
            range.Bits >>= index;
            range.Bits = Compact(range.Bits);
            range.Begin += index;
        }
        --NumAvailableItems;
    }

    std::vector<bool> TGivenIdRange::ToDebugArray(size_t numItems) const {
        std::vector<bool> res(numItems, false);
        for (const TRange& item : Ranges) {
            Y_FOR_EACH_BIT(i, item.Bits) {
                Y_VERIFY_S(i < item.End - item.Begin, "i# " << i << " this# " << ToString());
                res[item.Begin + i] = true;
            }
        }
        return res;
    }

    void TGivenIdRange::CheckConsistency() const {
        ui32 numAvailableItems = 0;

        for (auto it = Ranges.begin(); it != Ranges.end(); ++it) {
            const TRange& range = *it;
            Y_VERIFY_S(range.Begin < range.End, ToString());

            if (it != Ranges.begin()) {
                const TRange& prev = *std::prev(it);
                Y_VERIFY_S(prev.End < range.Begin, ToString());
            }

            Y_VERIFY_S(range.NumSetBits, ToString());
            Y_VERIFY_S(range.NumSetBits == range.Bits.Count(), ToString());
            for (ui32 i = range.End - range.Begin; i < range.Bits.Size(); ++i) {
                Y_VERIFY_S(!range.Bits[i], "begin# " << range.Begin << " i# " << i << " this# " << ToString());
            }

            numAvailableItems += range.NumSetBits;
        }

        Y_VERIFY_S(numAvailableItems == NumAvailableItems, ToString());
    }

} // NKikimr::NBlobDepot
