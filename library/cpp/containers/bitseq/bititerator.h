#pragma once

#include "traits.h"

#include <library/cpp/pop_count/popcount.h>

template <typename T>
class TBitIterator {
public:
    using TWord = T;
    using TTraits = TBitSeqTraits<TWord>;

public:
    TBitIterator(const T* data = nullptr)
        : Current(0)
        , Mask(0)
        , Data(data)
    {
    }

    /// Get the word next to the one we are currenlty iterating over.
    const TWord* NextWord() const {
        return Data;
    }

    /// Get the next bit without moving the iterator.
    bool Peek() const {
        return Mask ? (Current & Mask) : (*Data & 1);
    }

    /// Get the next bit and move forward.
    /// TODO: Implement inversed iteration as well.
    bool Next() {
        if (!Mask) {
            Current = *Data++;
            Mask = 1;
        }
        const bool bit = Current & Mask;
        Mask <<= 1;
        return bit;
    }

    /// Get the next count bits without moving the iterator.
    TWord Peek(ui8 count) const {
        if (!count)
            return 0;
        Y_DEBUG_ABORT_UNLESS(count <= TTraits::NumBits);

        if (!Mask)
            return *Data & TTraits::ElemMask(count);

        auto usedBits = (size_t)PopCount(Mask - 1);
        TWord result = Current >> usedBits;
        auto leftInCurrent = TTraits::NumBits - usedBits;
        if (count <= leftInCurrent)
            return result & TTraits::ElemMask(count);

        count -= leftInCurrent;
        result |= (*Data & TTraits::ElemMask(count)) << leftInCurrent;
        return result;
    }

    /// Get the next count bits and move forward by count bits.
    TWord Read(ui8 count) {
        if (!count)
            return 0;
        Y_DEBUG_ABORT_UNLESS(count <= TTraits::NumBits);

        if (!Mask) {
            Current = *Data++;
            Mask = 1 << count;
            return Current & TTraits::ElemMask(count);
        }

        auto usedBits = (size_t)PopCount(Mask - 1);
        TWord result = Current >> usedBits;
        auto leftInCurrent = TTraits::NumBits - usedBits;
        if (count < leftInCurrent) {
            Mask <<= count;
            return result & TTraits::ElemMask(count);
        }

        count -= leftInCurrent;
        if (count) {
            Current = *Data++;
            Mask = 1 << count;
            result |= (Current & TTraits::ElemMask(count)) << leftInCurrent;
        } else {
            Mask = 0;
        }

        return result;
    }

    /// Move the iterator forward by count bits.
    void Forward(int count) {
        if (!count)
            return;

        int leftInCurrent = (size_t)PopCount(~(Mask - 1));
        if (count < leftInCurrent) {
            Mask <<= count;
            return;
        }

        count -= leftInCurrent;
        Data += count >> TTraits::DivShift;
        auto remainder = count & TTraits::ModMask;

        if (remainder) {
            Current = *Data++;
            Mask = 1 << remainder;
        } else {
            Current = 0;
            Mask = 0;
        }
    }

    /// Skip trailing bits of the current word and move by count words forward.
    void Align(int count = 0) {
        Current = 0;
        if (Mask)
            Mask = 0;
        Data += count;
    }

    /// Initialize the iterator.
    void Reset(const TWord* data) {
        Current = 0;
        Mask = 0;
        Data = data;
    }

private:
    TWord Current;
    TWord Mask;
    const TWord* Data;
};
