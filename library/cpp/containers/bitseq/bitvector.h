#pragma once

#include "traits.h"

#include <library/cpp/pop_count/popcount.h>

#include <util/generic/vector.h>
#include <util/ysaveload.h>

template <typename T>
class TReadonlyBitVector;

template <typename T>
class TBitVector {
public:
    using TWord = T;
    using TTraits = TBitSeqTraits<TWord>;

private:
    friend class TReadonlyBitVector<T>;
    ui64 Size_;
    TVector<TWord> Data_;

public:
    TBitVector()
        : Size_(0)
        , Data_(0)
    {
    }

    TBitVector(ui64 size)
        : Size_(size)
        , Data_(static_cast<size_t>((Size_ + TTraits::ModMask) >> TTraits::DivShift), 0)
    {
    }

    virtual ~TBitVector() = default;

    void Clear() {
        Size_ = 0;
        Data_.clear();
    }

    void Resize(ui64 size) {
        Size_ = size;
        Data_.resize((Size_ + TTraits::ModMask) >> TTraits::DivShift);
    }

    void Swap(TBitVector& other) {
        DoSwap(Size_, other.Size_);
        DoSwap(Data_, other.Data_);
    }

    bool Set(ui64 pos) {
        Y_ASSERT(pos < Size_);
        TWord& val = Data_[pos >> TTraits::DivShift];
        if (val & TTraits::BitMask(pos & TTraits::ModMask))
            return false;
        val |= TTraits::BitMask(pos & TTraits::ModMask);
        return true;
    }

    bool Test(ui64 pos) const {
        return TTraits::Test(Data(), pos, Size_);
    }

    void Reset(ui64 pos) {
        Y_ASSERT(pos < Size_);
        Data_[pos >> TTraits::DivShift] &= ~TTraits::BitMask(pos & TTraits::ModMask);
    }

    TWord Get(ui64 pos, ui8 width, TWord mask) const {
        return TTraits::Get(Data(), pos, width, mask, Size_);
    }

    TWord Get(ui64 pos, ui8 width) const {
        return Get(pos, width, TTraits::ElemMask(width));
    }

    void Set(ui64 pos, TWord value, ui8 width, TWord mask) {
        if (!width)
            return;
        Y_ASSERT((pos + width) <= Size_);
        size_t word = pos >> TTraits::DivShift;
        TWord shift1 = pos & TTraits::ModMask;
        TWord shift2 = TTraits::NumBits - shift1;
        Data_[word] &= ~(mask << shift1);
        Data_[word] |= (value & mask) << shift1;
        if (shift2 < width) {
            Data_[word + 1] &= ~(mask >> shift2);
            Data_[word + 1] |= (value & mask) >> shift2;
        }
    }

    void Set(ui64 pos, TWord value, ui8 width) {
        Set(pos, value, width, TTraits::ElemMask(width));
    }

    void Append(TWord value, ui8 width, TWord mask) {
        if (!width)
            return;
        if (Data_.size() * TTraits::NumBits < Size_ + width) {
            Data_.push_back(0);
        }
        Size_ += width;
        Set(Size_ - width, value, width, mask);
    }

    void Append(TWord value, ui8 width) {
        Append(value, width, TTraits::ElemMask(width));
    }

    size_t Count() const {
        size_t count = 0;
        for (size_t i = 0; i < Data_.size(); ++i) {
            count += (size_t)PopCount(Data_[i]);
        }
        return count;
    }

    ui64 Size() const {
        return Size_;
    }

    size_t Words() const {
        return Data_.size();
    }

    const TWord* Data() const {
        return Data_.data();
    }

    void Save(IOutputStream* out) const {
        ::Save(out, Size_);
        ::Save(out, Data_);
    }

    void Load(IInputStream* inp) {
        ::Load(inp, Size_);
        ::Load(inp, Data_);
    }

    ui64 Space() const {
        return CHAR_BIT * (sizeof(Size_) +
                           Data_.size() * sizeof(TWord));
    }

    void Print(IOutputStream& out, size_t truncate = 128) {
        for (size_t i = 0; i < Data_.size() && i < truncate; ++i) {
            for (int j = TTraits::NumBits - 1; j >= 0; --j) {
                size_t pos = TTraits::NumBits * i + j;
                out << (pos < Size_ && Test(pos) ? '1' : '0');
            }
            out << " ";
        }
        out << Endl;
    }
};
