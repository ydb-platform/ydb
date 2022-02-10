#pragma once

#include "bitvector.h"
#include "traits.h"

#include <util/memory/blob.h>

#include <cstring>

template <typename T>
class TReadonlyBitVector {
public:
    using TWord = T;
    using TTraits = TBitSeqTraits<TWord>;

    TReadonlyBitVector()
        : Size_()
        , Data_()
    {
    }

    explicit TReadonlyBitVector(const TBitVector<T>& vector)
        : Size_(vector.Size_)
        , Data_(vector.Data_.data())
    {
    }

    bool Test(ui64 pos) const {
        return TTraits::Test(Data_, pos, Size_);
    }

    TWord Get(ui64 pos, ui8 width, TWord mask) const {
        return TTraits::Get(Data_, pos, width, mask, Size_);
    }

    TWord Get(ui64 pos, ui8 width) const {
        return Get(pos, width, TTraits::ElemMask(width));
    }

    ui64 Size() const {
        return Size_;
    }

    const T* Data() const {
        return Data_;
    }

    static void SaveForReadonlyAccess(IOutputStream* out, const TBitVector<T>& bv) {
        ::Save(out, bv.Size_);
        ::Save(out, static_cast<ui64>(bv.Data_.size()));
        ::SavePodArray(out, bv.Data_.data(), bv.Data_.size());
    }

    virtual TBlob LoadFromBlob(const TBlob& blob) {
        size_t read = 0;
        auto cursor = [&]() { return blob.AsUnsignedCharPtr() + read; };
        auto readToPtr = [&](auto* ptr) {
            memcpy(ptr, cursor(), sizeof(*ptr));
            read += sizeof(*ptr);
        };

        readToPtr(&Size_);

        ui64 wordCount{};
        readToPtr(&wordCount);

        Data_ = reinterpret_cast<const T*>(cursor());
        read += wordCount * sizeof(T);

        return blob.SubBlob(read, blob.Size());
    }

private:
    ui64 Size_;
    const T* Data_;
};
