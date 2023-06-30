#pragma once

#include "range.h"

#include <library/cpp/yt/memory/ref.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
typename std::enable_if<std::is_unsigned<T>::value, TSharedRef>::type
BitPackUnsignedVector(TRange<T> values, ui64 maxValue);

/*!
 *  \note Memory allocated under #dst must be initialized with zeroes.
 */
template <class T>
typename std::enable_if<std::is_unsigned<T>::value, size_t>::type
BitPackUnsignedVector(TRange<T> values, ui64 maxValue, ui64* dst);

////////////////////////////////////////////////////////////////////////////////

template <class T, bool Scan = true>
class TBitPackedUnsignedVectorReader
{
public:
    TBitPackedUnsignedVectorReader();
    explicit TBitPackedUnsignedVectorReader(const ui64* data);

    T operator[] (size_t index) const;

    //! Number of elements in the vector.
    size_t GetSize() const;

    //! Number of bytes occupied by the vector.
    size_t GetByteSize() const;

    //! Returns the raw values.
    TRange<T> GetData() const;

private:
    const ui64* Data_;
    size_t Size_;
    ui8 Width_;

    const T* Values_;
    std::unique_ptr<T[]> ValuesHolder_;

    T GetValue(size_t index) const;
    void UnpackValues();
    template <int Width>
    void UnpackValuesUnrolled();
    template <class S>
    void UnpackValuesAligned();
    void UnpackValuesFallback();

};

////////////////////////////////////////////////////////////////////////////////

void PrepareDiffFromExpected(std::vector<ui32>* values, ui32* expected, ui32* maxDiff);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define BIT_PACKED_UNSIGNED_VECTOR_INL_H_
#include "bit_packed_unsigned_vector-inl.h"
#undef BIT_PACKED_UNSIGNED_VECTOR_INL_H_
