#pragma once

#include <yql/essentials/public/udf/arrow/bit_util.h>
#include <yql/essentials/utils/swap_bytes.h>

#include <util/generic/yexception.h>
#include <util/system/types.h>

#include <arrow/datum.h>
#include <arrow/util/bit_util.h>
#include <yql/essentials/minikql/defs.h>

#include <algorithm>
#include <array>
#include <cstddef>

namespace NKikimr::NMiniKQL {

template <bool isScalar, bool isOptional>
Y_FORCE_INLINE bool GetBit(const ui8* bitMask, size_t offset) {
    if constexpr (isScalar || !isOptional) {
        return 1;
    } else {
        return arrow::BitUtil::GetBit(bitMask, offset);
    }
}

template <bool isOptional>
Y_FORCE_INLINE void SetBitTo(ui8* bitMask, size_t offset, bool bit_value) {
    if constexpr (!isOptional) {
        return;
    } else {
        arrow::BitUtil::SetBitTo(bitMask, offset, bit_value);
    }
}

template <typename TType>
TType* GetScalar(const arrow::Datum& datum) {
    auto& buffer = arrow::internal::checked_cast<arrow::internal::PrimitiveScalarBase&>(*datum.scalar());
    return static_cast<TType*>(buffer.mutable_data());
};

template <typename TType>
class TDatumStorageView {
public:
    TDatumStorageView(const arrow::Datum& datum)
        : Datum_(datum) {
    }

    TType* data() {
        if (Datum_.is_scalar()) {
            return GetScalar<TType>(Datum_);
        } else {
            MKQL_ENSURE(Datum_.is_array(), "Invalid datum");
            MKQL_ENSURE(Datum_.array()->buffers.size() > 1, "Invalid datum");
            MKQL_ENSURE(Datum_.array()->buffers[1], "Invalid datum");
            return reinterpret_cast<TType*>(Datum_.array()->buffers[1]->mutable_data());
        }
    }

    size_t offset() {
        if (Datum_.is_scalar()) {
            return 0;
        } else {
            MKQL_ENSURE(Datum_.is_array(), "Invalid datum");
            return Datum_.array()->offset;
        }
    }

    ui8* bitMask() {
        if (Datum_.is_scalar()) {
            return nullptr;
        } else {
            MKQL_ENSURE(Datum_.is_array(), "Invalid datum");
            MKQL_ENSURE(Datum_.array()->buffers.size() > 1, "Invalid datum");
            if (!Datum_.array()->buffers[0]) {
                return nullptr;
            }
            return static_cast<ui8*>(Datum_.array()->buffers[0]->mutable_data());
        }
    }

private:
    const arrow::Datum& Datum_;
};

template <typename TType, bool rightIsScalar, bool rightHasBitmask>
void CoalesceByOneElement(size_t elements,
                          const ui8* leftBitMask,
                          const ui8* rightBitMask,
                          size_t leftOffset,
                          size_t rightOffset,
                          const TType* left,
                          const TType* right,
                          TType* out,
                          size_t outOffset,
                          ui8* outBitMask) {
    for (size_t i = 0; i < elements; i++) {
        if (arrow::BitUtil::GetBit(leftBitMask, i + leftOffset)) {
            out[i + outOffset] = left[i + leftOffset];
            SetBitTo<rightHasBitmask>(outBitMask, i + outOffset, true);
        } else {
            if constexpr (rightIsScalar) {
                out[i + outOffset] = right[0];
            } else {
                out[i + outOffset] = right[i + rightOffset];
            }

            SetBitTo<rightHasBitmask>(outBitMask,
                                      i + outOffset,
                                      GetBit<rightIsScalar, rightHasBitmask>(rightBitMask, i + rightOffset));
        }
    }
}

template <bool isAligned>
Y_FORCE_INLINE ui8 GetMaskValue(const ui8* mask, size_t offset, size_t bitMaskPosition) {
    if constexpr (isAligned) {
        return mask[offset / 8 + bitMaskPosition];
    } else {
        const ui8 rightBitsCount = offset % 8;
        const ui8 leftBitsCount = 8 - rightBitsCount;
        ui8 leftMaskPart = mask[offset / 8 + bitMaskPosition] >> rightBitsCount;
        ui8 rightMaskPart = mask[offset / 8 + bitMaskPosition + 1] & ((1 << rightBitsCount) - 1);
        return rightMaskPart << leftBitsCount | leftMaskPart;
    }
}

template <typename TType, bool rightIsScalar, bool rightHasBitmask, bool rightIsAlignedAsLeft>
void VectorizedCoalesce(const ui8* __restrict leftBitMask,
                        const ui8* __restrict rightBitMask,
                        size_t leftOffset,
                        size_t rightOffset,
                        const TType* __restrict left,
                        const TType* __restrict right,
                        TType* __restrict out,
                        size_t outOffset,
                        ui8* __restrict outBitMask,
                        size_t lengthInElements) {
    constexpr auto sizeInBytes = sizeof(TType) * 8;
    auto lengthInBytes = lengthInElements * sizeof(TType);
    // Calculates the truncated length for working with data blocks.
    auto truncatedLengthInBytes =
        lengthInBytes / sizeInBytes * sizeInBytes;
    for (size_t bytesProcessed = 0u, elemShift = 0, step = 0;
         bytesProcessed < truncatedLengthInBytes;
         bytesProcessed += sizeInBytes, elemShift += 8, step += 1) {
        auto currentLeftMask = leftBitMask[leftOffset / 8 + step];
        if constexpr (rightHasBitmask) {
            // If right is optional, updates the output bit mask.
            // Otherwise, the output bit mask doesn't exist.
            if constexpr (rightIsScalar) {
                outBitMask[outOffset / 8 + step] = 0xFFU;
            } else {
                outBitMask[outOffset / 8 + step] =
                    currentLeftMask | GetMaskValue<rightIsAlignedAsLeft>(rightBitMask, rightOffset, step);
            }
        }
        // Expands the current mask to an array of unsigned of type.
        auto expandedMask = NYql::NUdf::BitToByteExpand<std::make_unsigned_t<TType>>(currentLeftMask);
        for (size_t j = 0u; j < 8; ++j) {
            auto arrayIdx = elemShift + j;
            auto rightArrayIdx = arrayIdx + rightOffset;
            if constexpr (rightIsScalar) {
                rightArrayIdx = 0;
            }
            // Performs the operation of mixing data from left and right based on the mask.
            out[outOffset + arrayIdx] = (left[leftOffset + arrayIdx] & expandedMask[j]) | right[rightArrayIdx] & ~expandedMask[j];
        }
    }
}
// Coalesces data from two inputs based on bit masks, handling either (array, array) or (array, scalar).
// This function efficiently merges data from 'left' and 'right' into 'out' array using 'left.bitMask()' and 'right.bitMask()'.
// The function is vectorization friendly, which can significantly improve performance.
template <typename TType, bool rightIsScalar, bool rightHasBitmask>
void BlendCoalesce(TDatumStorageView<TType> left,
                   TDatumStorageView<TType> right,
                   TDatumStorageView<TType> out,
                   size_t lengthInElements) {
    Y_ENSURE(left.offset() % 8 == out.offset() % 8);
    auto firstElementsToProcess = std::min((8 - left.offset() % 8) % 8, lengthInElements);
    // Process one by one until left mask is aligned by byte.
    CoalesceByOneElement<TType, rightIsScalar, rightHasBitmask>(firstElementsToProcess,
                                                                left.bitMask(),
                                                                right.bitMask(),
                                                                left.offset(),
                                                                right.offset(),
                                                                left.data(),
                                                                right.data(),
                                                                out.data(),
                                                                out.offset(),
                                                                out.bitMask());
    lengthInElements -= firstElementsToProcess;

    // Process vectorized.
    if (left.offset() % 8 != right.offset() % 8) {
        VectorizedCoalesce<TType, rightIsScalar, rightHasBitmask, false>(left.bitMask(),
                                                                         right.bitMask(),
                                                                         left.offset() + firstElementsToProcess,
                                                                         right.offset() + firstElementsToProcess,
                                                                         left.data(),
                                                                         right.data(),
                                                                         out.data(),
                                                                         out.offset() + firstElementsToProcess,
                                                                         out.bitMask(),
                                                                         lengthInElements);
    } else {
        VectorizedCoalesce<TType, rightIsScalar, rightHasBitmask, true>(left.bitMask(),
                                                                        right.bitMask(),
                                                                        left.offset() + firstElementsToProcess,
                                                                        right.offset() + firstElementsToProcess,
                                                                        left.data(),
                                                                        right.data(),
                                                                        out.data(),
                                                                        out.offset() + firstElementsToProcess,
                                                                        out.bitMask(),
                                                                        lengthInElements);
    }
    // Process remaining bits that take less memory than one byte.
    size_t remainingBits = (lengthInElements) % 8;
    CoalesceByOneElement<TType, rightIsScalar, rightHasBitmask>(remainingBits,
                                                                left.bitMask(),
                                                                right.bitMask(),
                                                                left.offset() + firstElementsToProcess + lengthInElements - remainingBits,
                                                                right.offset() + firstElementsToProcess + lengthInElements - remainingBits,
                                                                left.data(),
                                                                right.data(),
                                                                out.data(),
                                                                out.offset() + firstElementsToProcess + lengthInElements - remainingBits,
                                                                out.bitMask());
}

} // namespace NKikimr::NMiniKQL
