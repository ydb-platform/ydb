#pragma once

#include "knn-defines.h"
#include "knn-serializer.h"

#include <ydb/library/yql/public/udf/udf_helpers.h>

#include <library/cpp/dot_product/dot_product.h>
#include <library/cpp/l1_distance/l1_distance.h>
#include <library/cpp/l2_distance/l2_distance.h>
#include <util/generic/array_ref.h>
#include <util/generic/buffer.h>
#include <util/stream/format.h>

#include <bit>

using namespace NYql;
using namespace NYql::NUdf;

inline void BitVectorHandleShort(ui64 bitLen, const ui64* v1, const ui64* v2, auto&& op) {
    ui64 d1 = 0;
    ui64 d2 = 0;
    const auto byteLen = bitLen / 8; // TODO manual switch for [1..7]?
    std::memcpy(&d1, v1, byteLen);
    std::memcpy(&d2, v2, byteLen);
    op(d1, d2);
}

inline void BitVectorHandleTail(ui64 bitLen, const ui64* v1, const ui64* v2, auto&& op) {
    if (Y_LIKELY(bitLen == 0)) // fast-path for aligned case
        return;
    const auto unneededBytes = sizeof(ui64) - bitLen / 8;
    const auto* r1 = reinterpret_cast<const ui8*>(v1) - unneededBytes;
    const auto* r2 = reinterpret_cast<const ui8*>(v2) - unneededBytes;
    ui64 d1, d2; // unligned loads
    std::memcpy(&d1, r1, sizeof(ui64));
    std::memcpy(&d2, r2, sizeof(ui64));
    ui64 mask = ((1 << (unneededBytes * 8)) - 1);
    // big    endian: 0 1 2 3 4 5 6 7 | 0 1 2 3 | 0 1 | 0 | 0 => needs to zero high bits
    // little endian: 7 6 5 4 3 2 1 0 | 3 2 1 0 | 1 0 | 0 | 0 => needs to zero low  bits
    if constexpr (std::endian::native == std::endian::little) {
        mask = ~mask;
    }
    op(d1 & mask, d2 & mask);
}

inline void BitVectorHandleOp(ui64 bitLen, const ui64* v1, const ui64* v2, auto&& op) {
    const auto wordLen = bitLen / 64;
    if (Y_LIKELY(wordLen == 0)) // fast-path for short case
        return BitVectorHandleShort(bitLen, v1, v2, op);

    bitLen %= 64;
    for (const auto* end = v1 + wordLen; v1 != end; ++v1, ++v2) {
        op(*v1, *v2);
    }
    BitVectorHandleTail(bitLen, v1, v2, op);
}

inline std::optional<float> KnnManhattanDistance(const TStringRef& str1, const TStringRef& str2) {
    const ui8 format1 = str1.Data()[str1.Size() - HeaderLen];
    const ui8 format2 = str2.Data()[str2.Size() - HeaderLen];

    if (Y_UNLIKELY(format1 != format2))
        return {};

    switch (format1) {
        case EFormat::FloatVector: {
            const TArrayRef<const float> vector1 = TKnnSerializerFacade::GetArray<float>(str1);
            const TArrayRef<const float> vector2 = TKnnSerializerFacade::GetArray<float>(str2);

            if (Y_UNLIKELY(vector1.size() != vector2.size() || vector1.empty()))
                return {};

            return ::L1Distance(vector1.data(), vector2.data(), vector1.size());
        }
        case EFormat::Uint8Vector: {
            const TArrayRef<const ui8> vector1 = TKnnSerializerFacade::GetArray<ui8>(str1);
            const TArrayRef<const ui8> vector2 = TKnnSerializerFacade::GetArray<ui8>(str2);

            if (Y_UNLIKELY(vector1.size() != vector2.size() || vector1.empty()))
                return {};

            return ::L1Distance(vector1.data(), vector2.data(), vector1.size());
        }
        case EFormat::BitVector: {
            auto [v1, len1] = TKnnBitVectorSerializer::GetArray(str1);
            auto [v2, len2] = TKnnBitVectorSerializer::GetArray(str2);

            if (Y_UNLIKELY(len1 != len2 || len1 == 0))
                return {};

            ui64 ret = 0;
            BitVectorHandleOp(len1, v1, v2, [&](ui64 d1, ui64 d2) {
                ret += std::popcount(d1 ^ d2);
            });
            return ret;
        }
        default:
            return {};
    }
}

inline std::optional<float> KnnEuclideanDistance(const TStringRef& str1, const TStringRef& str2) {
    const ui8 format1 = str1.Data()[str1.Size() - HeaderLen];
    const ui8 format2 = str2.Data()[str2.Size() - HeaderLen];

    if (Y_UNLIKELY(format1 != format2))
        return {};

    switch (format1) {
        case EFormat::FloatVector: {
            const TArrayRef<const float> vector1 = TKnnSerializerFacade::GetArray<float>(str1);
            const TArrayRef<const float> vector2 = TKnnSerializerFacade::GetArray<float>(str2);

            if (Y_UNLIKELY(vector1.size() != vector2.size() || vector1.empty()))
                return {};

            return ::L2Distance(vector1.data(), vector2.data(), vector1.size());
        }
        case EFormat::Uint8Vector: {
            const TArrayRef<const ui8> vector1 = TKnnSerializerFacade::GetArray<ui8>(str1);
            const TArrayRef<const ui8> vector2 = TKnnSerializerFacade::GetArray<ui8>(str2);

            if (Y_UNLIKELY(vector1.size() != vector2.size() || vector1.empty()))
                return {};

            return ::L2Distance(vector1.data(), vector2.data(), vector1.size());
        }
        case EFormat::BitVector: {
            auto [v1, len1] = TKnnBitVectorSerializer::GetArray(str1);
            auto [v2, len2] = TKnnBitVectorSerializer::GetArray(str2);

            if (Y_UNLIKELY(len1 != len2 || len1 == 0))
                return {};

            ui64 ret = 0;
            BitVectorHandleOp(len1, v1, v2, [&](ui64 d1, ui64 d2) {
                ret += std::popcount(d1 ^ d2);
            });
            return NPrivate::NL2Distance::L2DistanceSqrt(ret);
        }
        default:
            return {};
    }
}

inline std::optional<float> KnnDotProduct(const TStringRef& str1, const TStringRef& str2) {
    const ui8 format1 = str1.Data()[str1.Size() - HeaderLen];
    const ui8 format2 = str2.Data()[str2.Size() - HeaderLen];

    if (Y_UNLIKELY(format1 != format2))
        return {};

    switch (format1) {
        case EFormat::FloatVector: {
            const TArrayRef<const float> vector1 = TKnnSerializerFacade::GetArray<float>(str1);
            const TArrayRef<const float> vector2 = TKnnSerializerFacade::GetArray<float>(str2);

            if (Y_UNLIKELY(vector1.size() != vector2.size() || vector1.empty()))
                return {};

            return ::DotProduct(vector1.data(), vector2.data(), vector1.size());
        }
        case EFormat::Uint8Vector: {
            const TArrayRef<const ui8> vector1 = TKnnSerializerFacade::GetArray<ui8>(str1);
            const TArrayRef<const ui8> vector2 = TKnnSerializerFacade::GetArray<ui8>(str2);

            if (Y_UNLIKELY(vector1.size() != vector2.size() || vector1.empty()))
                return {};

            return ::DotProduct(vector1.data(), vector2.data(), vector1.size());
        }
        case EFormat::BitVector: {
            auto [v1, len1] = TKnnBitVectorSerializer::GetArray(str1);
            auto [v2, len2] = TKnnBitVectorSerializer::GetArray(str2);

            if (Y_UNLIKELY(len1 != len2 || len1 == 0))
                return {};

            ui64 ret = 0;
            BitVectorHandleOp(len1, v1, v2, [&](ui64 d1, ui64 d2) {
                ret += std::popcount(d1 & d2);
            });
            return ret;
        }
        default:
            return {};
    }
}

inline std::optional<TTriWayDotProduct<float>> KnnTriWayDotProduct(const TStringRef& str1, const TStringRef& str2) {
    const ui8 format1 = str1.Data()[str1.Size() - HeaderLen];
    const ui8 format2 = str2.Data()[str2.Size() - HeaderLen];

    if (Y_UNLIKELY(format1 != format2))
        return {};

    switch (format1) {
        case EFormat::FloatVector: {
            const TArrayRef<const float> vector1 = TKnnSerializerFacade::GetArray<float>(str1);
            const TArrayRef<const float> vector2 = TKnnSerializerFacade::GetArray<float>(str2);

            if (Y_UNLIKELY(vector1.size() != vector2.size() || vector1.empty()))
                return {};

            return ::TriWayDotProduct(vector1.data(), vector2.data(), vector1.size());
        }
        case EFormat::Uint8Vector: {
            const TArrayRef<const ui8> vector1 = TKnnSerializerFacade::GetArray<ui8>(str1);
            const TArrayRef<const ui8> vector2 = TKnnSerializerFacade::GetArray<ui8>(str2);

            if (Y_UNLIKELY(vector1.size() != vector2.size() || vector1.empty()))
                return {};

            TTriWayDotProduct<float> result;
            result.LL = ::DotProduct(vector1.data(), vector1.data(), vector1.size());
            result.LR = ::DotProduct(vector1.data(), vector2.data(), vector1.size());
            result.RR = ::DotProduct(vector2.data(), vector2.data(), vector1.size());
            return result;
        }
        case EFormat::BitVector: {
            auto [v1, len1] = TKnnBitVectorSerializer::GetArray(str1);
            auto [v2, len2] = TKnnBitVectorSerializer::GetArray(str2);

            if (Y_UNLIKELY(len1 != len2 || len1 == 0))
                return {};

            ui64 ll = 0;
            ui64 rr = 0;
            ui64 lr = 0;
            BitVectorHandleOp(len1, v1, v2, [&](ui64 d1, ui64 d2) {
                ll += std::popcount(d1);
                rr += std::popcount(d2);
                lr += std::popcount(d1 & d2);
            });

            TTriWayDotProduct<float> result;
            result.LL = ll;
            result.LR = lr;
            result.RR = rr;
            return result;
        }
        default:
            return {};
    }
}
