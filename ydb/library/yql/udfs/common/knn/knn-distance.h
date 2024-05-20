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
            const TArrayRef<const ui64> vector1 = TKnnBitVectorSerializer::GetArray64(str1);
            const TArrayRef<const ui64> vector2 = TKnnBitVectorSerializer::GetArray64(str2);

            if (Y_UNLIKELY(vector1.size() != vector2.size() || vector1.empty() || vector1.size() > UINT16_MAX))
                return {};

            ui64 ret = 0;
            for (size_t i = 0; i < vector1.size(); ++i)
                ret += std::popcount(vector1[i] ^ vector2[i]);
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
            const TArrayRef<const ui64> vector1 = TKnnBitVectorSerializer::GetArray64(str1);
            const TArrayRef<const ui64> vector2 = TKnnBitVectorSerializer::GetArray64(str2);

            if (Y_UNLIKELY(vector1.size() != vector2.size() || vector1.empty() || vector1.size() > UINT16_MAX))
                return {};

            ui64 ret = 0;
            for (size_t i = 0; i < vector1.size(); ++i)
                ret += std::popcount(vector1[i] ^ vector2[i]);
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
            const TArrayRef<const ui64> vector1 = TKnnBitVectorSerializer::GetArray64(str1);
            const TArrayRef<const ui64> vector2 = TKnnBitVectorSerializer::GetArray64(str2);

            if (Y_UNLIKELY(vector1.size() != vector2.size() || vector1.empty() || vector1.size() > UINT16_MAX))
                return {};

            ui64 ret = 0;
            for (size_t i = 0; i < vector1.size(); ++i)
                ret += std::popcount(vector1[i] & vector2[i]);
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
            const TArrayRef<const ui64> vector1 = TKnnBitVectorSerializer::GetArray64(str1);
            const TArrayRef<const ui64> vector2 = TKnnBitVectorSerializer::GetArray64(str2);

            if (Y_UNLIKELY(vector1.size() != vector2.size() || vector1.empty() || vector1.size() > UINT16_MAX))
                return {};

            ui64 ll = 0;
            ui64 rr = 0;
            ui64 lr = 0;
            const auto* v1 = vector1.data();
            const auto* v2 = vector2.data();
            for (const auto* end = v1 + vector1.size(); v1 != end; ++v1, ++v2) {
                ll += std::popcount(*v1);
                rr += std::popcount(*v2);
                lr += std::popcount(*v1 & *v2);
            }

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
