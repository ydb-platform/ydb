#pragma once

#include "knn-defines.h"
#include "knn-serializer.h"

#include <ydb/library/yql/public/udf/udf_helpers.h>

#include <library/cpp/dot_product/dot_product.h>
#include <util/generic/array_ref.h>
#include <util/generic/buffer.h>
#include <util/stream/format.h>

using namespace NYql;
using namespace NYql::NUdf;

static ui16 KnnManhattenDistance(const TArrayRef<const ui64> vector1, const TArrayRef<const ui64> vector2) {
    Y_DEBUG_ABORT_UNLESS(vector1.size() == vector2.size());
    Y_DEBUG_ABORT_UNLESS(vector1.size() <= UINT16_MAX);

    ui16 ret = 0;
    for (size_t i = 0; i < vector1.size(); ++i) {
        ret += __builtin_popcountll(vector1[i] ^ vector2[i]);
    }
    return ret;
}

static std::optional<ui16> KnnManhattenDistance(const TStringRef& str1, const TStringRef& str2) {
    const TArrayRef<const ui64> vector1 = TKnnBitVectorSerializer::GetArray64(str1); 
    const TArrayRef<const ui64> vector2 = TKnnBitVectorSerializer::GetArray64(str2); 

    if (Y_UNLIKELY(vector1.size() != vector2.size() || vector1.empty() || vector1.size() > UINT16_MAX))
        return {};   

    return KnnManhattenDistance(vector1, vector2);
}

static std::optional<float> KnnDotProduct(const TStringRef& str1, const TStringRef& str2) {
    const ui8 format1 = str1.Data()[str1.Size() - HeaderLen];
    const ui8 format2 = str2.Data()[str2.Size() - HeaderLen];

    if (Y_UNLIKELY(format1 != format2))
        return {};

    switch (format1) {
        case EFormat::FloatVector: {
            const TArrayRef<const float> vector1 = TKnnSerializerFacade::GetArray<float>(str1); 
            const TArrayRef<const float> vector2 = TKnnSerializerFacade::GetArray<float>(str2); 

            if (Y_UNLIKELY(vector1.size() != vector2.size() || vector1.empty() || vector2.empty()))
                return {};

            return ::DotProduct(vector1.data(), vector2.data(), vector1.size());
        }
        case EFormat::FloatByteVector: {
            const TArrayRef<const ui8> vector1 = TKnnSerializerFacade::GetArray<ui8>(str1); 
            const TArrayRef<const ui8> vector2 = TKnnSerializerFacade::GetArray<ui8>(str2); 

            if (Y_UNLIKELY(vector1.size() != vector2.size() || vector1.empty() || vector2.empty()))
                return {};

            return ::DotProduct(vector1.data(), vector2.data(), vector1.size());
        }
        default:
            return {};
    }
}

static std::optional<TTriWayDotProduct<float>> KnnTriWayDotProduct(const TStringRef& str1, const TStringRef& str2) {
    const ui8 format1 = str1.Data()[str1.Size() - HeaderLen];
    const ui8 format2 = str2.Data()[str2.Size() - HeaderLen];

    if (Y_UNLIKELY(format1 != format2))
        return {};

    switch (format1) {
        case EFormat::FloatVector: {
            const TArrayRef<const float> vector1 = TKnnSerializerFacade::GetArray<float>(str1); 
            const TArrayRef<const float> vector2 = TKnnSerializerFacade::GetArray<float>(str2); 

            if (Y_UNLIKELY(vector1.size() != vector2.size() || vector1.empty() || vector2.empty()))
                return {};

            return ::TriWayDotProduct(vector1.data(), vector2.data(), vector1.size());
        }
        case EFormat::FloatByteVector: {
            const TArrayRef<const ui8> vector1 = TKnnSerializerFacade::GetArray<ui8>(str1); 
            const TArrayRef<const ui8> vector2 = TKnnSerializerFacade::GetArray<ui8>(str2); 

            if (Y_UNLIKELY(vector1.size() != vector2.size() || vector1.empty() || vector2.empty()))
                return {};

            TTriWayDotProduct<float> result;
            result.LL = ::DotProduct(vector1.data(), vector1.data(), vector1.size());
            result.LR = ::DotProduct(vector1.data(), vector2.data(), vector1.size());
            result.RR = ::DotProduct(vector2.data(), vector2.data(), vector1.size());
            return result;
        }
        default:
            return {};
    }
}

   