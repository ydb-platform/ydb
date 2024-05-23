#include "knn-enumerator.h"
#include "knn-serializer.h"
#include "knn-distance.h"

#include <ydb/library/yql/public/udf/udf_helpers.h>

#include <util/generic/buffer.h>
#include <util/generic/queue.h>
#include <util/stream/format.h>

using namespace NYql;
using namespace NYql::NUdf;

SIMPLE_STRICT_UDF(TToBinaryStringFloat, const char*(TAutoMap<TListType<float>>)) {
    return TKnnVectorSerializer<float, EFormat::FloatVector>::Serialize(valueBuilder, args[0]);
}

SIMPLE_STRICT_UDF(TToBinaryStringByte, const char*(TAutoMap<TListType<float>>)) {
    return TKnnVectorSerializer<ui8, EFormat::Uint8Vector>::Serialize(valueBuilder, args[0]);
}

SIMPLE_STRICT_UDF(TToBinaryStringBit, const char*(TAutoMap<TListType<float>>)) {
    return TKnnBitVectorSerializer::Serialize(valueBuilder, args[0]);
}

SIMPLE_STRICT_UDF(TFloatFromBinaryString, TOptional<TListType<float>>(TAutoMap<const char*>)) {
    return TKnnSerializerFacade::Deserialize(valueBuilder, args[0].AsStringRef());
}

SIMPLE_STRICT_UDF(TInnerProductSimilarity, TOptional<float>(TAutoMap<const char*>, TAutoMap<const char*>)) {
    Y_UNUSED(valueBuilder);

    const auto ret = KnnDotProduct(args[0].AsStringRef(), args[1].AsStringRef());
    if (Y_UNLIKELY(!ret))
        return {};

    return TUnboxedValuePod{ret.value()};
}

SIMPLE_STRICT_UDF(TCosineSimilarity, TOptional<float>(TAutoMap<const char*>, TAutoMap<const char*>)) {
    Y_UNUSED(valueBuilder);

    const auto ret = KnnTriWayDotProduct(args[0].AsStringRef(), args[1].AsStringRef());
    if (Y_UNLIKELY(!ret))
        return {};

    const auto [ll, lr, rr] = ret.value();
    const float cosine = lr / std::sqrt(ll * rr);
    return TUnboxedValuePod{cosine};
}

SIMPLE_STRICT_UDF(TCosineDistance, TOptional<float>(TAutoMap<const char*>, TAutoMap<const char*>)) {
    Y_UNUSED(valueBuilder);

    const auto ret = KnnTriWayDotProduct(args[0].AsStringRef(), args[1].AsStringRef());
    if (Y_UNLIKELY(!ret))
        return {};

    const auto [ll, lr, rr] = ret.value();
    const float cosine = lr / std::sqrt(ll * rr);
    return TUnboxedValuePod{1 - cosine};
}

SIMPLE_STRICT_UDF(TManhattanDistance, TOptional<float>(TAutoMap<const char*>, TAutoMap<const char*>)) {
    Y_UNUSED(valueBuilder);

    const auto ret = KnnManhattanDistance(args[0].AsStringRef(), args[1].AsStringRef());
    if (Y_UNLIKELY(!ret))
        return {};

    return TUnboxedValuePod{ret.value()};
}

SIMPLE_STRICT_UDF(TEuclideanDistance, TOptional<float>(TAutoMap<const char*>, TAutoMap<const char*>)) {
    Y_UNUSED(valueBuilder);

    const auto ret = KnnEuclideanDistance(args[0].AsStringRef(), args[1].AsStringRef());
    if (Y_UNLIKELY(!ret))
        return {};

    return TUnboxedValuePod{ret.value()};
}

SIMPLE_MODULE(TKnnModule,
              TToBinaryStringFloat,
              TToBinaryStringByte,
              TToBinaryStringBit,
              TFloatFromBinaryString,
              TInnerProductSimilarity,
              TCosineSimilarity,
              TCosineDistance,
              TManhattanDistance,
              TEuclideanDistance)

REGISTER_MODULES(TKnnModule)
