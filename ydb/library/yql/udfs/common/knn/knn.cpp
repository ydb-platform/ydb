#include "knn-enumerator.h"
#include "knn-serializer.h"
#include "knn-distance.h"

#include <ydb/library/yql/public/udf/udf_helpers.h>

#include <util/generic/buffer.h>
#include <util/generic/queue.h>
#include <util/stream/format.h>

using namespace NYql;
using namespace NYql::NUdf;


SIMPLE_STRICT_UDF_WITH_OPTIONAL_ARGS(TToBinaryString, TOptional<char*>(TAutoMap<TListType<float>>, TOptional<const char*>), 1) {
    const TUnboxedValuePod x = args[0];

    EFormat format = EFormat::FloatVector;
    if(args[1]) {
        const TStringRef formatStr = args[1].AsStringRef();
        if (formatStr == "float")
            format = EFormat::FloatVector;
        else if (formatStr == "byte")
            format = EFormat::ByteVector;
        else if (formatStr == "bit")
            format = EFormat::BitVector;
        else
            return {};
    }
    
    return TKnnSerializerFacade::Serialize(format, valueBuilder, x);
}

SIMPLE_STRICT_UDF(TFromBinaryString, TOptional<TListType<float>>(TAutoMap<const char*>)) {
    TStringRef str = args[0].AsStringRef();

    return TKnnSerializerFacade::Deserialize(valueBuilder, str);
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

    const auto& [ll, lr, rr] = ret.value();
    const float cosine = lr / std::sqrt(ll * rr);
    return TUnboxedValuePod{cosine};
}

SIMPLE_STRICT_UDF(TCosineDistance, TOptional<float>(TAutoMap<const char*>, TAutoMap<const char*>)) {
    Y_UNUSED(valueBuilder);

    const auto ret = KnnTriWayDotProduct(args[0].AsStringRef(), args[1].AsStringRef());
    if (Y_UNLIKELY(!ret))
        return {};

    const auto& [ll, lr, rr] = ret.value();
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
    TFromBinaryString, 
    TToBinaryString,
    TInnerProductSimilarity,
    TCosineSimilarity,
    TCosineDistance,
    TManhattanDistance,
    TEuclideanDistance
    )

REGISTER_MODULES(TKnnModule)

