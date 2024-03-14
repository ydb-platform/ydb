#include "knn-enumerator.h"
#include "knn-serializer.h"

#include <ydb/library/yql/public/udf/udf_helpers.h>

#include <util/generic/buffer.h>
#include <util/stream/format.h>

using namespace NYql;
using namespace NYql::NUdf;


SIMPLE_STRICT_UDF(TToBinaryString, char*(TAutoMap<TListType<float>>)) {
    const TUnboxedValuePod x = args[0];
    const EFormat format = EFormat::FloatVector; // will be taken from args in future
    
    return TSerializerFacade::Serialize(format, valueBuilder, x);
}

SIMPLE_STRICT_UDF(TFromBinaryString, TOptional<TListType<float>>(const char*)) {
    TStringRef str = args[0].AsStringRef();

    return TSerializerFacade::Deserialize(valueBuilder, str);
}


std::optional<float> InnerProductSimilarity(const TUnboxedValuePod vector1, const TUnboxedValuePod vector2) {
    float ret = 0;

    if (!EnumerateVectors(vector1, vector2, [&ret](float el1, float el2) { ret += el1 * el2;}))
        return {};

    return ret;
}

std::optional<float> CosineSimilarity(const TUnboxedValuePod vector1, const TUnboxedValuePod vector2) {
    float len1 = 0;
    float len2 = 0;
    float innerProduct = 0;

    if (!EnumerateVectors(vector1, vector2, [&](float el1, float el2) { 
        innerProduct += el1 * el2;
        len1 += el1 * el1;
        len2 += el2 * el2;
        }))
        return {};

    len1 = sqrt(len1);
    len2 = sqrt(len2);

    float cosine = innerProduct / len1 / len2;

    return cosine;
}

std::optional<float> EuclideanDistance(const TUnboxedValuePod vector1, const TUnboxedValuePod vector2) {
    float ret = 0;

    if (!EnumerateVectors(vector1, vector2, [&ret](float el1, float el2) { ret += (el1 - el2) * (el1 - el2);}))
        return {};

    ret = sqrtf(ret);

    return ret;
}

SIMPLE_STRICT_UDF(TInnerProductSimilarity, TOptional<float>(TAutoMap<TListType<float>>, TAutoMap<TListType<float>>)) {
    Y_UNUSED(valueBuilder);

    auto innerProduct = InnerProductSimilarity(args[0], args[1]);
    if (!innerProduct)
        return {};

    return TUnboxedValuePod{innerProduct.value()};
}

SIMPLE_STRICT_UDF(TCosineSimilarity, TOptional<float>(TAutoMap<TListType<float>>, TAutoMap<TListType<float>>)) {
    Y_UNUSED(valueBuilder);

    auto cosine = CosineSimilarity(args[0], args[1]);
    if (!cosine)
        return {};

    return TUnboxedValuePod{cosine.value()};
}

SIMPLE_STRICT_UDF(TCosineDistance, TOptional<float>(TAutoMap<TListType<float>>, TAutoMap<TListType<float>>)) {
    Y_UNUSED(valueBuilder);

    auto cosine = CosineSimilarity(args[0], args[1]);
    if (!cosine)
        return {};

    return TUnboxedValuePod{1 - cosine.value()};
}

SIMPLE_STRICT_UDF(TEuclideanDistance, TOptional<float>(TAutoMap<TListType<float>>, TAutoMap<TListType<float>>)) {
    Y_UNUSED(valueBuilder);

    auto distance = EuclideanDistance(args[0], args[1]);
    if (!distance)
        return {};

    return TUnboxedValuePod{distance.value()};
}

SIMPLE_MODULE(TKnnModule,
    TFromBinaryString, 
    TToBinaryString,
    TInnerProductSimilarity,
    TCosineSimilarity,
    TCosineDistance,
    TEuclideanDistance
    )

REGISTER_MODULES(TKnnModule)

