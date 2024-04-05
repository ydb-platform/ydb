#include "knn-enumerator.h"
#include "knn-serializer.h"

#include <ydb/library/yql/public/udf/udf_helpers.h>

#include <library/cpp/dot_product/dot_product.h>
#include <util/generic/buffer.h>
#include <util/stream/format.h>

using namespace NYql;
using namespace NYql::NUdf;


SIMPLE_STRICT_UDF(TToBinaryString, char*(TAutoMap<TListType<float>>)) {
    const TUnboxedValuePod x = args[0];
    const EFormat format = EFormat::FloatVector; // will be taken from args in future
    
    return TSerializerFacade::Serialize(format, valueBuilder, x);
}

SIMPLE_STRICT_UDF(TFromBinaryString, TOptional<TListType<float>>(TAutoMap<const char*>)) {
    TStringRef str = args[0].AsStringRef();

    return TSerializerFacade::Deserialize(valueBuilder, str);
}

SIMPLE_STRICT_UDF(TInnerProductSimilarity, TOptional<float>(TAutoMap<const char*>, TAutoMap<const char*>)) {
    Y_UNUSED(valueBuilder);

    const TArrayRef<const float> vector1 = TSerializerFacade::GetArray(args[0].AsStringRef()); 
    const TArrayRef<const float> vector2 = TSerializerFacade::GetArray(args[1].AsStringRef()); 

    if (vector1.size() != vector2.size() || vector1.empty() || vector2.empty())
        return {};

    const float dotProduct = DotProduct(vector1.data(), vector2.data(), vector1.size());
    return TUnboxedValuePod{dotProduct};
}

SIMPLE_STRICT_UDF(TCosineSimilarity, TOptional<float>(TAutoMap<const char*>, TAutoMap<const char*>)) {
    Y_UNUSED(valueBuilder);

    const TArrayRef<const float> vector1 = TSerializerFacade::GetArray(args[0].AsStringRef()); 
    const TArrayRef<const float> vector2 = TSerializerFacade::GetArray(args[1].AsStringRef()); 

    if (vector1.size() != vector2.size() || vector1.empty() || vector2.empty())
        return {};    

    const auto [ll, lr, rr] = TriWayDotProduct(vector1.data(), vector2.data(), vector1.size());
    const float cosine = lr / std::sqrt(ll * rr);
    return TUnboxedValuePod{cosine};
}

SIMPLE_STRICT_UDF(TCosineDistance, TOptional<float>(TAutoMap<const char*>, TAutoMap<const char*>)) {
    Y_UNUSED(valueBuilder);

    const TArrayRef<const float> vector1 = TSerializerFacade::GetArray(args[0].AsStringRef()); 
    const TArrayRef<const float> vector2 = TSerializerFacade::GetArray(args[1].AsStringRef()); 

    if (vector1.size() != vector2.size() || vector1.empty() || vector2.empty())
        return {};    

    const auto [ll, lr, rr] = TriWayDotProduct(vector1.data(), vector2.data(), vector1.size());
    const float cosine = lr / std::sqrt(ll * rr);
    return TUnboxedValuePod{1 - cosine};
}

SIMPLE_MODULE(TKnnModule,
    TFromBinaryString, 
    TToBinaryString,
    TInnerProductSimilarity,
    TCosineSimilarity,
    TCosineDistance
    )

REGISTER_MODULES(TKnnModule)

