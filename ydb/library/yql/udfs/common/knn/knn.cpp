#include "knn-enumerator.h"
#include "knn-serializer.h"

#include <ydb/library/yql/public/udf/udf_helpers.h>

#include <library/cpp/dot_product/dot_product.h>
#include <util/generic/buffer.h>
#include <util/generic/queue.h>
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

SIMPLE_STRICT_UDF(TToBitString, char*(TAutoMap<TListType<float>>)) {
    const TUnboxedValuePod x = args[0];
    
    return TBitVectorSerializer::Serialize(valueBuilder, x);
}

ui16 GetManhattenDistance(const TArrayRef<const ui64> vector1, const TArrayRef<const ui64> vector2) {
    Y_ABORT_UNLESS(vector1.size() == vector2.size());
    Y_ABORT_UNLESS(vector1.size() <= UINT16_MAX);

    ui16 ret = 0;
    for (size_t i = 0; i < vector1.size(); ++i) {
        ret += __builtin_popcountll(vector1[i] ^ vector2[i]);
    }
    return ret;
}

SIMPLE_STRICT_UDF(TBitIndexes, TOptional<TListType<ui64>>(TAutoMap<const char*>, TAutoMap<const char*>, ui16, ui16, ui64)) {
    Y_UNUSED(valueBuilder);

    const TArrayRef<const ui64> targetVector = TBitVectorSerializer::GetArray64(args[0].AsStringRef()); 
    const TArrayRef<const ui64> storedVector = TBitVectorSerializer::GetArray64(args[1].AsStringRef()); 
    const ui16 topK = args[2].Get<ui16>();
    const ui16 distanceThreshold = args[3].Get<ui16>();
    const ui64 seed = args[4].Get<ui64>();

    if (targetVector.empty() || storedVector.empty() || storedVector.size() % targetVector.size() != 0)
        return {};    

    constexpr ui16 MAX_INDEXES = 128;
    typedef std::pair<ui16, ui16> TSimilarityAndIndex;
    TPriorityQueue<TSimilarityAndIndex, TStackVec<TSimilarityAndIndex, MAX_INDEXES + 1>> heap;

    // Add vector distances to priority queue
    const ui32 totalVectors = storedVector.size() / targetVector.size();
    for (ui32 index = 0; index < totalVectors; ++index) {
        const TArrayRef<const ui64> nextVector = {storedVector.data() + index * targetVector.size(), targetVector.size()};
        ui32 distance = GetManhattenDistance(targetVector, nextVector);
        if (distance > distanceThreshold)
            continue;
        heap.push({distance, index});
        if (heap.size() == topK + 1)        
            heap.pop();
    }

    // Return min elemens from priority queue
    TUnboxedValue* items = nullptr;
    ui64 heapSize = heap.size();
    auto res = valueBuilder->NewArray(heapSize, items);
    for (ui64 i = 0; i < heapSize; ++i) {
        items[heapSize-i-1] = TUnboxedValuePod{heap.top().second + seed};
        heap.pop();
    }
    return res.Release();
}

SIMPLE_MODULE(TKnnModule,
    TFromBinaryString, 
    TToBinaryString,
    TInnerProductSimilarity,
    TCosineSimilarity,
    TCosineDistance,
    TToBitString,
    TBitIndexes
    )

REGISTER_MODULES(TKnnModule)

