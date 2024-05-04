#include "knn-enumerator.h"
#include "knn-serializer.h"
#include "knn-distance.h"

#include <ydb/library/yql/public/udf/udf_helpers.h>

#include <util/generic/buffer.h>
#include <util/generic/queue.h>
#include <util/stream/format.h>

using namespace NYql;
using namespace NYql::NUdf;


SIMPLE_STRICT_UDF_WITH_OPTIONAL_ARGS(TToBinaryString, char*(TAutoMap<TListType<float>>, TOptional<const char*>), 1) {
    const TUnboxedValuePod x = args[0];

    EFormat format = EFormat::FloatVector;
    if(args[1]) {
        const TStringRef formatStr = args[1].AsStringRef();
        if (formatStr == "float")
            format = EFormat::FloatVector;
        if (formatStr == "floatbyte")
            format = EFormat::FloatByteVector;
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



SIMPLE_STRICT_UDF(TManhattenDistance, TOptional<ui16>(TAutoMap<const char*>, TAutoMap<const char*>)) {
    Y_UNUSED(valueBuilder);

    const auto ret = KnnManhattenDistance(args[0].AsStringRef(), args[1].AsStringRef());
    if (Y_UNLIKELY(!ret))
        return {};

    return TUnboxedValuePod{ret.value()};
}

SIMPLE_STRICT_UDF(TBitIndexes, TOptional<TListType<ui64>>(TAutoMap<const char*>, TAutoMap<const char*>, ui16, ui16, ui64)) {
    Y_UNUSED(valueBuilder);

    const TArrayRef<const ui64> targetVector = TKnnBitVectorSerializer::GetArray64(args[0].AsStringRef()); 
    const TArrayRef<const ui64> storedVector = TKnnBitVectorSerializer::GetArray64(args[1].AsStringRef()); 
    const ui16 topK = args[2].Get<ui16>();
    const ui16 distanceThreshold = args[3].Get<ui16>();
    const ui64 seed = args[4].Get<ui64>();

    if (Y_UNLIKELY(targetVector.empty() || storedVector.empty() || storedVector.size() % targetVector.size() != 0))
        return {};    

    constexpr ui16 MAX_INDEXES = 128;
    typedef std::pair<ui16, ui16> TSimilarityAndIndex;
    TPriorityQueue<TSimilarityAndIndex, TStackVec<TSimilarityAndIndex, MAX_INDEXES + 1>> heap;

    // Add vector distances to priority queue
    const ui32 totalVectors = storedVector.size() / targetVector.size();
    for (ui32 index = 0; index < totalVectors; ++index) {
        const TArrayRef<const ui64> nextVector = {storedVector.data() + index * targetVector.size(), targetVector.size()};
        ui32 distance = KnnManhattenDistance(targetVector, nextVector);
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
    TManhattenDistance,
    TBitIndexes
    )

REGISTER_MODULES(TKnnModule)

