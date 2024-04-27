#include "knn-enumerator.h"
#include "knn-serializer.h"

#include <ydb/library/yql/public/udf/udf_helpers.h>

#include <library/cpp/dot_product/dot_product.h>
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
        else if (formatStr == "bit")
            format = EFormat::BitVector;
        else
            return {};
    }
    
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

ui32 GetManhattenDistance(const TArrayRef<const ui64> vector1, const TArrayRef<const ui64> vector2) {
    Y_ABORT_UNLESS(vector1.size() == vector2.size());

    ui32 ret = 0;
    for (size_t i = 0; i < vector1.size(); ++i) {
        ret += __builtin_popcountll(vector1[i] ^ vector2[i]);
    }
    return ret;
}

SIMPLE_STRICT_UDF(TBitIndexes, TOptional<TListType<ui32>>(TAutoMap<const char*>, TAutoMap<const char*>)) {
    Y_UNUSED(valueBuilder);

    const TArrayRef<const ui64> targetVector = TSerializerFacade::GetArray64(args[0].AsStringRef()); 
    const TArrayRef<const ui64> storedVector = TSerializerFacade::GetArray64(args[1].AsStringRef()); 

    if (targetVector.empty() || storedVector.empty() || storedVector.size() % targetVector.size() != 0)
        return {};    

    constexpr ui32 MAX_INDEXES = 10;
    typedef std::pair<ui32, ui32> TSimilarityAndIndex;
    TPriorityQueue<TSimilarityAndIndex, TStackVec<TSimilarityAndIndex, MAX_INDEXES + 1>> heap;

    const ui32 totalVectors = storedVector.size() / targetVector.size();
    for (ui32 index = 0; index < totalVectors; ++index) {
        const TArrayRef<const ui64> nextVector = {storedVector.data() + index * targetVector.size(), targetVector.size()};
        ui32 distance = GetManhattenDistance(targetVector, nextVector);
        heap.push({distance, index});
        if (heap.size() == MAX_INDEXES + 1)        
            heap.pop();
    }

    TUnboxedValue* items = nullptr;
    ui32 heapSize = heap.size();
    auto res = valueBuilder->NewArray(heapSize, items);
    for (ui32 i = 0; i < heapSize; ++i) {
        items[heapSize-i-1] = TUnboxedValuePod{heap.top().second};
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
    TBitIndexes
    )

REGISTER_MODULES(TKnnModule)

