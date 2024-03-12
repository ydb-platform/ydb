#include <ydb/library/yql/public/udf/udf_helpers.h>

#include <util/generic/buffer.h>
#include <util/stream/format.h>

using namespace NYql;
using namespace NYql::NUdf;

template <typename TCallback>
void EnumerateVector(const TUnboxedValuePod vector, TCallback&& callback) {
    const auto elements = vector.GetElements();
    if (elements) {
        const auto size = vector.GetListLength();
        
        for (ui32 i = 0; i < size; ++i) {
            callback(elements[i].Get<float>());
        }
    } else {
        TUnboxedValue value;
        const auto it = vector.GetListIterator();
        while (it.Next(value)) {
            callback(value.Get<float>());
        }
    }
}

template <typename TCallback>
bool EnumerateVectors(const TUnboxedValuePod vector1, const TUnboxedValuePod vector2, TCallback&& callback) {
    
    auto enumerateBothSized = [&callback] (const TUnboxedValuePod vector1, const TUnboxedValue* elements1, const TUnboxedValuePod vector2, const TUnboxedValue* elements2) {
        const auto size1 = vector1.GetListLength();
        const auto size2 = vector2.GetListLength();
        
        // Length mismatch
        if (size1 != size2)
            return false;

        for (ui32 i = 0; i < size1; ++i) {
            callback(elements1[i].Get<float>(), elements2[i].Get<float>());
        }
        
        return true;
    };
    
    auto enumerateOneSized = [&callback] (const TUnboxedValuePod vector1, const TUnboxedValue* elements1, const TUnboxedValuePod vector2) {
        const auto size = vector1.GetListLength();
        ui32 idx = 0;
        TUnboxedValue value;
        const auto it = vector2.GetListIterator();          

        while (it.Next(value)) {
            callback(elements1[idx++].Get<float>(), value.Get<float>());
        }

        // Length mismatch
        if (it.Next(value) || idx != size)
            return false;

        return true;
    };

    auto enumerateNoSized = [&callback] (const TUnboxedValuePod vector1, const TUnboxedValuePod vector2) {
        TUnboxedValue value1, value2;
        const auto it1 = vector1.GetListIterator();
        const auto it2 = vector2.GetListIterator();    
        for (; it1.Next(value1) && it2.Next(value2);) {
            callback(value1.Get<float>(), value2.Get<float>());
        }

        // Length mismatch
        if (it1.Next(value1) || it2.Next(value2))
            return false;

        return true;
    };

    const auto elements1 = vector1.GetElements();
    const auto elements2 = vector2.GetElements();
    if (elements1 && elements2) {
        if (!enumerateBothSized(vector1, elements1, vector2, elements2))
            return false;
    } else if (elements1) {
        if (!enumerateOneSized(vector1, elements1, vector2))
            return false;
    } else if (elements2) {
        if (!enumerateOneSized(vector2, elements2, vector1))
            return false;
    } else {
        if (!enumerateNoSized(vector1, vector2))
            return false;
    }

    return true;
}

enum EFormat : unsigned char {
    FloatVector = 1
};

TString SerializeFloatVector(const TUnboxedValuePod x) {
    const EFormat format = EFormat::FloatVector;
    
    TString str;
    TStringOutput outStr(str);

    if (const auto elements = x.GetElements()) {
        const auto size = x.GetListLength();
        outStr.Reserve(1 + size * sizeof(float));
    }
    outStr.Write(&format, sizeof(unsigned char));

    EnumerateVector(x,  [&outStr] (float element) { outStr.Write(&element, sizeof(float)); });

    return str;
}

NYql::NUdf::TUnboxedValue DeserializeFloatVector(const IValueBuilder *valueBuilder, TStringRef str) {
    if (str.Size() % sizeof(float) != 0)    
        return {};
    
    const ui32 count = str.Size() / sizeof(float);

    TUnboxedValue* items = nullptr;
    auto res = valueBuilder->NewArray(count, items);
    
    TMemoryInput inStr(str);
    for (ui32 i = 0; i < count; ++i) {
        float element;
        if (inStr.Read(&element, sizeof(float)) != sizeof(float))
            return {};
        *items++ = TUnboxedValuePod{element};
    }

    return res.Release();
}

SIMPLE_STRICT_UDF(TToBinaryString, char*(TAutoMap<TListType<float>>)) {
    return valueBuilder->NewString(SerializeFloatVector(args[0]));
}

SIMPLE_STRICT_UDF(TFromBinaryString, TOptional<TListType<float>>(const char*)) {
    TStringRef str = args[0].AsStringRef();
    if (str.Size() == 0)
        return {};

    const EFormat format = static_cast<EFormat>(str.Data()[0]);
    str = TStringRef{str.Data() + 1, str.Size() -1};
    switch (format) {
        case EFormat::FloatVector: 
            return DeserializeFloatVector(valueBuilder, str);
        default:
            return {};
    }
}

float CalcLength(const TUnboxedValuePod vector) {
    float ret = 0;

    EnumerateVector(vector, [&ret](float el) { ret += el * el;});

    ret = sqrt(ret);

    return ret;
}

std::optional<float> InnerProductSimilarity(const TUnboxedValuePod vector1, const TUnboxedValuePod vector2) {
    float ret = 0;

    if (!EnumerateVectors(vector1, vector2, [&ret](float el1, float el2) { ret += el1 * el2;}))
        return {};

    return ret;
}

std::optional<float> CosineSimilarity(const TUnboxedValuePod vector1, const TUnboxedValuePod vector2) {
    auto innerProduct = InnerProductSimilarity(vector1, vector2);
    if (!innerProduct)
        return {};

    float len0 = CalcLength(vector1);
    float len1 = CalcLength(vector2);

    float cosine = innerProduct.value() / len0 / len1;

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

