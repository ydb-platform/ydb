#pragma once

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