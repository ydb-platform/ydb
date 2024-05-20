#pragma once

#include <ydb/library/yql/public/udf/udf_helpers.h>

#include <util/generic/buffer.h>
#include <util/stream/format.h>

using namespace NYql;
using namespace NYql::NUdf;

template <typename TCallback>
void EnumerateVector(const TUnboxedValuePod vector, TCallback&& callback) {
    const auto* elements = vector.GetElements();
    if (elements) {
        const auto* end = elements + vector.GetListLength();
        while (elements != end) {
            callback(elements++->Get<float>());
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
    TUnboxedValue value1, value2;
    const auto* elements1 = vector1.GetElements();
    const auto* elements2 = vector2.GetElements();

    auto enumerateBothSized = [&] {
        const auto size = vector1.GetListLength();
        // Length mismatch
        if (size != vector2.GetListLength())
            return false;

        for (ui64 i = 0; i != size; ++i) {
            callback(elements1[i].Get<float>(), elements2[i].Get<float>());
        }
        return true;
    };

    auto enumerateOneSized = [&]<bool Invert>(const TUnboxedValue* elements, size_t size, const TUnboxedValuePod vector, std::bool_constant<Invert>) {
        const auto* end = elements + size;
        const auto it = vector.GetListIterator();
        while (elements != end && it.Next(value1)) {
            if constexpr (Invert) {
                callback(value1.Get<float>(), elements++->Get<float>());
            } else {
                callback(elements++->Get<float>(), value1.Get<float>());
            }
        }

        // Length mismatch
        return elements == end && !it.Next(value1);
    };

    auto enumerateNoSized = [&](const TUnboxedValuePod vector1, const TUnboxedValuePod vector2) {
        const auto it1 = vector1.GetListIterator();
        const auto it2 = vector2.GetListIterator();
        while (it1.Next(value1) && it2.Next(value2)) {
            callback(value1.Get<float>(), value2.Get<float>());
        }

        // Length mismatch
        return !it1.Next(value1) && !it2.Next(value2);
    };

    if (elements1 && elements2) {
        if (!enumerateBothSized())
            return false;
    } else if (elements1) {
        if (!enumerateOneSized(elements1, vector1.GetListLength(), vector2, std::false_type{}))
            return false;
    } else if (elements2) {
        if (!enumerateOneSized(elements2, vector2.GetListLength(), vector1, std::true_type{}))
            return false;
    } else {
        if (!enumerateNoSized(vector1, vector2))
            return false;
    }

    return true;
}