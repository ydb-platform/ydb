#pragma once

#include <ydb/library/yql/public/udf/udf_helpers.h>

#include <util/generic/array_ref.h>

using namespace NYql;
using namespace NYql::NUdf;

template <typename T, typename TCallback>
void EnumerateVector(const TUnboxedValuePod vector, TCallback&& callback) {
    const auto* elements = vector.GetElements();
    if (elements) {
        for (auto& value : TArrayRef{elements, vector.GetListLength()}) {
            callback(value.Get<T>());
        }
    } else {
        TUnboxedValue value;
        const auto it = vector.GetListIterator();
        while (it.Next(value)) {
            callback(value.Get<T>());
        }
    }
}
