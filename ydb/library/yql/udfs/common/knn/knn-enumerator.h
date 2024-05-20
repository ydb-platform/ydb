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
