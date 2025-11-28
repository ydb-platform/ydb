#pragma once

#include "udf_value.h"

#include <yql/essentials/public/udf/arrow/block_item.h>

namespace NYql::NUdf {

template <bool IsNull>
constexpr TUnboxedValuePod CreateSingularUnboxedValuePod() {
    if constexpr (IsNull) {
        return TUnboxedValuePod();
    } else {
        return TUnboxedValuePod::Zero();
    }
}

template <bool IsNull>
constexpr TBlockItem CreateSingularBlockItem() {
    if constexpr (IsNull) {
        return TBlockItem();
    } else {
        return TBlockItem::Zero();
    }
}

} // namespace NYql::NUdf
