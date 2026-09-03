#pragma once

#include <util/generic/array_ref.h>
#include <util/generic/vector.h>
#include <util/str_stl.h>

namespace NYql::NUdf {

template <typename T>
class TUnboxedValueComparatorStreamView {
public:
    explicit TUnboxedValueComparatorStreamView(TArrayRef<const T> data)
        : Data_(data)
    {
    }

    TArrayRef<const T> Data() const {
        return Data_;
    }

private:
    TArrayRef<const T> Data_;
};

} // namespace NYql::NUdf
