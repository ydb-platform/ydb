#pragma once

#include <util/system/types.h>

namespace NSQLTranslationV1 {

struct TTokenIndexRange {
    ui32 Start = 0;
    ui32 End = 0;

    bool operator==(const TTokenIndexRange& other) const = default;

    bool IsEmpty() const;
};

template <typename T>
TTokenIndexRange GetTokenIndexRange(const T& x) {
    return {x.GetStartTokenIndex(), x.GetEndTokenIndex()};
}

} // namespace NSQLTranslationV1
