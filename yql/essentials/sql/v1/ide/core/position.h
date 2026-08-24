#pragma once

#include <util/generic/hash.h>
#include <util/stream/output.h>

namespace NSQLPureAST {

struct TPosition {
    ui32 Line = 0;
    ui32 Column = 0;

    friend bool operator==(const TPosition& lhs, const TPosition& rhs) = default;
    friend bool operator<(const TPosition& lhs, const TPosition& rhs);
};

} // namespace NSQLPureAST

template <>
struct THash<NSQLPureAST::TPosition> {
    size_t operator()(const NSQLPureAST::TPosition& x) const {
        return THash<std::tuple<ui32, ui32>>()(std::tie(x.Line, x.Column));
    }
};
