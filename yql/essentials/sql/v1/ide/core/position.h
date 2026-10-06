#pragma once

#include <yql/essentials/utils/meta/hash.h>

#include <util/stream/output.h>

namespace NSQLPureAST {

struct TPosition {
    ui32 Line = 0;
    ui32 Column = 0;

    friend bool operator==(const TPosition& lhs, const TPosition& rhs) = default;
    friend bool operator<(const TPosition& lhs, const TPosition& rhs);
};

} // namespace NSQLPureAST

namespace NYql::NReflection {

YQL_DEFINE_REFLECTING(NSQLPureAST::TPosition, (Line)(Column));

} // namespace NYql::NReflection

YQL_DERIVE_HASH(NSQLPureAST::TPosition);
