#pragma once

#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/tuple.h>

namespace NKikimr::NMiniKQL {

// Common types used by both IBlockLayoutConverter and IScalarLayoutConverter
struct TPackResult {
    std::vector<ui8, TMKQLAllocator<ui8>> PackedTuples;
    std::vector<ui8, TMKQLAllocator<ui8>> Overflow;
    int NTuples{0};
};

using TPackedTuple = std::vector<ui8, TMKQLAllocator<ui8>>;
using TOverflow = std::vector<ui8, TMKQLAllocator<ui8>>;

} // namespace NKikimr::NMiniKQL
