#pragma once

#include <util/generic/vector.h>
#include <algorithm>

namespace NKikimr::NActorTracing {

inline constexpr ui32 DefaultTreeFanOut = 8;

inline TVector<std::pair<ui32, TVector<ui32>>> GetDirectChildren(const TVector<ui32>& nodes, ui32 K = DefaultTreeFanOut) {
    if (nodes.empty()) {
        return {};
    }

    const ui32 groupCount = std::min<ui32>(K, static_cast<ui32>(nodes.size()));
    const ui32 baseSize = static_cast<ui32>(nodes.size()) / groupCount;
    const ui32 remainder = static_cast<ui32>(nodes.size()) % groupCount;

    TVector<std::pair<ui32, TVector<ui32>>> result;
    result.reserve(groupCount);

    ui32 offset = 0;
    for (ui32 i = 0; i < groupCount; ++i) {
        const ui32 groupSize = baseSize + (i < remainder ? 1 : 0);
        const ui32 childId = nodes[offset];
        TVector<ui32> subtree(nodes.begin() + offset + 1, nodes.begin() + offset + groupSize);
        result.emplace_back(childId, std::move(subtree));
        offset += groupSize;
    }

    return result;
}

} // namespace NKikimr::NActorTracing
