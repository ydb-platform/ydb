#pragma once

#include <util/system/types.h>
#include <optional>
#include <tuple>
#include <vector>

namespace NKikimr {
    struct TReadCandidateTrace {
        ui64 Score;
        ui32 SubgroupIndex;
        ui32 DiskOrder;
        bool Eligible;
        bool Slow;
    };

    struct TReadChoiceTrace {
        std::vector<TReadCandidateTrace> Candidates;
        std::optional<ui32> Selected;
        bool NewRequest = false;
        bool Complete = true;
    };

    struct TReadChoiceReplay {
        bool Valid = false;
        std::optional<ui32> Selected;
    };

    // Replay only the mirror-3-dc candidate-choice boundary. The capture already
    // incorporates readability, situation, topology distance and slow-disk flags.
    // It does not replay byte coverage, restoration or the entire Get request.
    inline TReadChoiceReplay ReplayMirrorReadChoice(const TReadChoiceTrace& record) {
        if (!record.Complete || record.Candidates.size() > 9) {
            return {};
        }
        std::optional<std::tuple<ui64, ui32>> best;
        ui32 seen = 0;
        for (const auto& candidate : record.Candidates) {
            if (candidate.SubgroupIndex >= 9 || (seen & (1u << candidate.SubgroupIndex))) {
                return {};
            }
            seen |= 1u << candidate.SubgroupIndex;
            const auto rank = std::tuple(candidate.Score, candidate.SubgroupIndex);
            if (candidate.Eligible && (!best || rank < *best)) {
                best = rank;
            }
        }
        return {true, best ? std::optional<ui32>(std::get<1>(*best)) : std::nullopt};
    }
} // namespace NKikimr
