#pragma once

#include <ydb/core/persqueue/public/utils.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/control_plane.h>
#include <span>
#include <vector>

namespace NKikimr::NPQ::NMirror {

    enum class EPartitionAction {
        Modify,
        Create,
    };

    struct TPartitionWithBounds {
        ui32 Id;
        EPartitionAction Action;
        std::optional<std::string> FromBound;
        std::optional<std::string> ToBound;
    };

    struct TRootPartitionsMismatch {
        std::vector<TPartitionWithBounds> AlterRootPartitions;
        std::optional<std::string> Error;
    };

    struct TMirrorGraphComparisonResult {
        std::optional<TRootPartitionsMismatch> RootPartitionsMismatch;
    };

    TMirrorGraphComparisonResult ComparePartitionGraphs(const TPartitionGraph& target, std::span<const NYdb::NTopic::TPartitionInfo> source);
} // namespace NKikimr::NPQ::NMirror
