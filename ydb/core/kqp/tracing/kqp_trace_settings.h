#pragma once

#include <cstddef>

namespace NKikimr::NKqp::NQueryTraceSettings {

inline constexpr size_t MaxQueryTextBytes = 64 * 1024;
inline constexpr size_t MaxStages = 128;
inline constexpr size_t MaxTasksPerStage = 5;
inline constexpr size_t MaxTaskNameOperations = 2;
inline constexpr size_t MaxNodesPerStage = 32;
inline constexpr size_t MaxShardEvents = 32;
inline constexpr size_t MaxActiveShardReads = 32;
inline constexpr size_t MaxRetainedReadShards = 32;
inline constexpr size_t MaxInterestingReadShards = 5;

}
