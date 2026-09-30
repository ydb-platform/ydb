#pragma once

#include <cstddef>

namespace NKikimr::NKqp::NQueryTraceSettings {

inline constexpr size_t MAX_QUERY_TEXT_BYTES = 64 * 1024;
inline constexpr size_t MAX_STAGES = 128;
inline constexpr size_t MAX_TASKS_PER_STAGE = 5;
inline constexpr size_t MAX_TASK_NAME_OPERATIONS = 2;
inline constexpr size_t MAX_NODES_PER_STAGE = 32;
inline constexpr size_t MAX_SHARD_EVENTS = 32;
inline constexpr size_t MAX_ACTIVE_SHARD_READS = 32;
inline constexpr size_t MAX_RETAINED_READ_SHARDS = 32;
inline constexpr size_t MAX_INTERESTING_READ_SHARDS = 5;
} // namespace NKikimr::NKqp::NQueryTraceSettings
