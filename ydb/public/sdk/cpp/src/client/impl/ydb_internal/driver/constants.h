#pragma once

#include <src/client/impl/ydb_internal/internal_header.h>

#include <cstdint>

namespace NYdb::inline V3 {

constexpr uint64_t TCP_KEEPALIVE_IDLE = 30; // The time the connection needs to remain idle
                                        // before TCP starts sending keepalive probes, seconds
constexpr uint64_t TCP_KEEPALIVE_COUNT = 5; // The maximum number of keepalive probes TCP should send before
                                        // dropping the connection
constexpr uint64_t TCP_KEEPALIVE_INTERVAL = 10; // The time between individual keepalive probes, seconds

} // namespace NYdb
