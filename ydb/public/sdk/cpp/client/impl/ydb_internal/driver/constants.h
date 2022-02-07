#pragma once

#include <ydb/public/sdk/cpp/client/impl/ydb_internal/internal_header.h>

namespace NYdb {

constexpr ui64 TCP_KEEPALIVE_IDLE = 30; // The time the connection needs to remain idle
                                        // before TCP starts sending keepalive probes, seconds
constexpr ui64 TCP_KEEPALIVE_COUNT = 5; // The maximum number of keepalive probes TCP should send before
                                        // dropping the connection
constexpr ui64 TCP_KEEPALIVE_INTERVAL = 10; // The time between individual keepalive probes, seconds

} // namespace NYdb
