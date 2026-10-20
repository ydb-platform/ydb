#pragma once

#include "rows.h"

namespace NYdb::NWasm::NAuth {

inline constexpr uint32_t Version = 1;
inline constexpr uint32_t MaxTicketBytes = 16384;
inline constexpr uint32_t MaxParametersBytes = 8192;

// Followed by ParametersBytes UTF-8 JSON public parameters and SecretCount
// opaque uint64 handles in manifest order. Handles are scoped to this auth call.
struct TRequest {
    uint32_t AbiVersion = Version;
    uint32_t ParametersBytes = 0;
    uint32_t SecretCount = 0;
    uint32_t Reserved = 0;
};

// Followed by TicketBytes of printable ASCII. TTL starts at refresh start,
// not receipt, so network latency cannot extend the issuer's lifetime.
struct TResult {
    uint32_t AbiVersion = Version;
    uint32_t TicketBytes = 0;
    uint64_t LifetimeMs = 0;
};

static_assert(sizeof(TRequest) == 16 && sizeof(TResult) == 16);

inline bool ReadResult(std::string_view bytes, TResult& result, std::string_view& ticket) {
    NServices::TRowReader reader(bytes);
    if (!reader.Get(result) || result.AbiVersion != Version || !result.LifetimeMs || result.LifetimeMs > 86400000 || !result.TicketBytes ||
        result.TicketBytes > MaxTicketBytes || reader.Remaining().size() != result.TicketBytes) {
        return false;
    }
    ticket = reader.Remaining();
    for (unsigned char c : ticket) {
        if (c < 32 || c >= 127) {
            return false;
        }
    }
    return true;
}
}   // namespace NYdb::NWasm::NAuth
