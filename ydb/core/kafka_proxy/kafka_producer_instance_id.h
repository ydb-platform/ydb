#pragma once

#include <compare>
#include <util/system/types.h>

namespace NKafka {
    struct TProducerInstanceId {
        i64 Id;
        i32 Epoch;
    
        auto operator<=>(TProducerInstanceId const&) const = default;
    };
} // namespace NKafka