#pragma once

#include <compare>
#include <util/stream/output.h>
#include <util/system/types.h>

namespace NKafka {
    struct TProducerInstanceId {
        i64 Id;
        i32 Epoch;
    
        auto operator<=>(TProducerInstanceId const&) const = default;
    };
    inline IOutputStream& operator<<(IOutputStream& os, const TProducerInstanceId& obj) {
        os << "{Id: " << obj.Id << ", Epoch: " << obj.Epoch << "}";
        return os;
    }

    static const TProducerInstanceId INVALID_PRODUCER_INSTANCE_ID = {-1, -1};
    
    struct TProducerInstanceIdHashFn {
        size_t operator()(const TProducerInstanceId& producerInstanceId) const {
            return std::hash<i64>()(producerInstanceId.Id) ^ std::hash<i32>()(producerInstanceId.Epoch);
        }
    };
} // namespace NKafka