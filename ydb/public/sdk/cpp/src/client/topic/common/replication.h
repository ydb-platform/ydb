#pragma once

#include <cstdint>
#include <functional>

namespace NYdb::inline Dev::NTopic {

    // Metadata is a range of key/value pairs; a projection also supports protobuf items.
    template <class TMetadata, class TProjection = std::identity>
    bool IsReplicationServiceMessage(uint64_t offset, const TMetadata& metadata, TProjection projection = {}) {
        if (offset != 0) {
            return false;
        }
        for (const auto& item : metadata) {
            const auto& [key, value] = projection(item);
            if (key == "_ydb_service_replication_message" && value == "true") {
                return true;
            }
        }
        return false;
    }

} // namespace NYdb::inline Dev::NTopic
