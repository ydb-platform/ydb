#pragma once

#include <util/string/vector.h>

namespace NYdb::NConsoleClient {
    enum class ETopicMetadataField {
        Body /* "body" */,
        PartitionID /* "partition_id" */,
        WriteTime /* "write_time" */,
        CreateTime /* "create_time" */,
        ProducerID /* "producer_id" */,
        Offset /* "offset" */,
        SeqNo /* "seq_no" */,
        Meta /* meta */,
        MessageMeta /* "message_meta" */,
        SessionMeta  /* "session_meta" */
    };

    enum class ETransformBody {
        None = 0 /* "none" */,
        Base64 = 1 /* "base64" */,
    };
} // namespace NYdb::NConsoleClient
