#pragma once

#include <util/string/vector.h>

namespace NYdb::NConsoleClient {
    enum class ETopicMetadataField {
        Body /* "body" */,
        WriteTime /* "write_time" */,
        CreateTime /* "create_time" */,
        MessageGroupID /* "message_group_id" */,
        Offset /* "offset" */,
        SeqNo /* "seq_no" */,
        Meta /* "meta" */,
    };

    enum class ETransformBody {
        None = 0 /* "none" */,
        Base64 = 1 /* "base64" */,
    };
} // namespace NYdb::NConsoleClient