#pragma once

#include <util/generic/string.h>

namespace NKafka {
    static const i8 TOPIC_RESOURCE_TYPE = 2;

    static const TString RETENTION_MS_CONFIG_NAME = "retention.ms";
    static const TString RETENTION_BYTES_CONFIG_NAME = "retention.bytes";
    static const TString COMPRESSION_TYPE = "compression.type";
    static const TString CLEANUP_POLICY = "cleanup.policy";


    static const ui64 TRANSACTIONAL_ID_EXPIRATION_MS = 7 * 24 * 60 * 60 * 1000; // 7 days

    static const i64 NO_PRODUCER_ID = -1;
    static const i16 NO_PRODUCER_EPOCH = -1;
}
