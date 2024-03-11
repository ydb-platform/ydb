#pragma once

#include <util/generic/string.h>

namespace NKafka {
    static const i8 TOPIC_RESOURCE_TYPE = 2;

    static const TString RETENTION_MS_CONFIG_NAME = "retention.ms";
    static const TString RETENTION_BYTES_CONFIG_NAME = "retention.bytes";
    static const TString COMPRESSION_TYPE = "compression.type";
}
