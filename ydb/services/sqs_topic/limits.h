#pragma once

#include <util/datetime/base.h>
#include <util/generic/strbuf.h>
#include <expected>

namespace NKikimr::NSqsTopic::V1 {

    constexpr TDuration MAX_VISIBILITY_TIMEOUT = TDuration::Hours(12);
    constexpr TDuration MIN_VISIBILITY_TIMEOUT = TDuration::Seconds(0);
    constexpr TDuration DEFAULT_MESSAGE_RETENTION_PERIOD = TDuration::Days(1);  // differs from aws sqs default
    constexpr int DEFAULT_MIN_PARTITION_COUNT = 1;
    constexpr int DEFAULT_MAX_PARTITION_COUNT = 100;

    std::expected<void, std::string> ValidateQueueName(const TStringBuf name);
} // namespace NKikimr::NSqsTopic::V1
