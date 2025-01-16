#pragma once

#include <util/datetime/base.h>
#include <util/system/defaults.h>

namespace NKikimr::NSQS {

namespace TLimits {
    static constexpr size_t MinBatchSize = 1;

    static constexpr size_t MaxBatchSize = 10;

    static constexpr size_t MaxDelaySeconds = 900;

    static constexpr size_t MaxInflightStandard = 120000;

    static constexpr size_t MaxMessageAttributes = 10;

    static constexpr size_t MaxMessageSize = (256 * 1024);

    static constexpr TDuration MaxMessageRetentionPeriod = TDuration::Days(14);

    static constexpr size_t DelaySeconds = 0;

    static constexpr size_t VisibilityTimeout = 30;

    static constexpr TDuration MaxVisibilityTimeout = TDuration::Hours(12);

    static constexpr size_t MinMaxReceiveCount = 1;

    static constexpr size_t MaxMaxReceiveCount = 1000;

    static constexpr size_t MaxTagCount = 50;
};

} // namespace NKikimr::NSQS
