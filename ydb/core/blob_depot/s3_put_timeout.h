#pragma once

#include <util/datetime/base.h>

namespace NKikimrConfig {
    class TAwsClientConfig;
}

namespace NKikimrSchemeOp {
    class TS3Settings;
}

namespace NKikimr::NBlobDepot {

    // Includes time waiting for a balancer, an SDK worker, and the upload itself.
    inline constexpr TDuration DefaultS3PutTimeout = TDuration::Minutes(10);
    inline constexpr TDuration MaxS3PutTimeout = TDuration::Days(1);

    TDuration GetS3PutTimeout(const NKikimrSchemeOp::TS3Settings& settings,
        const NKikimrConfig::TAwsClientConfig& defaults);

} // namespace NKikimr::NBlobDepot
