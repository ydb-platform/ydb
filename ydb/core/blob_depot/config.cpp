#include "config.h"

#include <ydb/core/protos/config.pb.h>

namespace NKikimr::NBlobDepot {

    TDuration GetS3PutTimeout(const NKikimrSchemeOp::TS3Settings& settings,
            const NKikimrConfig::TAwsClientConfig& defaults) {
        const auto httpMs = settings.HasHttpRequestTimeoutMs() ? settings.GetHttpRequestTimeoutMs() : defaults.GetHttpRequestTimeoutMs();
        return ClampVal(TDuration::MilliSeconds(httpMs), DefaultS3PutTimeout, MaxS3PutTimeout);
    }

} // NKikimr::NBlobDepot
