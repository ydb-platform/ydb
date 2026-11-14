#pragma once

#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/struct_log/structured_message.h>
#include <ydb/library/actors/protos/services_common.pb.h>

namespace NActors {
    class TInterconnectLoggingBase {
    protected:
        NActors::NStructuredLog::TStructuredMessage LogPrefix;

    public:
        TInterconnectLoggingBase() = default;

        TInterconnectLoggingBase(NActors::NStructuredLog::TStructuredMessage&& prefix)
            : LogPrefix(std::move(prefix))
        {
        }

        void SetPrefix(NActors::NStructuredLog::TStructuredMessage&& prefix) {
            LogPrefix = std::move(prefix);
        }
    };
}
