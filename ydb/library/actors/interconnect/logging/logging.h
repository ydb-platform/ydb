#pragma once

#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/protos/services_common.pb.h>

#define LOG_LOG_IC_X(component, marker, priority, ...)                                                                                   \
    do {                                                                                                                               \
        LOG_LOG(this->GetActorContext(), (priority), (component), "%s " marker " %s", LogPrefix.data(), Sprintf(__VA_ARGS__).data()); \
    } while (false)

#define LOG_LOG_IC(component, marker, priority, ...)                                                                                   \
    do {                                                                                                                               \
        LOG_LOG(::NActors::TActivationContext::AsActorContext(), (priority), (component), "%s " marker " %s", LogPrefix.data(), Sprintf(__VA_ARGS__).data()); \
    } while (false)

namespace NActors {
    class TInterconnectLoggingBase {
    protected:
        const TString LogPrefix;

    public:
        TInterconnectLoggingBase() = default;

        TInterconnectLoggingBase(const TString& prefix)
            : LogPrefix(prefix)
        {
        }

        void SetPrefix(TString logPrefix) const {
            logPrefix.swap(const_cast<TString&>(LogPrefix));
        }
    };
}
