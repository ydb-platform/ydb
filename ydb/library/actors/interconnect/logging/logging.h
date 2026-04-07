#pragma once

#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/protos/services_common.pb.h>

#define LOG_LOG_IC_X(component, marker, priority, ...)                                                                                   \
    do {                                                                                                                               \
        LOG_LOG(this->GetActorContext(), (priority), (component), "%s " marker " %s", LogPrefix.data(), Sprintf(__VA_ARGS__).data()); \
    } while (false)

#define LOG_LOG_NET_X(priority, NODE_ID, FMT, ...) \
    do { \
        const TActorContext& ctx = this->GetActorContext(); \
        LOG_LOG(ctx, (priority), ::NActorsServices::INTERCONNECT_NETWORK, "[%" PRIu32 " <-> %" PRIu32 "] %s", \
                ctx.SelfID.NodeId(), (NODE_ID), Sprintf(FMT, __VA_ARGS__).data()); \
    } while (false)

#define LOG_LOG_IC(component, marker, priority, ...)                                                                                   \
    do {                                                                                                                               \
        LOG_LOG(::NActors::TActivationContext::AsActorContext(), (priority), (component), "%s " marker " %s", LogPrefix.data(), Sprintf(__VA_ARGS__).data()); \
    } while (false)

#define LOG_LOG_NET(priority, NODE_ID, FMT, ...) \
    do { \
        const TActorContext& ctx = ::NActors::TActivationContext::AsActorContext(); \
        LOG_LOG(ctx, (priority), ::NActorsServices::INTERCONNECT_NETWORK, "[%" PRIu32 " <-> %" PRIu32 "] %s", \
                ctx.SelfID.NodeId(), (NODE_ID), Sprintf(FMT, __VA_ARGS__).data()); \
    } while (false)

#define LOG_EMER_IC(marker, ...) LOG_LOG_IC(::NActorsServices::INTERCONNECT, marker, ::NActors::NLog::PRI_EMER, __VA_ARGS__)
#define LOG_ALERT_IC(marker, ...) LOG_LOG_IC(::NActorsServices::INTERCONNECT, marker, ::NActors::NLog::PRI_ALERT, __VA_ARGS__)
#define LOG_CRIT_IC(marker, ...) LOG_LOG_IC(::NActorsServices::INTERCONNECT, marker, ::NActors::NLog::PRI_CRIT, __VA_ARGS__)
#define LOG_ERROR_IC(marker, ...) LOG_LOG_IC(::NActorsServices::INTERCONNECT, marker, ::NActors::NLog::PRI_ERROR, __VA_ARGS__)
#define LOG_WARN_IC(marker, ...) LOG_LOG_IC(::NActorsServices::INTERCONNECT, marker, ::NActors::NLog::PRI_WARN, __VA_ARGS__)
#define LOG_NOTICE_IC(marker, ...) LOG_LOG_IC(::NActorsServices::INTERCONNECT, marker, ::NActors::NLog::PRI_NOTICE, __VA_ARGS__)
#define LOG_INFO_IC(marker, ...) LOG_LOG_IC(::NActorsServices::INTERCONNECT, marker, ::NActors::NLog::PRI_INFO, __VA_ARGS__)
#define LOG_DEBUG_IC(marker, ...) LOG_LOG_IC(::NActorsServices::INTERCONNECT, marker, ::NActors::NLog::PRI_DEBUG, __VA_ARGS__)
#define LOG_TRACE_IC(marker, ...) LOG_LOG_IC(::NActorsServices::INTERCONNECT, marker, ::NActors::NLog::PRI_TRACE, __VA_ARGS__)

#define LOG_EMER_IC_SESSION(marker, ...) LOG_LOG_IC(::NActorsServices::INTERCONNECT_SESSION, marker, ::NActors::NLog::PRI_EMER, __VA_ARGS__)
#define LOG_ALERT_IC_SESSION(marker, ...) LOG_LOG_IC(::NActorsServices::INTERCONNECT_SESSION, marker, ::NActors::NLog::PRI_ALERT, __VA_ARGS__)
#define LOG_CRIT_IC_SESSION(marker, ...) LOG_LOG_IC(::NActorsServices::INTERCONNECT_SESSION, marker, ::NActors::NLog::PRI_CRIT, __VA_ARGS__)
#define LOG_ERROR_IC_SESSION(marker, ...) LOG_LOG_IC(::NActorsServices::INTERCONNECT_SESSION, marker, ::NActors::NLog::PRI_ERROR, __VA_ARGS__)
#define LOG_WARN_IC_SESSION(marker, ...) LOG_LOG_IC(::NActorsServices::INTERCONNECT_SESSION, marker, ::NActors::NLog::PRI_WARN, __VA_ARGS__)
#define LOG_NOTICE_IC_SESSION(marker, ...) LOG_LOG_IC(::NActorsServices::INTERCONNECT_SESSION, marker, ::NActors::NLog::PRI_NOTICE, __VA_ARGS__)
#define LOG_INFO_IC_SESSION(marker, ...) LOG_LOG_IC(::NActorsServices::INTERCONNECT_SESSION, marker, ::NActors::NLog::PRI_INFO, __VA_ARGS__)
#define LOG_DEBUG_IC_SESSION(marker, ...) LOG_LOG_IC(::NActorsServices::INTERCONNECT_SESSION, marker, ::NActors::NLog::PRI_DEBUG, __VA_ARGS__)

#define LOG_NOTICE_NET(NODE_ID, FMT, ...) LOG_LOG_NET(::NActors::NLog::PRI_NOTICE, NODE_ID, FMT, __VA_ARGS__)
#define LOG_DEBUG_NET(NODE_ID, FMT, ...) LOG_LOG_NET(::NActors::NLog::PRI_DEBUG, NODE_ID, FMT, __VA_ARGS__)

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
