#pragma once

#include "mvp_log_context.h"

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/log.h>

namespace NMVP {

enum EService : NActors::NLog::EComponent {
    MIN = 257,
    Logger,
    MVP,
    GRPC,
    QUERY,
    MAX
};

inline TString GetLogPrefix() {
    return {};
}

}

using NMVP::GetLogPrefix;

#define BLOG_D(stream) LOG_DEBUG_S(*NActors::TlsActivationContext, EService::MVP, GetLogPrefix() << stream)
#define BLOG_I(stream) LOG_INFO_S(*NActors::TlsActivationContext, EService::MVP, GetLogPrefix() << stream)
#define BLOG_W(stream) LOG_WARN_S(*NActors::TlsActivationContext, EService::MVP, GetLogPrefix() << stream)
#define BLOG_NOTICE(stream) LOG_NOTICE_S(*NActors::TlsActivationContext, EService::MVP, GetLogPrefix() << stream)
#define BLOG_ERROR(stream) LOG_ERROR_S(*NActors::TlsActivationContext, EService::MVP, GetLogPrefix() << stream)
#define BLOG_GRPC_D(stream) LOG_DEBUG_S(*NActors::TlsActivationContext, EService::GRPC, GetLogPrefix() << stream)
#define BLOG_GRPC_DC(context, stream) LOG_DEBUG_S(context, EService::GRPC, stream)
#define BLOG_QUERY_I(stream) LOG_INFO_S(*NActors::TlsActivationContext, EService::QUERY, GetLogPrefix() << stream)
