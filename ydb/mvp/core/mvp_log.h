#pragma once

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

#define BLOG_D(stream) LOG_DEBUG_S(*NActors::TlsActivationContext, EService::MVP, stream)
#define BLOG_I(stream) LOG_INFO_S(*NActors::TlsActivationContext, EService::MVP, stream)
#define BLOG_W(stream) LOG_WARN_S(*NActors::TlsActivationContext, EService::MVP, stream)
#define BLOG_NOTICE(stream) LOG_NOTICE_S(*NActors::TlsActivationContext, EService::MVP, stream)
#define BLOG_ERROR(stream) LOG_ERROR_S(*NActors::TlsActivationContext, EService::MVP, stream)
#define BLOG_GRPC_D(stream) LOG_DEBUG_S(*NActors::TlsActivationContext, EService::GRPC, stream)
#define BLOG_GRPC_DC(context, stream) LOG_DEBUG_S(context, EService::GRPC, stream)
#define BLOG_QUERY_I(stream) LOG_INFO_S(*NActors::TlsActivationContext, EService::QUERY, stream)

}
