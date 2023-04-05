#pragma once

#include <library/cpp/actors/core/actorsystem.h>
#include <library/cpp/actors/core/log.h>

namespace NPGW {

enum EService : NActors::NLog::EComponent {
    MIN = 1100,
    Logger,
    PGYDB,
    MAX
};

inline const TString& GetEServiceName(NActors::NLog::EComponent component) {
    static const TString loggerName("LOGGER");
    static const TString pgYdbName("PG-YDB");
    static const TString unknownName("UNKNOWN");
    switch (component) {
    case EService::Logger:
        return loggerName;
    case EService::PGYDB:
        return pgYdbName;
    default:
        return unknownName;
    }
}


#define BLOG_D(stream) LOG_DEBUG_S(*NActors::TlsActivationContext, EService::PGYDB, stream)
#define BLOG_I(stream) LOG_INFO_S(*NActors::TlsActivationContext, EService::PGYDB, stream)
#define BLOG_W(stream) LOG_WARN_S(*NActors::TlsActivationContext, EService::PGYDB, stream)
#define BLOG_NOTICE(stream) LOG_NOTICE_S(*NActors::TlsActivationContext, EService::PGYDB, stream)
#define BLOG_ERROR(stream) LOG_ERROR_S(*NActors::TlsActivationContext, EService::PGYDB, stream)


}
