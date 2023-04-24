#pragma once

#include "pg_log.h"

#define BLOG_D(stream) LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::PGWIRE, LogPrefix() << stream)
#define BLOG_I(stream) LOG_INFO_S(*NActors::TlsActivationContext, NKikimrServices::PGWIRE, LogPrefix() << stream)
#define BLOG_W(stream) LOG_WARN_S(*NActors::TlsActivationContext, NKikimrServices::PGWIRE, LogPrefix() << stream)
#define BLOG_NOTICE(stream) LOG_NOTICE_S(*NActors::TlsActivationContext, NKikimrServices::PGWIRE, LogPrefix() << stream)
#define BLOG_ERROR(stream) LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::PGWIRE, LogPrefix() << stream)

