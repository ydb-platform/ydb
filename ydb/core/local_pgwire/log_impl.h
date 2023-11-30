#pragma once

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

#define BLOG_D(stream) LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::LOCAL_PGWIRE, stream)
#define BLOG_I(stream) LOG_INFO_S(*NActors::TlsActivationContext, NKikimrServices::LOCAL_PGWIRE, stream)
#define BLOG_W(stream) LOG_WARN_S(*NActors::TlsActivationContext, NKikimrServices::LOCAL_PGWIRE, stream)
#define BLOG_NOTICE(stream) LOG_NOTICE_S(*NActors::TlsActivationContext, NKikimrServices::LOCAL_PGWIRE, stream)
#define BLOG_ERROR(stream) LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::LOCAL_PGWIRE, stream)

#define BLOG_ENSURE(condition) do{if(!condition)BLOG_ERROR(#condition);}while(false)
