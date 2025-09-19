#pragma once

#if defined BLOG_D || defined BLOG_I || defined BLOG_ERROR || defined BLOG_TRACE
#error log macro definition clash
#endif

#define BLOG_D(stream) LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::TOKEN_MANAGER, stream)
#define BLOG_TRACE(stream) LOG_TRACE_S(*NActors::TlsActivationContext, NKikimrServices::TOKEN_MANAGER, stream)
#define BLOG_ERROR(stream) LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::TOKEN_MANAGER, stream)
#define BLOG_W(stream) LOG_WARN_S(*NActors::TlsActivationContext, NKikimrServices::TOKEN_MANAGER, stream)
