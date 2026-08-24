#pragma once

#if defined BLOG_ERROR || defined BLOG_WARN || defined BLOG_NOTICE || defined BLOG_DEBUG || defined BLOG_TRACE
#error log macro definition clash
#endif

#define BLOG_ERROR(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::TICKET_PARSER, stream)
#define BLOG_WARN(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::TICKET_PARSER, stream)
#define BLOG_NOTICE(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::TICKET_PARSER, stream)
#define BLOG_DEBUG(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TICKET_PARSER, stream)
#define BLOG_TRACE(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TICKET_PARSER, stream)
