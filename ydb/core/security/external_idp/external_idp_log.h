#pragma once

#if defined BLOG_T || defined BLOG_D || defined BLOG_I || defined BLOG_W || defined BLOG_E
#error log macro definition clash
#endif

#define BLOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::EXTERNAL_IDP_PROVIDER, stream)
#define BLOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::EXTERNAL_IDP_PROVIDER, stream)
#define BLOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::EXTERNAL_IDP_PROVIDER, stream)
#define BLOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::EXTERNAL_IDP_PROVIDER, stream)
#define BLOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::EXTERNAL_IDP_PROVIDER, stream)
