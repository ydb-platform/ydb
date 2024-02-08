#pragma once

#define SA_LOG_T(stream) LOG_TRACE_S((TlsActivationContext->AsActorContext()), NKikimrServices::STATISTICS, stream)
#define SA_LOG_D(stream) LOG_DEBUG_S((TlsActivationContext->AsActorContext()), NKikimrServices::STATISTICS, stream)
#define SA_LOG_I(stream) LOG_INFO_S((TlsActivationContext->AsActorContext()), NKikimrServices::STATISTICS, stream)
#define SA_LOG_N(stream) LOG_NOTICE_S((TlsActivationContext->AsActorContext()), NKikimrServices::STATISTICS, stream)
#define SA_LOG_W(stream) LOG_WARN_S((TlsActivationContext->AsActorContext()), NKikimrServices::STATISTICS, stream)
#define SA_LOG_E(stream) LOG_ERROR_S((TlsActivationContext->AsActorContext()), NKikimrServices::STATISTICS, stream)
#define SA_LOG_CRIT(stream) LOG_CRIT_S((TlsActivationContext->AsActorContext()), NKikimrServices::STATISTICS, stream)
