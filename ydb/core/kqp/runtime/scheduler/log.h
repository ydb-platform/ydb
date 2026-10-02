#pragma once

#include <ydb/library/actors/core/log.h>

#define LOG_T(stream) LOG_TRACE_S (*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE_SCHEDULER, stream)
#define LOG_D(stream) LOG_DEBUG_S (*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE_SCHEDULER, stream)
#define LOG_I(stream) LOG_INFO_S  (*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE_SCHEDULER, stream)
#define LOG_N(stream) LOG_NOTICE_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE_SCHEDULER, stream)
#define LOG_W(stream) LOG_WARN_S  (*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE_SCHEDULER, stream)
#define LOG_E(stream) LOG_ERROR_S (*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE_SCHEDULER, stream)
#define LOG_C(stream) LOG_CRIT_S  (*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE_SCHEDULER, stream)
