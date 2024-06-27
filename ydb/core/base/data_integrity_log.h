#pragma once

#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

#define LOG_DI_T(stream) LOG_TRACE_S (*TlsActivationContext, NKikimrServices::CONTINUOUS_BACKUP, stream)
#define LOG_DI_D(stream) LOG_DEBUG_S (*TlsActivationContext, NKikimrServices::CONTINUOUS_BACKUP, stream)
#define LOG_DI_I(stream) LOG_INFO_S  (*TlsActivationContext, NKikimrServices::CONTINUOUS_BACKUP, stream)
#define LOG_DI_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::CONTINUOUS_BACKUP, stream)
#define LOG_DI_W(stream) LOG_WARN_S  (*TlsActivationContext, NKikimrServices::CONTINUOUS_BACKUP, stream)
#define LOG_DI_E(stream) LOG_ERROR_S (*TlsActivationContext, NKikimrServices::CONTINUOUS_BACKUP, stream)
#define LOG_DI_C(stream) LOG_CRIT_S  (*TlsActivationContext, NKikimrServices::CONTINUOUS_BACKUP, stream)
