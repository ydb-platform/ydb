#pragma once

#if defined EXPORT_LOG_T || \
    defined EXPORT_LOG_D || \
    defined EXPORT_LOG_I || \
    defined EXPORT_LOG_N || \
    defined EXPORT_LOG_W || \
    defined EXPORT_LOG_E || \
    defined EXPORT_LOG_C
#error log macro redefinition
#endif

#define EXPORT_LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::DATASHARD_BACKUP, "[Export] [" << LogPrefix() << "] " << stream)
#define EXPORT_LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::DATASHARD_BACKUP, "[Export] [" << LogPrefix() << "] " << stream)
#define EXPORT_LOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::DATASHARD_BACKUP, "[Export] [" << LogPrefix() << "] " << stream)
#define EXPORT_LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::DATASHARD_BACKUP, "[Export] [" << LogPrefix() << "] " << stream)
#define EXPORT_LOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::DATASHARD_BACKUP, "[Export] [" << LogPrefix() << "] " << stream)
#define EXPORT_LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::DATASHARD_BACKUP, "[Export] [" << LogPrefix() << "] " << stream)
#define EXPORT_LOG_C(stream) LOG_CRIT_S(*TlsActivationContext, NKikimrServices::DATASHARD_BACKUP, "[Export] [" << LogPrefix() << "] " << stream)
