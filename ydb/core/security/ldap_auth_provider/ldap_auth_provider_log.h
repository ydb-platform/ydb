#pragma once

#include <ydb/library/actors/core/log.h>

#if defined LDAP_LOG_D || defined LDAP_LOG_W || defined LDAP_LOG_ERROR || defined LDAP_LOG_TRACE
#error log macro definition clash
#endif

#define LDAP_LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::LDAP_AUTH_PROVIDER, stream)
#define LDAP_LOG_TRACE(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::LDAP_AUTH_PROVIDER, stream)
#define LDAP_LOG_ERROR(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::LDAP_AUTH_PROVIDER, stream)
#define LDAP_LOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::LDAP_AUTH_PROVIDER, stream)
