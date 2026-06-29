#pragma once

#include "kafka_log.h"

#define KAFKA_LOG_T(stream) LOG_TRACE_S(*NActors::TlsActivationContext, NKikimrServices::KAFKA_PROXY, LogPrefix() << stream)
#define KAFKA_LOG_D(stream) LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KAFKA_PROXY, LogPrefix() << stream)
#define KAFKA_LOG_I(stream) LOG_INFO_S(*NActors::TlsActivationContext, NKikimrServices::KAFKA_PROXY, LogPrefix() << stream)
#define KAFKA_LOG_W(stream) LOG_WARN_S(*NActors::TlsActivationContext, NKikimrServices::KAFKA_PROXY, LogPrefix() << stream)
#define KAFKA_LOG_NOTICE(stream) LOG_NOTICE_S(*NActors::TlsActivationContext, NKikimrServices::KAFKA_PROXY, LogPrefix() << stream)
#define KAFKA_LOG_ERROR(stream) LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::KAFKA_PROXY, LogPrefix() << stream)
#define KAFKA_LOG_CRIT(stream) LOG_CRIT_S(*NActors::TlsActivationContext, NKikimrServices::KAFKA_PROXY, LogPrefix() << stream)
