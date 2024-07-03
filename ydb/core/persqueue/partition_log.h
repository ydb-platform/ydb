#pragma once

#include <ydb/library/actors/core/log.h>
#include <util/generic/string.h>

namespace NKikimr::NPQ {

inline TString LogPrefix() { return {}; }

#define PQ_LOG_T(stream) LOG_TRACE_S(*NActors::TlsActivationContext, NKikimrServices::PERSQUEUE, LogPrefix() << stream)
#define PQ_LOG_D(stream) LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::PERSQUEUE, LogPrefix() << stream)
#define PQ_LOG_I(stream) LOG_INFO_S(*NActors::TlsActivationContext, NKikimrServices::PERSQUEUE, LogPrefix() << stream)
#define PQ_LOG_W(stream) LOG_WARN_S(*NActors::TlsActivationContext, NKikimrServices::PERSQUEUE, LogPrefix() << stream)
#define PQ_LOG_NOTICE(stream) LOG_NOTICE_S(*NActors::TlsActivationContext, NKikimrServices::PERSQUEUE, LogPrefix() << stream)
#define PQ_LOG_ERROR(stream) LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::PERSQUEUE, LogPrefix() << stream)
#define PQ_LOG_CRIT(stream) LOG_CRIT_S(*NActors::TlsActivationContext, NKikimrServices::PERSQUEUE, LogPrefix() << stream)

} // namespace NKikimr::NPQ
