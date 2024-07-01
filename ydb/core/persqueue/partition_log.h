#pragma once

#include <ydb/library/actors/core/log.h>
#include <util/generic/string.h>

namespace NKikimr::NPQ {

inline TString LogPrefix() { return {}; }

#define PQ_LOG_T(stream) if (NActors::TlsActivationContext) { LOG_TRACE_S(*NActors::TlsActivationContext, NKikimrServices::PERSQUEUE, LogPrefix() << stream); }
#define PQ_LOG_D(stream) if (NActors::TlsActivationContext) { LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::PERSQUEUE, LogPrefix() << stream); }
#define PQ_LOG_I(stream) if (NActors::TlsActivationContext) { LOG_INFO_S(*NActors::TlsActivationContext, NKikimrServices::PERSQUEUE, LogPrefix() << stream); }
#define PQ_LOG_W(stream) if (NActors::TlsActivationContext) { LOG_WARN_S(*NActors::TlsActivationContext, NKikimrServices::PERSQUEUE, LogPrefix() << stream); }
#define PQ_LOG_NOTICE(stream) if (NActors::TlsActivationContext) { LOG_NOTICE_S(*NActors::TlsActivationContext, NKikimrServices::PERSQUEUE, LogPrefix() << stream); }
#define PQ_LOG_ERROR(stream) if (NActors::TlsActivationContext) { LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::PERSQUEUE, LogPrefix() << stream); }
#define PQ_LOG_CRIT(stream) if (NActors::TlsActivationContext) { LOG_CRIT_S(*NActors::TlsActivationContext, NKikimrServices::PERSQUEUE, LogPrefix() << stream); }

} // namespace NKikimr::NPQ
