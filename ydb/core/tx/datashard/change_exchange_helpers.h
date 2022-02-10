#pragma once

#include "defs.h"

#define LOG_T(stream) LOG_TRACE_S (*TlsActivationContext, NKikimrServices::CHANGE_EXCHANGE, GetLogPrefix() << stream)
#define LOG_D(stream) LOG_DEBUG_S (*TlsActivationContext, NKikimrServices::CHANGE_EXCHANGE, GetLogPrefix() << stream)
#define LOG_I(stream) LOG_INFO_S  (*TlsActivationContext, NKikimrServices::CHANGE_EXCHANGE, GetLogPrefix() << stream)
#define LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::CHANGE_EXCHANGE, GetLogPrefix() << stream)
#define LOG_W(stream) LOG_WARN_S  (*TlsActivationContext, NKikimrServices::CHANGE_EXCHANGE, GetLogPrefix() << stream)
#define LOG_E(stream) LOG_ERROR_S (*TlsActivationContext, NKikimrServices::CHANGE_EXCHANGE, GetLogPrefix() << stream)
#define LOG_C(stream) LOG_CRIT_S  (*TlsActivationContext, NKikimrServices::CHANGE_EXCHANGE, GetLogPrefix() << stream)

namespace NKikimr {
namespace NDataShard {

struct TDataShardId {
    ui64 TabletId;
    ui64 Generation;
    TActorId ActorId;
};

} // NDataShard
} // NKikimr
