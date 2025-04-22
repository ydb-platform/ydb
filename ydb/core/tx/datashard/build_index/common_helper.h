#pragma once

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NDataShard {

#define LOG_T(stream) LOG_TRACE_S (*TlsActivationContext, NKikimrServices::BUILD_INDEX, stream)
#define LOG_D(stream) LOG_DEBUG_S (*TlsActivationContext, NKikimrServices::BUILD_INDEX, stream)
#define LOG_I(stream) LOG_INFO_S  (*TlsActivationContext, NKikimrServices::BUILD_INDEX, stream)
#define LOG_W(stream) LOG_WARN_S  (*TlsActivationContext, NKikimrServices::BUILD_INDEX, stream)
#define LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::BUILD_INDEX, stream)
#define LOG_E(stream) LOG_ERROR_S (*TlsActivationContext, NKikimrServices::BUILD_INDEX, stream)

}
