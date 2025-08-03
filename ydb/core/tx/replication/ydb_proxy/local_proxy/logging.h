#pragma once

#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

#define CLOG_T(ctx, stream) LOG_TRACE_S (ctx, NKikimrServices::LOCAL_YDB_PROXY, LogPrefix << stream)
#define CLOG_D(ctx, stream) LOG_DEBUG_S (ctx, NKikimrServices::LOCAL_YDB_PROXY, LogPrefix << stream)
#define CLOG_I(ctx, stream) LOG_INFO_S  (ctx, NKikimrServices::LOCAL_YDB_PROXY, LogPrefix << stream)
#define CLOG_N(ctx, stream) LOG_NOTICE_S(ctx, NKikimrServices::LOCAL_YDB_PROXY, LogPrefix << stream)
#define CLOG_W(ctx, stream) LOG_WARN_S  (ctx, NKikimrServices::LOCAL_YDB_PROXY, LogPrefix << stream)
#define CLOG_E(ctx, stream) LOG_ERROR_S (ctx, NKikimrServices::LOCAL_YDB_PROXY, LogPrefix << stream)

#define LOG_T(stream) CLOG_T(*TlsActivationContext, stream)
#define LOG_D(stream) CLOG_D(*TlsActivationContext, stream)
#define LOG_I(stream) CLOG_I(*TlsActivationContext, stream)
#define LOG_N(stream) CLOG_N(*TlsActivationContext, stream)
#define LOG_W(stream) CLOG_W(*TlsActivationContext, stream)
#define LOG_E(stream) CLOG_E(*TlsActivationContext, stream)
