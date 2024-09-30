#pragma once
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

namespace NKikimr::NViewer {

inline TString GetLogPrefix() {
    return {};
}

}

#define BLOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::VIEWER, GetLogPrefix() << stream)
#define BLOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::VIEWER, GetLogPrefix() << stream)
#define BLOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::VIEWER, GetLogPrefix() << stream)
#define BLOG_NOTICE(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::VIEWER, GetLogPrefix() << stream)
#define BLOG_ERROR(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::VIEWER, GetLogPrefix() << stream)
#define BLOG_CRIT(stream) LOG_CRIT_S(*TlsActivationContext, NKikimrServices::VIEWER, GetLogPrefix() << stream)
#define BLOG_TRACE(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::VIEWER, GetLogPrefix() << stream)
#define Y_ENSURE_LOG(cond, stream) if (!(cond)) { BLOG_ERROR("Failed condition \"" << #cond << "\" " << stream); }
