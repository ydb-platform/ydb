#pragma once
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

namespace NKikimr::NViewer {

inline TString GetLogPrefix() {
    return {};
}

}

#define Y_ENSURE_LOG(cond, stream) if (!(cond)) { LOG_ERROR_S(*TlsActivationContext, NKikimrServices::VIEWER, GetLogPrefix() << "Failed condition \"" << #cond << "\" " << stream); }
