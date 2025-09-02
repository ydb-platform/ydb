#pragma once

#include <yql/essentials/public/issue/protos/issue_severity.pb.h>

#include <util/generic/string.h>

#include <vector>

#if defined LOG_T || \
    defined LOG_D || \
    defined LOG_I || \
    defined LOG_N || \
    defined LOG_W || \
    defined LOG_E
# error log macro redefinition
#endif

#define LOG_T(stream) LOG_TRACE_S((TlsActivationContext->AsActorContext()), NKikimrServices::BUILD_INDEX, LogPrefix << stream)
#define LOG_D(stream) LOG_DEBUG_S((TlsActivationContext->AsActorContext()), NKikimrServices::BUILD_INDEX, LogPrefix << stream)
#define LOG_I(stream) LOG_INFO_S((TlsActivationContext->AsActorContext()), NKikimrServices::BUILD_INDEX, LogPrefix << stream)
#define LOG_N(stream) LOG_NOTICE_S((TlsActivationContext->AsActorContext()), NKikimrServices::BUILD_INDEX, LogPrefix << stream)
#define LOG_W(stream) LOG_WARN_S((TlsActivationContext->AsActorContext()), NKikimrServices::BUILD_INDEX, LogPrefix << stream)
#define LOG_E(stream) LOG_ERROR_S((TlsActivationContext->AsActorContext()), NKikimrServices::BUILD_INDEX, LogPrefix << stream)

namespace NKikimr {

class TSerializedTableRange;

namespace NScheme {
struct TTypeInfoOrder;
}

namespace NSchemeShard {
bool PerformCrossShardUniqIndexValidation(const std::vector<NScheme::TTypeInfoOrder>& indexColumnTypeInfos, const std::vector<TString>& indexColumns, const std::vector<const TSerializedTableRange*>& ranges, TString& errorDesc);
}

}
