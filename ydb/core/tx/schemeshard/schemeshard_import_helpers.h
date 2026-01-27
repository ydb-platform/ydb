#pragma once

#include <yql/essentials/public/issue/protos/issue_severity.pb.h>

#if defined LOG_T || \
    defined LOG_D || \
    defined LOG_I || \
    defined LOG_N || \
    defined LOG_W || \
    defined LOG_W
#error log macro redefinition
#endif

#define LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::IMPORT, stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::IMPORT, stream)
#define LOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::IMPORT, stream)
#define LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::IMPORT, stream)
#define LOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::IMPORT, stream)
#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::IMPORT, stream)

namespace NKikimr::NSchemeShard {

template <typename T>
void AddIssue(T& response, const TString& message, NYql::TSeverityIds::ESeverityId severity = NYql::TSeverityIds::S_ERROR) {
    auto& issue = *response.AddIssues();
    issue.set_severity(severity);
    issue.set_message(message);
}

struct TImportInfo;

TString MakeIndexBuildUid(const TImportInfo& importInfo, ui32 itemIdx, i32 indexIdx);
TString MakeIndexBuildUid(const TImportInfo& importInfo, ui32 itemIdx);

bool NeedToBuildIndexes(const TImportInfo& importInfo, ui32 itemIdx);

class TSchemeShard;

bool ValidateImportDstPath(const TString& dstPath, TSchemeShard* ss, TString& explain);

} // NKikimr::NSchemeShard
