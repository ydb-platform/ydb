#pragma once

#include <yql/essentials/public/issue/protos/issue_severity.pb.h>

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
