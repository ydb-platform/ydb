#pragma once

#include <ydb/library/aclib/aclib.h>

namespace NKikimr::NSQS {

enum class EACLSourceType : ui32 {
    Unknown,
    RootDir,
    AccountDir,
    QueueDir,
    Custom
};

EACLSourceType GetActionACLSourceType(const TString& actionName);
ui32 GetActionRequiredAccess(const TString& actionName);
ui32 GetACERequiredAccess(const TString& aceName);
TString GetActionMatchingACE(const TString& actionName);
TVector<TStringBuf> GetAccessMatchingACE(const ui32 access);

} // namespace NKikimr::NSQS
