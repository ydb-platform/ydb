#pragma once

#include <util/generic/vector.h>
#include <util/generic/string.h>

#include <ydb/core/base/appdata_fwd.h>

namespace NACLib {
class TUserToken;
}  // namespace NACLib

namespace NKikimr::NSchemeShard {

struct TPathCreationContext {
    bool IsInsideBackupCollection = false;
};

bool CheckReservedName(const TString& name, const NACLib::TUserToken* userToken, const TVector<TString>& adminSids, TString& explain);
bool CheckReservedName(const TString& name, const TAppData* appData, const NACLib::TUserToken* userToken, TString& explain);

bool IsBackupServiceReservedName(const TString& name);

// Extended check that accepts path creation context for context-aware validation
bool CheckReservedName(
    const TString& name,
    const TAppData* appData,
    const NACLib::TUserToken* userToken,
    const TPathCreationContext& context,
    TString& explain);

}  // namespace NKikimr::NSchemeShard
