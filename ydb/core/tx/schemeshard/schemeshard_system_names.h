#pragma once

#include <util/generic/vector.h>
#include <util/generic/string.h>

#include <ydb/core/base/appdata_fwd.h>

namespace NACLib {
class TUserToken;
}  // namespace NACLib

namespace NKikimr::NSchemeShard {

bool CheckReservedName(const TString& name, const NACLib::TUserToken* userToken, const TVector<TString>& adminSids, TString& explain);
bool CheckReservedName(const TString& name, const TAppData* appData, const NACLib::TUserToken* userToken, TString& explain);

}  // namespace NKikimr::NSchemeShard
