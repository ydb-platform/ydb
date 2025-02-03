#pragma once

#include "appdata_fwd.h"
#include <ydb/library/aclib/aclib.h>

namespace NKikimr {

bool IsAdministrator(const TAppData* appData, const TString& userToken);

bool IsAdministrator(const TAppData* appData, const NACLib::TUserToken* userToken);

}
