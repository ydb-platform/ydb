#pragma once

#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/library/aclib/aclib.h>

namespace NKikimr::NSysView::NAuth {

bool CanAccessUser(const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, const TString& user, const TIntrusivePtr<TSecurityObject>& securityObject);

}
