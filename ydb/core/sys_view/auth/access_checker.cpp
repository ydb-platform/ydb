#include "access_checker.h"

namespace NKikimr::NSysView::NAuth {

bool CanAccessUser(const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, const TString& user, const TIntrusivePtr<TSecurityObject>& rootSecurityObject) {
    Y_UNUSED(userToken, user, rootSecurityObject);
    return true;
}

}
