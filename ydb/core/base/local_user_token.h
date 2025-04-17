#pragma once

#include <util/generic/string.h>
#include <ydb/library/aclib/aclib.h>


namespace NLoginProto {
class TSecurityState;
}
namespace NLogin {
class TLoginProvider;
}

namespace NKikimr {

// Recreates user token from local user login/sid and it's database login provider or security state.
// Token should be used to determine access level only (e.g. cluster/database admin status),
// and not for authentication.
// See methods in ydb/core/base/auth.h.
NACLib::TUserToken BuildLocalUserToken(const NLogin::TLoginProvider& loginProvider, const TString& user);
NACLib::TUserToken BuildLocalUserToken(const NLoginProto::TSecurityState& state, const TString& user);

}
