#pragma once

#include <ydb/library/aclib/aclib.h>

namespace NKikimr {

class ISecureRequestIface {
public:
    virtual ~ISecureRequestIface() = default;
    virtual TString GetUserSID() const = 0;
    virtual TString GetSanitizedToken() const = 0;
    virtual TIntrusiveConstPtr<NACLib::TUserToken> GetParsedToken() const = 0;
};

}
