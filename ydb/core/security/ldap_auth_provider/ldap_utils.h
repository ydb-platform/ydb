#pragma once

#include <ydb/core/protos/auth.pb.h>

namespace NKikimr {

class TSearchFilterCreator {
public:
    TSearchFilterCreator(const NKikimrProto::TLdapAuthentication& settings);
    TString GetFilter(const TString& userName) const;

private:
    TString GetFormatSearchFilter(const TString& userName) const;

private:
    const NKikimrProto::TLdapAuthentication& Settings;
};

} // namespace NKikimr
