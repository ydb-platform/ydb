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

class TLdapUrisCreator {
public:
    TLdapUrisCreator(const NKikimrProto::TLdapAuthentication& settings, ui32 configuredPort);

    TString GetUris() const;
    ui32 GetConfiguredPort() const;

private:
    TString CreateUrisList() const;
    TString CreateUri(const TString& address) const;

private:
    const NKikimrProto::TLdapAuthentication& Settings;
    const TString Scheme;
    const ui32 ConfiguredPort;
    mutable TString Uris;
};

} // namespace NKikimr
