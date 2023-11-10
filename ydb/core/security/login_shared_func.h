#pragma once

#include <util/generic/string.h>
#include <util/generic/ptr.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

namespace NKikimrProto {
    class TAuthConfig;
};

namespace NKikimr {

struct TAuthCredentials {
    enum class EAuthType {
        Internal,
        Ldap
    };

    EAuthType AuthType = EAuthType::Internal;
    TString Login;
    TString Password;
};

struct TSendParameters {
    TActorId Recipient;
    THolder<IEventBase> Event;
};

THolder<NSchemeCache::TSchemeCacheNavigate> CreateNavigateKeySetRequest(const TString& pathToDatabase);
TAuthCredentials PrepareCredentials(const TString& login, const TString& password, const NKikimrProto::TAuthConfig& config);
NKikimrScheme::TEvLogin CreateLoginRequest(const TAuthCredentials& credentials, const NKikimrProto::TAuthConfig& config);
TSendParameters GetSendParameters(const TAuthCredentials& credentials, const TString& pathToDatabase);

} // namespace NKikimr
