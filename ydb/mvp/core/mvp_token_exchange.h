#pragma once

#include <ydb/mvp/core/protos/mvp.pb.h>

#include <util/generic/vector.h>

namespace NMVP {

struct TOAuth2ExchangeData {
    TString GrantType;
    TString RequestedTokenType;
    TString Audience;
    TVector<TString> Scopes;
    TVector<TString> Resources;
    TString SubjectToken;
    TString SubjectTokenType;
    TString ActorToken;
    TString ActorTokenType;
};

bool TryBuildOAuth2ExchangeData(const NMvp::TOAuth2Exchange* tokenExchangeInfo,
                                TOAuth2ExchangeData& prepared,
                                TString& error);

} // namespace NMVP
