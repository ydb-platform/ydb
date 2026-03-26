#include "mvp_startup_options.h"

#include <util/generic/hash_set.h>
#include <util/generic/strbuf.h>

namespace NMVP {

void TMvpStartupOptions::MigrateJwtInfoToOAuth2Exchange() {
    THashSet<TString> oauth2Names;
    for (const auto& tokenExchangeInfo : Tokens.GetOAuth2Exchange()) {
        oauth2Names.insert(tokenExchangeInfo.GetName());
    }

    static constexpr TStringBuf DEFAULT_OAUTH2_SUBJECT_TOKEN_TYPE = "urn:ietf:params:oauth:token-type:jwt";
    static constexpr TStringBuf DEFAULT_OAUTH2_YANDEX_JWT_ALG = "PS256";
    static constexpr TStringBuf DEFAULT_OAUTH2_NEBIUS_JWT_ALG = "RS256";

    for (const auto& jwtInfo : Tokens.GetJwtInfo()) {
        if (oauth2Names.contains(jwtInfo.GetName())) {
            continue;
        }

        auto* tokenExchangeInfo = Tokens.AddOAuth2Exchange();
        tokenExchangeInfo->SetName(jwtInfo.GetName());
        tokenExchangeInfo->SetTokenEndpoint(jwtInfo.GetEndpoint());

        auto* subjectCreds = tokenExchangeInfo->MutableSubjectCredentials();
        subjectCreds->SetType(NMvp::TOAuth2Exchange::TCredentials::JWT);
        subjectCreds->SetTokenType(TString(DEFAULT_OAUTH2_SUBJECT_TOKEN_TYPE));
        subjectCreds->SetPrivateKey(jwtInfo.GetPrivateKey());
        subjectCreds->SetKid(jwtInfo.GetKeyId());
        subjectCreds->SetIss(jwtInfo.GetAccountId());

        if (AccessServiceType == NMvp::yandex_v2) {
            subjectCreds->SetAlg(TString(DEFAULT_OAUTH2_YANDEX_JWT_ALG));
            if (!jwtInfo.GetAudience().empty()) {
                subjectCreds->AddAud(jwtInfo.GetAudience());
            }
        }
        if (AccessServiceType == NMvp::nebius_v1) {
            subjectCreds->SetAlg(TString(DEFAULT_OAUTH2_NEBIUS_JWT_ALG));
            subjectCreds->SetSub(jwtInfo.GetAccountId());
        }
    }

    Tokens.ClearJwtInfo();
}

} // namespace NMVP
