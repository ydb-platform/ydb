#pragma once

#include <util/generic/string.h>


namespace NKikimr::NSQS {

template <typename TReq>
TString ExtractSecurityToken(const TReq& request) {
    if (request.HasCredentials()) {
        switch (request.GetCredentials().AccessKey_case()) {
            case TCredentials::kOAuthToken: {
                return request.GetCredentials().GetOAuthToken();
            }
            case TCredentials::kTvmTicket: {
                return request.GetCredentials().GetTvmTicket();
            }
            case TCredentials::kStaticCreds: {
                return request.GetCredentials().GetStaticCreds();
            }
            default: {
                // leave the token empty
            }
        }
    }

    return {};
}

} // namespace NKikimr::NSQS
