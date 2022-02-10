#pragma once

#include <util/generic/string.h>


namespace NKikimr::NSQS {

template <typename TReq, typename TCreds>
TString ExtractSecurityToken(const TReq& request) {
    if (request.HasCredentials()) {
        switch (request.GetCredentials().AccessKey_case()) {
            case TCreds::kOAuthToken: {
                return request.GetCredentials().GetOAuthToken();
            }
            case TCreds::kTvmTicket: {
                return request.GetCredentials().GetTvmTicket();
            }
            case TCreds::kStaticCreds: {
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
