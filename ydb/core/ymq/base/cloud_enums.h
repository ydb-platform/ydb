#pragma once

namespace NKikimr::NSQS::NCloudAuth {
    enum EActionType {
        Authenticate = 0,
        Authorize,
        GetCloudId,
        ActionTypesCount
    };

    enum ECredentialType {
        IamToken = 0,
        Signature,
        CredentialTypesCount
    };
} // namespace NKikimr::NSQS::NCloudAuth
