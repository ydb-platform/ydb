#include "actor_persqueue_client_iface.h"

namespace NKikimr::NPQ {

bool MirrorCredentialsNeedResolution(const NKikimrPQ::TMirrorPartitionConfig::TCredentials& credentials) {
    switch (credentials.GetCredentialsCase()) {
        case NKikimrPQ::TMirrorPartitionConfig::TCredentials::kOauthTokenSecretName:
        case NKikimrPQ::TMirrorPartitionConfig::TCredentials::kJwtParamsSecretName:
            return true;
        case NKikimrPQ::TMirrorPartitionConfig::TCredentials::kIam:
            return credentials.GetIam().HasServiceAccountKeySecretName();
        default:
            return false;
    }
}

TString GetMirrorCredentialsSecretName(const NKikimrPQ::TMirrorPartitionConfig::TCredentials& credentials) {
    switch (credentials.GetCredentialsCase()) {
        case NKikimrPQ::TMirrorPartitionConfig::TCredentials::kOauthTokenSecretName:
            return credentials.GetOauthTokenSecretName();
        case NKikimrPQ::TMirrorPartitionConfig::TCredentials::kJwtParamsSecretName:
            return credentials.GetJwtParamsSecretName();
        case NKikimrPQ::TMirrorPartitionConfig::TCredentials::kIam:
            return credentials.GetIam().GetServiceAccountKeySecretName();
        default:
            return {};
    }
}

bool SetMirrorCredentialsSecretValue(
    NKikimrPQ::TMirrorPartitionConfig::TCredentials& credentials,
    const TString& value
) {
    switch (credentials.GetCredentialsCase()) {
        case NKikimrPQ::TMirrorPartitionConfig::TCredentials::kOauthTokenSecretName:
            credentials.SetOauthToken(value);
            return true;
        case NKikimrPQ::TMirrorPartitionConfig::TCredentials::kJwtParamsSecretName:
            credentials.SetJwtParams(value);
            return true;
        case NKikimrPQ::TMirrorPartitionConfig::TCredentials::kIam:
            if (!credentials.GetIam().HasServiceAccountKeySecretName()) {
                return false;
            }
            credentials.MutableIam()->ClearServiceAccountKeySecretName();
            credentials.MutableIam()->SetServiceAccountKey(value);
            return true;
        default:
            return false;
    }
}

} // namespace NKikimr::NPQ
