#include "settings.h"

#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/replication.pb.h>

#include <util/string/builder.h>

namespace NKikimr::NIamDelegation {

TIamDelegationSettings TIamDelegationSettings::FromConfig(const NKikimrConfig::TIamConfig& iamConfig) {
    TIamDelegationSettings settings;
    settings.TokenServiceEndpoint = iamConfig.GetTokenServiceEndpoint();
    settings.ServiceControlEndpoint = iamConfig.GetServiceControlEndpoint();
    settings.ResourceManagerEndpoint = iamConfig.GetResourceManagerEndpoint();
    settings.EnableSsl = iamConfig.GetEnableSsl();
    settings.ServiceId = iamConfig.GetServiceId();
    settings.MicroserviceId = iamConfig.GetMicroserviceId();
    settings.ResourceType = iamConfig.GetResourceType();
    return settings;
}

TIamDelegationSettings TIamDelegationSettings::FromConfig(const NKikimrConfig::TIamConfig& iamConfig, const NKikimrReplication::TReplicationDefaults& replicationDefaults) {
    auto settings = FromConfig(iamConfig);
    const auto& shared = replicationDefaults.GetIamServiceControl();
    if (settings.TokenServiceEndpoint.empty()) {
        settings.TokenServiceEndpoint = shared.GetEndpoint();
    }
    if (settings.ServiceId.empty()) {
        settings.ServiceId = shared.GetServiceId();
    }
    if (settings.MicroserviceId.empty()) {
        settings.MicroserviceId = shared.GetMicroserviceId();
    }
    if (settings.ResourceType.empty()) {
        settings.ResourceType = shared.GetResourceType();
    }
    if (!iamConfig.HasEnableSsl()) {
        settings.EnableSsl = shared.GetEnableSsl();
    }
    return settings;
}

namespace {

// The identity of YDB as a cloud service, needed by every IAM call this feature makes. All three identity
// fields are required by IAM: the agent service account a delegation is granted to is named
// yc.<ServiceId>.<MicroserviceId>.<cloud>.agent, and IAM checks that both exist in its service registry.
void CollectMissingIdentity(const TIamDelegationSettings& settings, TStringBuilder& missing) {
    if (settings.TokenServiceEndpoint.empty()) {
        missing << " TokenServiceEndpoint";
    }
    if (settings.ServiceId.empty()) {
        missing << " ServiceId";
    }
    if (settings.MicroserviceId.empty()) {
        missing << " MicroserviceId";
    }
    if (settings.ResourceType.empty()) {
        missing << " ResourceType";
    }
}

} // namespace

TString TIamDelegationSettings::Validate() const {
    TStringBuilder missing;
    CollectMissingIdentity(*this, missing);
    if (!missing.empty()) {
        return TStringBuilder() << "IAM delegation is not configured, missing IamConfig fields:" << missing;
    }
    return {};
}

TString TIamDelegationSettings::ValidateForDelegation() const {
    TStringBuilder missing;
    CollectMissingIdentity(*this, missing);
    if (ServiceControlEndpoint.empty()) {
        missing << " ServiceControlEndpoint";
    }
    if (!missing.empty()) {
        return TStringBuilder() << "Setting up and revoking IAM delegations is not configured, missing IamConfig fields:" << missing;
    }
    return {};
}

} // namespace NKikimr::NIamDelegation
