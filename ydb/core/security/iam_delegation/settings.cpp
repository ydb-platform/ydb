#include "settings.h"

#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/replication.pb.h>

#include <util/string/builder.h>

namespace NKikimr::NIamDelegation {

TIamDelegationSettings TIamDelegationSettings::FromConfig(const NKikimrConfig::TIamConfig& iamConfig) {
    TIamDelegationSettings settings;
    settings.Config = iamConfig;
    return settings;
}

TIamDelegationSettings TIamDelegationSettings::FromConfig(const NKikimrConfig::TIamConfig& iamConfig, const NKikimrReplication::TReplicationDefaults& replicationDefaults) {
    auto settings = FromConfig(iamConfig);
    const auto& shared = replicationDefaults.GetIamServiceControl();
    if (settings.Config.GetTokenServiceEndpoint().empty()) {
        settings.Config.SetTokenServiceEndpoint(shared.GetEndpoint());
    }
    if (settings.Config.GetServiceId().empty()) {
        settings.Config.SetServiceId(shared.GetServiceId());
    }
    if (settings.Config.GetMicroserviceId().empty()) {
        settings.Config.SetMicroserviceId(shared.GetMicroserviceId());
    }
    if (settings.Config.GetResourceType().empty()) {
        settings.Config.SetResourceType(shared.GetResourceType());
    }
    if (!iamConfig.HasEnableSsl()) {
        settings.Config.SetEnableSsl(shared.GetEnableSsl());
    }
    return settings;
}

namespace {

// The identity of YDB as a cloud service, needed by every IAM call this feature makes. All three identity
// fields are required by IAM: the agent service account a delegation is granted to is named
// yc.<ServiceId>.<MicroserviceId>.<cloud>.agent, and IAM checks that both exist in its service registry.
void CollectMissingIdentity(const TIamDelegationSettings& settings, TStringBuilder& missing) {
    if (settings.Config.GetTokenServiceEndpoint().empty()) {
        missing << " TokenServiceEndpoint";
    }
    if (settings.Config.GetServiceId().empty()) {
        missing << " ServiceId";
    }
    if (settings.Config.GetMicroserviceId().empty()) {
        missing << " MicroserviceId";
    }
    if (settings.Config.GetResourceType().empty()) {
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
    if (Config.GetSystemTokenName().empty()) {
        missing << " SystemTokenName";
    }
    if (Config.GetServiceControlEndpoint().empty()) {
        missing << " ServiceControlEndpoint";
    }
    if (!missing.empty()) {
        return TStringBuilder() << "Setting up and revoking IAM delegations is not configured, missing IamConfig fields:" << missing;
    }
    return {};
}

} // namespace NKikimr::NIamDelegation
