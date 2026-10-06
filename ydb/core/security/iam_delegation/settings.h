#pragma once

#include <ydb/core/protos/config.pb.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NKikimrReplication {
    class TReplicationDefaults;
}

namespace NKikimr::NIamDelegation {

// Settings of the IAM delegation services. The identity of YDB as a Yandex Cloud service and the
// IAM endpoints come from IamConfig; the rest are constants for now.
//
// The endpoints are different services and must be configured separately: minting a token of a
// delegated service account goes to the token service, setting the delegation up goes to the IAM
// control plane, and finding the cloud of a service account additionally goes to Resource Manager.
struct TIamDelegationSettings {
    // Normalized once by FromConfig; keeps protobuf presence and any future fields intact.
    NKikimrConfig::TIamConfig Config;

    static constexpr char ReferrerType[] = "ydb.secret";
    static constexpr TDuration RequestTimeout = TDuration::Seconds(10);
    static constexpr TDuration OperationPollInterval = TDuration::Seconds(1);
    static constexpr TDuration OperationPollTimeout = TDuration::Seconds(60);
    static constexpr ui32 MaxRetries = 5; // total attempts of one IAM call

    static TIamDelegationSettings FromConfig(const NKikimrConfig::TIamConfig& iamConfig);

    // The same, with the identity of YDB and the token service taken from replication_config.iam_service_control
    // (the section async replication and the IAM auth of external data sources already use) for every field
    // IamConfig leaves empty. Only the endpoints IamConfig alone has (the control plane, Resource Manager)
    // then need to be configured for this feature, together with SystemTokenName.
    static TIamDelegationSettings FromConfig(const NKikimrConfig::TIamConfig& iamConfig, const NKikimrReplication::TReplicationDefaults& replicationDefaults);

    // Returns an error message when the settings are not sufficient to mint tokens of delegated
    // service accounts (IamTokenService.CreateForService on the token service). This is all a node
    // needs to read the delegation secrets it already has.
    TString Validate() const;

    // Returns an error message when the settings are not sufficient to set up and revoke
    // delegations (ServiceControlService and OperationService on the IAM control plane). Everything
    // Validate() requires plus the control plane endpoint and system token provider ID, so a cluster configured only for the IAM
    // auth of external data sources passes Validate() and fails this one.
    TString ValidateForDelegation() const;

    // Whether the cloud of a service account can be looked up (ServiceAccountService.Get on the
    // control plane, then FolderService.Resolve on Resource Manager). Optional: without it the
    // cloud of the database is used when RESOURCE is omitted.
    bool CanResolveCloud() const {
        return !Config.GetServiceControlEndpoint().empty() && !Config.GetResourceManagerEndpoint().empty();
    }
};

} // namespace NKikimr::NIamDelegation
