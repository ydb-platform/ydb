#pragma once

#include <ydb/core/protos/config.pb.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NKikimrReplication {
    class TReplicationDefaults;
}

namespace NKikimr::NIamDelegation {

// Startup settings of the node-local IAM delegation control service. IamConfig supplies the
// Yandex Cloud service identity, control-plane endpoint and token-manager provider name;
// request and polling budgets are constants for now.
struct TIamDelegationSettings {
    // Normalized once by FromConfig; keeps protobuf presence and any future fields intact.
    NKikimrConfig::TIamConfig Config;

    static constexpr char ReferrerType[] = "ydb.secret";
    static constexpr TDuration RequestTimeout = TDuration::Seconds(10);
    static constexpr TDuration OperationPollInterval = TDuration::Seconds(1);
    static constexpr TDuration OperationPollTimeout = TDuration::Seconds(60);
    static constexpr TDuration RequestRetryTimeout = TDuration::Minutes(1);

    static TIamDelegationSettings FromConfig(const NKikimrConfig::TIamConfig& iamConfig);

    // Fill empty identity/token-endpoint fields and an absent SSL setting from
    // replication_config.iam_service_control for compatibility with existing configuration.
    // Other protobuf fields remain unchanged.
    static TIamDelegationSettings FromConfig(const NKikimrConfig::TIamConfig& iamConfig, const NKikimrReplication::TReplicationDefaults& replicationDefaults);

    // Requires the service identity, control-plane endpoint and system token provider ID
    // needed by ServiceControlService and OperationService. Token minting is owned by
    // the existing token manager and requires no token-service endpoint here.
    TString ValidateForDelegation() const;
};

} // namespace NKikimr::NIamDelegation
