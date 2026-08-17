#pragma once

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/util/backoff.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/public/api/client/yc_private/iam/service_control_service.pb.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

#include <optional>

namespace NKikimr::NKqp::NExternalDataSource {

struct TIamDelegationSettings {
    TString Endpoint;
    TString ServiceId;
    TString MicroserviceId;
    TString ResourceType;
    bool EnableSsl = true;
    TDuration Timeout = TDuration::Seconds(10);
};

struct TIamDelegation {
    TString ResourceId;
    TString ServiceAccountId;
    TString ReferrerId;
    TString ReferrerType = "ydb.externalDataSource";
};

struct TIamDelegationResult {
    bool Success = false;
    TString Error;
};

struct TIamCallerIdentity {
    TString BearerToken;
    TString SubjectId;
};

enum class EDelegationCleanup {
    None,
    Previous,
    Staged,
};

enum class EIamOperationState {
    InProgress,
    Succeeded,
    Failed,
};

// Bounds the polling of an accepted-but-unfinished IAM operation: how long a DDL
// is willing to wait for a terminal state, and how fast it may ask. Without the
// budget, a lifecycle actor whose operation IAM never resolves waits forever and
// the DDL never completes.
//
// A DDL statement waits on the budget synchronously, so CREATE can spend at most
// two of them (EnsureEnabled, SetupDelegation) plus up to one gRPC request
// timeout of overshoot on the final poll.
constexpr TDuration IamOperationPollBudget = TDuration::Seconds(30);
constexpr TDuration IamOperationMinPollDelay = TDuration::MilliSeconds(100);
constexpr TDuration IamOperationMaxPollDelay = TDuration::Seconds(2);

yandex::cloud::priv::iam::v1::EnsureServicesEnabledRequest MakeEnsureEnabledRequest(
    const TIamDelegationSettings& settings,
    const TIamDelegation& delegation);

yandex::cloud::priv::iam::v1::SetupDelegationRequest MakeSetupDelegationRequest(
    const TIamDelegationSettings& settings,
    const TIamDelegation& delegation,
    const TString& subjectId);

yandex::cloud::priv::iam::v1::RevokeDelegationRequest MakeRevokeDelegationRequest(
    const TIamDelegationSettings& settings,
    const TIamDelegation& delegation);

TString NormalizeIamSubject(TString subjectId);
std::optional<TIamCallerIdentity> ParseIamCallerIdentity(
    const NACLib::TUserToken& token);
TString MakeIamDelegationReferrerId(TStringBuf externalDataSourceName, TStringBuf uniqueId);
EIamOperationState ClassifyIamOperation(
    const ydb::yc::priv::operation::Operation& operation);
void AddIamPathVersionPrecondition(
    NKikimrSchemeOp::TModifyScheme& schemeTx,
    ui64 pathId,
    ui64 pathVersion);
bool ShouldSkipIamDelegationSetup(
    const NKikimrSchemeOp::TModifyScheme& schemeTx,
    bool objectNotFound);
bool IsManagedIamDelegation(const TIamDelegation& delegation);
bool IsSameIamDelegation(const TIamDelegation& lhs, const TIamDelegation& rhs);
EDelegationCleanup SelectCleanupAfterSchemeRequest(
    bool schemeSuccess,
    const TIamDelegation& previous,
    const TIamDelegation& staged);
} // namespace NKikimr::NKqp::NExternalDataSource
