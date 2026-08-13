#pragma once

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/public/api/client/yc_private/iam/service_control_service.pb.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

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

yandex::cloud::priv::iam::v1::EnsureServicesEnabledRequest MakeEnsureEnabledRequest(
    const TIamDelegationSettings& settings,
    const TIamDelegation& delegation,
    const TString& subjectId);

yandex::cloud::priv::iam::v1::SetupDelegationRequest MakeSetupDelegationRequest(
    const TIamDelegationSettings& settings,
    const TIamDelegation& delegation,
    const TString& subjectId);

yandex::cloud::priv::iam::v1::RevokeDelegationRequest MakeRevokeDelegationRequest(
    const TIamDelegationSettings& settings,
    const TIamDelegation& delegation);

TString NormalizeIamSubject(TString subjectId);
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
EDelegationCleanup SelectCleanupAfterIamSchemeRequest(
    bool schemeSuccess,
    bool schemeAlreadyExists,
    const TIamDelegation& previous,
    const TIamDelegation& staged);

} // namespace NKikimr::NKqp::NExternalDataSource
