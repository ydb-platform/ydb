#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/public/api/client/yc_private/servicecontrol/service_control_service.pb.h>

#include <library/cpp/threading/future/future.h>

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

yandex::cloud::priv::servicecontrol::v1::EnsureEnabledRequest MakeEnsureEnabledRequest(
    const TIamDelegationSettings& settings,
    const TIamDelegation& delegation);

yandex::cloud::priv::servicecontrol::v1::SetupDelegationRequest MakeSetupDelegationRequest(
    const TIamDelegationSettings& settings,
    const TIamDelegation& delegation,
    const TString& subjectId);

yandex::cloud::priv::servicecontrol::v1::RevokeDelegationRequest MakeRevokeDelegationRequest(
    const TIamDelegationSettings& settings,
    const TIamDelegation& delegation);

TString NormalizeIamSubject(TString subjectId);
TString MakeIamDelegationReferrerId(TStringBuf externalDataSourceName, TStringBuf uniqueId);
bool IsManagedIamDelegation(const TIamDelegation& delegation);
bool IsSameIamDelegation(const TIamDelegation& lhs, const TIamDelegation& rhs);
EDelegationCleanup SelectCleanupAfterSchemeRequest(
    bool schemeSuccess,
    const TIamDelegation& previous,
    const TIamDelegation& staged);

NThreading::TFuture<TIamDelegationResult> SetupIamDelegation(
    const TIamDelegationSettings& settings,
    const TIamDelegation& delegation,
    const TString& subjectId,
    NActors::TActorSystem* actorSystem);

NThreading::TFuture<TIamDelegationResult> RevokeIamDelegation(
    const TIamDelegationSettings& settings,
    const TIamDelegation& delegation,
    NActors::TActorSystem* actorSystem);

} // namespace NKikimr::NKqp::NExternalDataSource
