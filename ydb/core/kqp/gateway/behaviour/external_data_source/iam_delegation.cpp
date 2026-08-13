#include "iam_delegation.h"

#include <util/string/ascii.h>
#include <util/string/builder.h>

namespace NKikimr::NKqp::NExternalDataSource {

yandex::cloud::priv::servicecontrol::v1::EnsureEnabledRequest MakeEnsureEnabledRequest(
    const TIamDelegationSettings& settings,
    const TIamDelegation& delegation)
{
    yandex::cloud::priv::servicecontrol::v1::EnsureEnabledRequest request;
    request.add_service_ids(settings.ServiceId);
    request.mutable_resource()->set_id(delegation.ResourceId);
    request.mutable_resource()->set_type(settings.ResourceType);
    return request;
}

yandex::cloud::priv::servicecontrol::v1::SetupDelegationRequest MakeSetupDelegationRequest(
    const TIamDelegationSettings& settings,
    const TIamDelegation& delegation,
    const TString& subjectId)
{
    yandex::cloud::priv::servicecontrol::v1::SetupDelegationRequest request;
    request.set_service_id(settings.ServiceId);
    request.set_microservice_id(settings.MicroserviceId);
    request.mutable_resource()->set_id(delegation.ResourceId);
    request.mutable_resource()->set_type(settings.ResourceType);
    request.set_target_service_account_id(delegation.ServiceAccountId);
    request.mutable_referrer()->set_id(delegation.ReferrerId);
    request.mutable_referrer()->set_type(delegation.ReferrerType);
    request.set_on_behalf_of_subject_id(subjectId);
    request.set_with_references(true);
    return request;
}

yandex::cloud::priv::servicecontrol::v1::RevokeDelegationRequest MakeRevokeDelegationRequest(
    const TIamDelegationSettings& settings,
    const TIamDelegation& delegation)
{
    yandex::cloud::priv::servicecontrol::v1::RevokeDelegationRequest request;
    request.set_service_id(settings.ServiceId);
    request.set_microservice_id(settings.MicroserviceId);
    request.mutable_resource()->set_id(delegation.ResourceId);
    request.mutable_resource()->set_type(settings.ResourceType);
    request.set_target_service_account_id(delegation.ServiceAccountId);
    request.mutable_referrer()->set_id(delegation.ReferrerId);
    request.mutable_referrer()->set_type(delegation.ReferrerType);
    request.set_with_references(true);
    return request;
}

TString NormalizeIamSubject(TString subjectId) {
    constexpr TStringBuf suffix = "@as";
    if (subjectId.EndsWith(suffix)) {
        subjectId.resize(subjectId.size() - suffix.size());
    }
    return subjectId;
}

TString MakeIamDelegationReferrerId(TStringBuf externalDataSourceName, TStringBuf uniqueId) {
    TString readable;
    readable.reserve(8);
    for (const char ch : externalDataSourceName) {
        if (readable.size() == 8) {
            break;
        }
        if (IsAsciiAlnum(ch) || ch == '-' || ch == '_') {
            readable += AsciiToLower(ch);
        } else if (readable.empty() || readable.back() != '-') {
            readable += '-';
        }
    }
    if (readable.empty()) {
        readable = "source";
    }
    TString result = TStringBuilder() << "eds:" << readable << ':';
    result.append(uniqueId.data(), Min(uniqueId.size(), size_t(50 - result.size())));
    return result;
}

bool IsManagedIamDelegation(const TIamDelegation& delegation) {
    return !delegation.ServiceAccountId.empty() && !delegation.ResourceId.empty() &&
        !delegation.ReferrerId.empty();
}

bool IsSameIamDelegation(const TIamDelegation& lhs, const TIamDelegation& rhs) {
    return lhs.ServiceAccountId == rhs.ServiceAccountId &&
        lhs.ResourceId == rhs.ResourceId && lhs.ReferrerId == rhs.ReferrerId;
}

EDelegationCleanup SelectCleanupAfterSchemeRequest(
    bool schemeSuccess,
    const TIamDelegation& previous,
    const TIamDelegation& staged)
{
    if (!schemeSuccess) {
        return IsManagedIamDelegation(staged)
            ? EDelegationCleanup::Staged
            : EDelegationCleanup::None;
    }
    if (IsManagedIamDelegation(previous) &&
        (!IsManagedIamDelegation(staged) || !IsSameIamDelegation(previous, staged)))
    {
        return EDelegationCleanup::Previous;
    }
    return EDelegationCleanup::None;
}

} // namespace NKikimr::NKqp::NExternalDataSource
