#include "iam_actor_base.h"

#include <ydb/library/actors/async/wait_for_event.h>

namespace NKikimr::NIamDelegation {

namespace {

NActors::async<void> WaitSystemToken(TIamCallCredentials& credentials) {
    while (credentials.Status.Code == TEvTokenManager::TStatus::ECode::NOT_READY) {
        co_await credentials.Updated.Wait();
    }
}

} // namespace

void CheckIamCallDeadline(TMonotonic deadline) {
    if (NActors::TActivationContext::Monotonic() >= deadline) {
        throw TIamCallError(Ydb::StatusIds::TIMEOUT) << "IAM operation polling deadline exceeded";
    }
}

NActors::async<TString> GetIamCallToken(TIamCallCredentials& credentials, TMonotonic deadline) {
    CheckIamCallDeadline(deadline);
    const auto tokenDeadline = Min(deadline, NActors::TActivationContext::Monotonic() + TIamDelegationSettings::RequestTimeout);
    const bool ready = co_await NActors::WithDeadline(tokenDeadline, &WaitSystemToken, std::ref(credentials));
    CheckIamCallDeadline(deadline);
    if (!ready) {
        throw TIamCallError(Ydb::StatusIds::UNAVAILABLE) << "timeout while obtaining the system service account token";
    }
    if (credentials.Status.Code != TEvTokenManager::TStatus::ECode::SUCCESS) {
        throw TIamCallError(Ydb::StatusIds::UNAVAILABLE) << "system service account token is unavailable";
    }
    if (credentials.Token.empty()) {
        throw TIamCallError(Ydb::StatusIds::UNAVAILABLE) << "system service account token is empty";
    }
    co_return credentials.Token;
}

NActors::async<NActors::IEventHandle::TPtr> IamClientRequest(NActors::TActorId client, NActors::IEventBase* request) {
    co_return co_await NActors::ActorRequest<NActors::IEventHandle>(client, request, NActors::IEventHandle::FlagTrackDelivery);
}


} // namespace NKikimr::NIamDelegation
