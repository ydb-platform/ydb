#include "kqp_federated_query_actors.h"

#include <ydb/services/metadata/secret/fetcher.h>
#include <ydb/services/metadata/secret/snapshot.h>


namespace NKikimr::NKqp {

namespace {

class TDescribeSecretsActor: public NActors::TActorBootstrapped<TDescribeSecretsActor> {
    STRICT_STFUNC(StateFunc,
        hFunc(NMetadata::NProvider::TEvRefreshSubscriberData, Handle);
        hFunc(NActors::TEvents::TEvWakeup, Handle);
    )

    void Handle(NMetadata::NProvider::TEvRefreshSubscriberData::TPtr& ev) {
        auto snapshot = ev->Get()->GetSnapshotAs<NMetadata::NSecret::TSnapshot>();

        std::vector<TString> secretValues;
        secretValues.reserve(SecretIds.size());
        for (const auto& secretId: SecretIds) {
            TString secretValue;
            const bool isFound = snapshot->GetSecretValue(NMetadata::NSecret::TSecretIdOrValue::BuildAsId(secretId), secretValue);
            if (!isFound) {
                LastResponse = TEvDescribeSecretsResponse::TDescription(Ydb::StatusIds::BAD_REQUEST, { NYql::TIssue("secret with name '" + secretId.GetSecretId() + "' not found") });
                return;
            }
            secretValues.push_back(secretValue);
        }
        Promise.SetValue(TEvDescribeSecretsResponse::TDescription(secretValues));

        UnsubscribeFromSecrets();
        PassAway();
    }

    void Handle(NActors::TEvents::TEvWakeup::TPtr&) {
        Promise.SetValue(LastResponse);

        UnsubscribeFromSecrets();
        PassAway();
    }

    NMetadata::NFetcher::ISnapshotsFetcher::TPtr GetSecretsSnapshotParser() {
        return std::make_shared<NMetadata::NSecret::TSnapshotsFetcher>();
    }

    void UnsubscribeFromSecrets() {
        this->Send(NMetadata::NProvider::MakeServiceId(SelfId().NodeId()), new NMetadata::NProvider::TEvUnsubscribeExternal(GetSecretsSnapshotParser()));
    }

public:
    TDescribeSecretsActor(const TString& ownerUserId, const std::vector<TString>& secretIds, NThreading::TPromise<TEvDescribeSecretsResponse::TDescription> promise, TDuration maximalSecretsSnapshotWaitTime)
        : SecretIds(CreateSecretIds(ownerUserId, secretIds))
        , Promise(promise)
        , LastResponse(Ydb::StatusIds::TIMEOUT, { NYql::TIssue("secrets snapshot fetching timeout") })
        , MaximalSecretsSnapshotWaitTime(maximalSecretsSnapshotWaitTime)
    {}

    void Bootstrap() {
        if (!NMetadata::NProvider::TServiceOperator::IsEnabled()) {
            Promise.SetValue(TEvDescribeSecretsResponse::TDescription(Ydb::StatusIds::INTERNAL_ERROR, { NYql::TIssue("metadata service is not active") }));
            PassAway();
            return;
        }

        this->Send(NMetadata::NProvider::MakeServiceId(SelfId().NodeId()), new NMetadata::NProvider::TEvSubscribeExternal(GetSecretsSnapshotParser()));
        this->Schedule(MaximalSecretsSnapshotWaitTime, new NActors::TEvents::TEvWakeup());
        Become(&TDescribeSecretsActor::StateFunc);
    }

private:
    static std::vector<NMetadata::NSecret::TSecretId> CreateSecretIds(const TString& ownerUserId, const std::vector<TString>& secretIds) {
        std::vector<NMetadata::NSecret::TSecretId> result;
        for (const auto& secretId: secretIds) {
            result.emplace_back(ownerUserId, secretId);
        }
        return result;
    }

private:
    const std::vector<NMetadata::NSecret::TSecretId> SecretIds;
    NThreading::TPromise<TEvDescribeSecretsResponse::TDescription> Promise;
    TEvDescribeSecretsResponse::TDescription LastResponse;
    TDuration MaximalSecretsSnapshotWaitTime;
};

}  // anonymous namespace

IActor* CreateDescribeSecretsActor(const TString& ownerUserId, const std::vector<TString>& secretIds, NThreading::TPromise<TEvDescribeSecretsResponse::TDescription> promise, TDuration maximalSecretsSnapshotWaitTime) {
    return new TDescribeSecretsActor(ownerUserId, secretIds, promise, maximalSecretsSnapshotWaitTime);
}

void RegisterDescribeSecretsActor(const NActors::TActorId& replyActorId, const TString& ownerUserId, const std::vector<TString>& secretIds, const TActorContext& actorContext, TDuration maximalSecretsSnapshotWaitTime) {
    auto promise = NThreading::NewPromise<TEvDescribeSecretsResponse::TDescription>();
    actorContext.Register(CreateDescribeSecretsActor(ownerUserId, secretIds, promise, maximalSecretsSnapshotWaitTime));

    promise.GetFuture().Subscribe([actorContext, replyActorId](const NThreading::TFuture<TEvDescribeSecretsResponse::TDescription>& result){
        actorContext.Send(replyActorId, new TEvDescribeSecretsResponse(result.GetValue()));
    });
}

}  // namespace NKikimr::NKqp
