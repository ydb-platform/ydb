#include "kqp_federated_query_actors.h"

#include <ydb/services/metadata/secret/fetcher.h>
#include <ydb/services/metadata/secret/snapshot.h>


namespace NKikimr::NKqp {

namespace {

class TDescribeSecretsActor: public NActors::TActorBootstrapped<TDescribeSecretsActor> {
    STRICT_STFUNC(StateFunc,
        hFunc(NMetadata::NProvider::TEvRefreshSubscriberData, Handle);
    )

    void Handle(NMetadata::NProvider::TEvRefreshSubscriberData::TPtr& ev) {
        auto snapshot = ev->Get()->GetSnapshotAs<NMetadata::NSecret::TSnapshot>();

        std::vector<TString> secretValues;
        secretValues.reserve(SecretIds.size());
        for (const auto& secretId: SecretIds) {
            TString secretValue;
            const bool isFound = snapshot->GetSecretValue(NMetadata::NSecret::TSecretIdOrValue::BuildAsId(secretId), secretValue);
            if (!isFound) {
                if (!AskSent) {
                    AskSent = true;
                    Send(NMetadata::NProvider::MakeServiceId(SelfId().NodeId()), new NMetadata::NProvider::TEvAskSnapshot(GetSecretsSnapshotParser()));
                } else {
                    CompleteAndPassAway(TEvDescribeSecretsResponse::TDescription(Ydb::StatusIds::BAD_REQUEST, { NYql::TIssue("secret with name '" + secretId.GetSecretId() + "' not found") }));
                }
                return;
            }
            secretValues.push_back(secretValue);
        }

        CompleteAndPassAway(TEvDescribeSecretsResponse::TDescription(secretValues));
    }

    void CompleteAndPassAway(const TEvDescribeSecretsResponse::TDescription& response) {
        Promise.SetValue(response);

        Send(NMetadata::NProvider::MakeServiceId(SelfId().NodeId()), new NMetadata::NProvider::TEvUnsubscribeExternal(GetSecretsSnapshotParser()));
        PassAway();
    }

    NMetadata::NFetcher::ISnapshotsFetcher::TPtr GetSecretsSnapshotParser() {
        return std::make_shared<NMetadata::NSecret::TSnapshotsFetcher>();
    }

public:
    TDescribeSecretsActor(const TString& ownerUserId, const std::vector<TString>& secretIds, NThreading::TPromise<TEvDescribeSecretsResponse::TDescription> promise)
        : SecretIds(CreateSecretIds(ownerUserId, secretIds))
        , Promise(promise)
    {}

    void Bootstrap() {
        if (!NMetadata::NProvider::TServiceOperator::IsEnabled()) {
            Promise.SetValue(TEvDescribeSecretsResponse::TDescription(Ydb::StatusIds::INTERNAL_ERROR, { NYql::TIssue("metadata service is not active") }));
            PassAway();
            return;
        }

        Send(NMetadata::NProvider::MakeServiceId(SelfId().NodeId()), new NMetadata::NProvider::TEvSubscribeExternal(GetSecretsSnapshotParser()));
        Become(&TDescribeSecretsActor::StateFunc);
    }

private:
    static std::vector<NMetadata::NSecret::TSecretId> CreateSecretIds(const TString& ownerUserId, const std::vector<TString>& secretIds) {
        std::vector<NMetadata::NSecret::TSecretId> result;
        for (const TString& secretId : secretIds) {
            result.emplace_back(ownerUserId, secretId);
        }
        return result;
    }

private:
    const std::vector<NMetadata::NSecret::TSecretId> SecretIds;
    NThreading::TPromise<TEvDescribeSecretsResponse::TDescription> Promise;
    bool AskSent = false;
};

}  // anonymous namespace

IActor* CreateDescribeSecretsActor(const TString& ownerUserId, const std::vector<TString>& secretIds, NThreading::TPromise<TEvDescribeSecretsResponse::TDescription> promise) {
    return new TDescribeSecretsActor(ownerUserId, secretIds, promise);
}

void RegisterDescribeSecretsActor(const NActors::TActorId& replyActorId, const TString& ownerUserId, const std::vector<TString>& secretIds, NActors::TActorSystem* actorSystem) {
    auto promise = NThreading::NewPromise<TEvDescribeSecretsResponse::TDescription>();
    actorSystem->Register(CreateDescribeSecretsActor(ownerUserId, secretIds, promise));

    promise.GetFuture().Subscribe([actorSystem, replyActorId](const NThreading::TFuture<TEvDescribeSecretsResponse::TDescription>& result){
        actorSystem->Send(replyActorId, new TEvDescribeSecretsResponse(result.GetValue()));
    });
}

NThreading::TFuture<TEvDescribeSecretsResponse::TDescription> DescribeExternalDataSourceSecrets(const NKikimrSchemeOp::TAuth& authDescription, const TString& ownerUserId, TActorSystem* actorSystem) {
    switch (authDescription.identity_case()) {
        case NKikimrSchemeOp::TAuth::kServiceAccount: {
            const TString& saSecretId = authDescription.GetServiceAccount().GetSecretName();
            auto promise = NThreading::NewPromise<TEvDescribeSecretsResponse::TDescription>();
            actorSystem->Register(CreateDescribeSecretsActor(ownerUserId, {saSecretId}, promise));
            return promise.GetFuture();
        }

        case NKikimrSchemeOp::TAuth::kNone:
            return NThreading::MakeFuture(TEvDescribeSecretsResponse::TDescription({}));

        case NKikimrSchemeOp::TAuth::kBasic: {
            const TString& passwordSecretId = authDescription.GetBasic().GetPasswordSecretName();
            auto promise = NThreading::NewPromise<TEvDescribeSecretsResponse::TDescription>();
            actorSystem->Register(CreateDescribeSecretsActor(ownerUserId, {passwordSecretId}, promise));
            return promise.GetFuture();
        }

        case NKikimrSchemeOp::TAuth::kMdbBasic: {
            const TString& saSecretId = authDescription.GetMdbBasic().GetServiceAccountSecretName();
            const TString& passwordSecreId = authDescription.GetMdbBasic().GetPasswordSecretName();
            auto promise = NThreading::NewPromise<TEvDescribeSecretsResponse::TDescription>();
            actorSystem->Register(CreateDescribeSecretsActor(ownerUserId, {saSecretId, passwordSecreId}, promise));
            return promise.GetFuture();
        }

        case NKikimrSchemeOp::TAuth::kAws: {
            const TString& awsAccessKeyIdSecretId = authDescription.GetAws().GetAwsAccessKeyIdSecretName();
            const TString& awsAccessKeyKeySecretId = authDescription.GetAws().GetAwsSecretAccessKeySecretName();
            auto promise = NThreading::NewPromise<TEvDescribeSecretsResponse::TDescription>();
            actorSystem->Register(CreateDescribeSecretsActor(ownerUserId, {awsAccessKeyIdSecretId, awsAccessKeyKeySecretId}, promise));
            return promise.GetFuture();
        }

        case NKikimrSchemeOp::TAuth::kToken: {
            const TString& tokenSecretId = authDescription.GetToken().GetTokenSecretName();
            auto promise = NThreading::NewPromise<TEvDescribeSecretsResponse::TDescription>();
            actorSystem->Register(CreateDescribeSecretsActor(ownerUserId, {tokenSecretId}, promise));
            return promise.GetFuture();
        }

        case NKikimrSchemeOp::TAuth::IDENTITY_NOT_SET:
            return NThreading::MakeFuture(TEvDescribeSecretsResponse::TDescription(Ydb::StatusIds::BAD_REQUEST, { NYql::TIssue("identity case is not specified") }));
    }
}

}  // namespace NKikimr::NKqp
