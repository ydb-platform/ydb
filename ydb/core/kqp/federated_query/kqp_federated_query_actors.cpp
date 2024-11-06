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
        secretValues.reserve(SecretIdsOrNames.size());
        for (const auto& secretIdOrName: SecretIdsOrNames) {
            if (const auto& secretId = secretIdOrName.GetSecretId()) {
                TString secretValue;
                bool isFound = snapshot->GetSecretValue(NMetadata::NSecret::TSecretIdOrValue::BuildAsId(*secretId), secretValue);
                if (isFound) {
                    secretValues.push_back(secretValue);
                    continue;
                }
            } else {
                const auto& secretName = secretIdOrName.GetValue();
                AFL_VERIFY(secretName);

                auto secretIds = snapshot->GetSecretIds(UserToken, *secretName);
                if (secretIds.size() > 1) {
                    CompleteAndPassAway(TEvDescribeSecretsResponse::TDescription(Ydb::StatusIds::BAD_REQUEST, { NYql::TIssue("several secrets with name '" + *secretName + "' were found") }));
                    return;
                }

                TString secretValue;
                bool isFound = !secretIds.empty() && snapshot->GetSecretValue(NMetadata::NSecret::TSecretIdOrValue::BuildAsId(secretIds[0]), secretValue);
                if (isFound) {
                    secretValues.push_back(secretValue);
                    continue;
                }
            }

            if (!AskSent) {
                AskSent = true;
                Send(NMetadata::NProvider::MakeServiceId(SelfId().NodeId()), new NMetadata::NProvider::TEvAskSnapshot(GetSecretsSnapshotParser()));
            } else {
                CompleteAndPassAway(TEvDescribeSecretsResponse::TDescription(Ydb::StatusIds::BAD_REQUEST, { NYql::TIssue("secret with name '" + secretIdOrName.SerializeToString() + "' not found") }));
            }
            return;
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
    TDescribeSecretsActor(const std::optional<NACLib::TUserToken>& userToken, const std::vector<NMetadata::NSecret::TSecretIdOrValue>& secretIds,
        NThreading::TPromise<TEvDescribeSecretsResponse::TDescription> promise)
        : UserToken(userToken)
        , SecretIdsOrNames(secretIds)
        , Promise(promise) {
    }

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
    std::optional<NACLib::TUserToken> UserToken;
    const std::vector<NMetadata::NSecret::TSecretIdOrValue> SecretIdsOrNames;
    NThreading::TPromise<TEvDescribeSecretsResponse::TDescription> Promise;
    bool AskSent = false;
};

}  // anonymous namespace

IActor* CreateDescribeSecretsActor(const std::optional<NACLib::TUserToken>& userToken,
    const std::vector<NMetadata::NSecret::TSecretIdOrValue>& secretIdsOrNames, NThreading::TPromise<TEvDescribeSecretsResponse::TDescription> promise) {
    return new TDescribeSecretsActor(userToken, secretIdsOrNames, promise);
}

void RegisterDescribeSecretsActor(const NActors::TActorId& replyActorId, const std::optional<NACLib::TUserToken>& userToken,
    const std::vector<NMetadata::NSecret::TSecretIdOrValue>& secretIdsOrNames, NActors::TActorSystem* actorSystem) {
    auto promise = NThreading::NewPromise<TEvDescribeSecretsResponse::TDescription>();
    actorSystem->Register(CreateDescribeSecretsActor(userToken, secretIdsOrNames, promise));

    promise.GetFuture().Subscribe([actorSystem, replyActorId](const NThreading::TFuture<TEvDescribeSecretsResponse::TDescription>& result){
        actorSystem->Send(replyActorId, new TEvDescribeSecretsResponse(result.GetValue()));
    });
}

NThreading::TFuture<TEvDescribeSecretsResponse::TDescription> DescribeExternalDataSourceSecrets(
    const NKikimrSchemeOp::TAuth& authDescription, const std::optional<NACLib::TUserToken>& userToken, TActorSystem* actorSystem) {
    switch (authDescription.identity_case()) {
        case NKikimrSchemeOp::TAuth::kServiceAccount: {
            const TString& saSecretId = authDescription.GetServiceAccount().GetSecretName();
            auto promise = NThreading::NewPromise<TEvDescribeSecretsResponse::TDescription>();
            actorSystem->Register(CreateDescribeSecretsActor(userToken, { NMetadata::NSecret::TSecretIdOrValue::BuildAsValue(saSecretId) }, promise));
            return promise.GetFuture();
        }

        case NKikimrSchemeOp::TAuth::kNone:
            return NThreading::MakeFuture(TEvDescribeSecretsResponse::TDescription({}));

        case NKikimrSchemeOp::TAuth::kBasic: {
            const TString& passwordSecretId = authDescription.GetBasic().GetPasswordSecretName();
            auto promise = NThreading::NewPromise<TEvDescribeSecretsResponse::TDescription>();
            actorSystem->Register(CreateDescribeSecretsActor(userToken, {  NMetadata::NSecret::TSecretIdOrValue::BuildAsValue(passwordSecretId) }, promise));
            return promise.GetFuture();
        }

        case NKikimrSchemeOp::TAuth::kMdbBasic: {
            const TString& saSecretId = authDescription.GetMdbBasic().GetServiceAccountSecretName();
            const TString& passwordSecreId = authDescription.GetMdbBasic().GetPasswordSecretName();
            auto promise = NThreading::NewPromise<TEvDescribeSecretsResponse::TDescription>();
            actorSystem->Register(CreateDescribeSecretsActor(userToken,
                { NMetadata::NSecret::TSecretIdOrValue::BuildAsValue(saSecretId),
                    NMetadata::NSecret::TSecretIdOrValue::BuildAsValue(passwordSecreId) },
                promise));
            return promise.GetFuture();
        }

        case NKikimrSchemeOp::TAuth::kAws: {
            const NMetadata::NSecret::TSecretIdOrValue awsAccessKeyIdSecretId =
                authDescription.GetAws().HasAwsAccessKeyIdSecretOwner()
                    ? NMetadata::NSecret::TSecretIdOrValue::BuildAsId(NMetadata::NSecret::TSecretId(
                          authDescription.GetAws().GetAwsAccessKeyIdSecretOwner(), authDescription.GetAws().GetAwsAccessKeyIdSecretName()))
                    : NMetadata::NSecret::TSecretIdOrValue::BuildAsValue(authDescription.GetAws().GetAwsAccessKeyIdSecretOwner());
            const NMetadata::NSecret::TSecretIdOrValue awsAccessKeyKeySecretId =
                authDescription.GetAws().HasAwsSecretAccessKeySecretOwner()
                    ? NMetadata::NSecret::TSecretIdOrValue::BuildAsId(NMetadata::NSecret::TSecretId(
                          authDescription.GetAws().GetAwsSecretAccessKeySecretOwner(), authDescription.GetAws().GetAwsSecretAccessKeySecretName()))
                    : NMetadata::NSecret::TSecretIdOrValue::BuildAsValue(authDescription.GetAws().GetAwsSecretAccessKeySecretName());
            auto promise = NThreading::NewPromise<TEvDescribeSecretsResponse::TDescription>();
            actorSystem->Register(CreateDescribeSecretsActor(userToken, { awsAccessKeyIdSecretId, awsAccessKeyKeySecretId }, promise));
            return promise.GetFuture();
        }

        case NKikimrSchemeOp::TAuth::kToken: {
            const TString& tokenSecretId = authDescription.GetToken().GetTokenSecretName();
            auto promise = NThreading::NewPromise<TEvDescribeSecretsResponse::TDescription>();
            actorSystem->Register(CreateDescribeSecretsActor(userToken, { NMetadata::NSecret::TSecretIdOrValue::BuildAsValue(tokenSecretId) }, promise));
            return promise.GetFuture();
        }

        case NKikimrSchemeOp::TAuth::IDENTITY_NOT_SET:
            return NThreading::MakeFuture(TEvDescribeSecretsResponse::TDescription(Ydb::StatusIds::BAD_REQUEST, { NYql::TIssue("identity case is not specified") }));
    }
}

}  // namespace NKikimr::NKqp
