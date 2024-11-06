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
            auto secretId = GetSecretId(secretIdOrName, *snapshot);

            if (!secretId.IsSuccess()) {
                CompleteAndPassAway(TEvDescribeSecretsResponse::TDescription(Ydb::StatusIds::BAD_REQUEST, { NYql::TIssue(secretId.GetErrorMessage()) }));
                return;
            }

            auto secretRef = NMetadata::NSecret::TSecretIdOrValue::BuildAsId(secretId.GetResult());
            TString secretValue;
            if (!snapshot->GetSecretValue(secretRef, secretValue)) {
                CompleteAndPassAway(TEvDescribeSecretsResponse::TDescription(Ydb::StatusIds::BAD_REQUEST, { NYql::TIssue("No such secret: " + secretId->SerializeToString()) }));
                return;
            }

            if (!snapshot->CheckSecretAccess(secretRef, UserToken)) {
                CompleteAndPassAway(TEvDescribeSecretsResponse::TDescription(Ydb::StatusIds::UNAUTHORIZED, { NYql::TIssue("No access to secret: " + secretId->SerializeToString()) }));
                return;
            }

            secretValues.emplace_back(secretValue);
        }

        CompleteAndPassAway(TEvDescribeSecretsResponse::TDescription(secretValues));
    }

    TConclusion<NMetadata::NSecret::TSecretId> GetSecretId(const NMetadata::NSecret::TSecretIdOrValue& secretIdOrName, const NMetadata::NSecret::TSnapshot& secrets) {
        if (const auto& secretId = secretIdOrName.GetSecretId()) {
            return *secretId;
        } else {
            const auto& secretName = secretIdOrName.GetValue();
            AFL_VERIFY(secretName);

            auto secretIds = secrets.GetSecretIds(UserToken, *secretName);
            if (secretIds.size() > 1) {
                return TConclusionStatus::Fail("several secrets with name '" + *secretName + "' were found");
            }

            if (secretIds.empty()) {
                return TConclusionStatus::Fail("secret with name '" + *secretName + "' not found");
            }

            return secretIds[0];
        }
    }

    void CompleteAndPassAway(const TEvDescribeSecretsResponse::TDescription& response) {
        Promise.SetValue(response);

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

        Send(NMetadata::NProvider::MakeServiceId(SelfId().NodeId()), new NMetadata::NProvider::TEvAskSnapshot(GetSecretsSnapshotParser()));
        Become(&TDescribeSecretsActor::StateFunc);
    }

private:
    std::optional<NACLib::TUserToken> UserToken;
    const std::vector<NMetadata::NSecret::TSecretIdOrValue> SecretIdsOrNames;
    NThreading::TPromise<TEvDescribeSecretsResponse::TDescription> Promise;
};

std::vector<NMetadata::NSecret::TSecretIdOrValue> MakeSecretIdsOrNames(const std::vector<TString> secretNames) {
    std::vector<NMetadata::NSecret::TSecretIdOrValue> secretIdsOrNames;
    for (const TString& name : secretNames) {
        secretIdsOrNames.emplace_back(NMetadata::NSecret::TSecretIdOrValue::BuildAsValue(name));
    }
    return secretIdsOrNames;
}

}  // anonymous namespace

IActor* CreateDescribeSecretsActor(const std::optional<NACLib::TUserToken>& userToken,
    const std::vector<NMetadata::NSecret::TSecretIdOrValue>& secretIdsOrNames, NThreading::TPromise<TEvDescribeSecretsResponse::TDescription> promise) {
    return new TDescribeSecretsActor(userToken, secretIdsOrNames, promise);
}

IActor* CreateDescribeSecretsActor(const std::optional<NACLib::TUserToken>& userToken, const std::vector<TString>& secretNames, NThreading::TPromise<TEvDescribeSecretsResponse::TDescription> promise) {
    return CreateDescribeSecretsActor(userToken, MakeSecretIdsOrNames(secretNames), promise);
}

void RegisterDescribeSecretsActor(const NActors::TActorId& replyActorId, const std::optional<NACLib::TUserToken>& userToken,
    const std::vector<NMetadata::NSecret::TSecretIdOrValue>& secretIdsOrNames, NActors::TActorSystem* actorSystem) {
    auto promise = NThreading::NewPromise<TEvDescribeSecretsResponse::TDescription>();
    actorSystem->Register(CreateDescribeSecretsActor(userToken, secretIdsOrNames, promise));

    promise.GetFuture().Subscribe([actorSystem, replyActorId](const NThreading::TFuture<TEvDescribeSecretsResponse::TDescription>& result){
        actorSystem->Send(replyActorId, new TEvDescribeSecretsResponse(result.GetValue()));
    });
}

void RegisterDescribeSecretsActor(const TActorId& replyActorId, const std::optional<NACLib::TUserToken>& userToken, const std::vector<TString>& secretNames, NActors::TActorSystem* actorSystem) {
    RegisterDescribeSecretsActor(replyActorId, userToken, MakeSecretIdsOrNames(secretNames), actorSystem);
}

NThreading::TFuture<TEvDescribeSecretsResponse::TDescription> DescribeExternalDataSourceSecrets(
    const NKikimrSchemeOp::TAuth& authDescription, const std::optional<NACLib::TUserToken>& userToken, TActorSystem* actorSystem) {
    switch (authDescription.identity_case()) {
        case NKikimrSchemeOp::TAuth::kServiceAccount: {
            const TString& saSecretId = authDescription.GetServiceAccount().GetSecretName();
            auto promise = NThreading::NewPromise<TEvDescribeSecretsResponse::TDescription>();
            actorSystem->Register(CreateDescribeSecretsActor(userToken, { saSecretId }, promise));
            return promise.GetFuture();
        }

        case NKikimrSchemeOp::TAuth::kNone:
            return NThreading::MakeFuture(TEvDescribeSecretsResponse::TDescription({}));

        case NKikimrSchemeOp::TAuth::kBasic: {
            const TString& passwordSecretId = authDescription.GetBasic().GetPasswordSecretName();
            auto promise = NThreading::NewPromise<TEvDescribeSecretsResponse::TDescription>();
            actorSystem->Register(CreateDescribeSecretsActor(userToken, { passwordSecretId }, promise));
            return promise.GetFuture();
        }

        case NKikimrSchemeOp::TAuth::kMdbBasic: {
            const TString& saSecretId = authDescription.GetMdbBasic().GetServiceAccountSecretName();
            const TString& passwordSecreId = authDescription.GetMdbBasic().GetPasswordSecretName();
            auto promise = NThreading::NewPromise<TEvDescribeSecretsResponse::TDescription>();
            actorSystem->Register(CreateDescribeSecretsActor(userToken, { saSecretId, passwordSecreId }, promise));
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
            actorSystem->Register(CreateDescribeSecretsActor(userToken, { tokenSecretId }, promise));
            return promise.GetFuture();
        }

        case NKikimrSchemeOp::TAuth::IDENTITY_NOT_SET:
            return NThreading::MakeFuture(TEvDescribeSecretsResponse::TDescription(Ydb::StatusIds::BAD_REQUEST, { NYql::TIssue("identity case is not specified") }));
    }
}

}  // namespace NKikimr::NKqp
