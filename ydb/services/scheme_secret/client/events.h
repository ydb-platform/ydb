#pragma once

#include <ydb/core/base/events.h>
#include <ydb/core/kqp/common/events/script_executions.h>

#include <library/cpp/retry/retry_policy.h>
#include <library/cpp/threading/future/future.h>

namespace NKikimr::NSecret {

struct TDescribeSecretSettings {
    IRetryPolicy<>::TPtr RetryPolicy;
    bool ForStoragePool = false;
};

inline NActors::TActorId MakeDescribeSchemaSecretServiceId(ui32 nodeId) {
    constexpr char name[NActors::TActorId::MaxServiceIDLength] = "desc_secret";
    return NActors::TActorId(nodeId, TStringBuf(name, NActors::TActorId::MaxServiceIDLength));
}

struct TEvResolveSecret : public NActors::TEventLocal<TEvResolveSecret, EventSpaceBegin(TKikimrEvents::ES_PRIVATE)> {
public:
    TEvResolveSecret(
        const TIntrusiveConstPtr<NACLib::TUserToken> userToken,
        const TString& database,
        const TVector<TString>& secretNames,
        NThreading::TPromise<NKqp::TEvDescribeSecretsResponse::TDescription> promise,
        TDescribeSecretSettings settings = {}
    )
        : UserToken(userToken)
        , Database(database)
        , SecretNames(secretNames)
        , Promise(promise)
        , Settings(std::move(settings))
    {
        Y_ENSURE(!Database.empty(), "Database name must be set in secret requests");
    }

    THolder<TEvResolveSecret> MakeCopy() const {
        return MakeHolder<TEvResolveSecret>(UserToken, Database, SecretNames, Promise, Settings);
    }

public:
    const TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    const TString Database;
    const TVector<TString> SecretNames;
    NThreading::TPromise<NKqp::TEvDescribeSecretsResponse::TDescription> Promise;
    const TDescribeSecretSettings Settings;
};

} // namespace NKikimr::NSecret
