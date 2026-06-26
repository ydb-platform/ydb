#pragma once

#include <ydb/core/base/feature_flags.h>
#include <ydb/core/kqp/common/events/script_executions.h>

#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/core/actor.h>

#include <library/cpp/retry/retry_policy.h>
#include <library/cpp/threading/future/future.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr::NSecret {

struct TDescribeSecretSettings {
    IRetryPolicy<>::TPtr RetryPolicy;
};

IRetryPolicy<>::TPtr MakeShortRetryPolicy();
IRetryPolicy<>::TPtr MakeLongRetryPolicy();

NThreading::TFuture<NKqp::TEvDescribeSecretsResponse::TDescription> DescribeSecret(
    const TVector<TString>& secretNames,
    const TIntrusiveConstPtr<NACLib::TUserToken> userToken,
    const TString& database,
    NActors::TActorSystem* actorSystem,
    TDescribeSecretSettings settings = {}
);

bool UseSchemaSecrets(const NKikimr::TFeatureFlags& flags, const TVector<TString>& secretNames);
bool UseSchemaSecrets(const NKikimr::TFeatureFlags& flags, const TString& secretName);

}  // namespace NKikimr::NSecret
