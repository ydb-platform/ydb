#include "fetch_database.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/path.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <library/cpp/retry/retry_policy.h>

namespace NKikimr::NMetadata::NModifications {

namespace {

class TDatabaseFetcherActor : public TActorBootstrapped<TDatabaseFetcherActor> {
    using TBase = TActorBootstrapped<TDatabaseFetcherActor>;
    using TRetryPolicy = IRetryPolicy<>;

public:
    explicit TDatabaseFetcherActor(const TString& database)
        : Database(database)
    {}

    void Registered(TActorSystem* sys, const TActorId& owner) override {
        TBase::Registered(sys, owner);
        Owner = owner;
    }

    void Bootstrap() {
        StartRequest();
        Become(&TDatabaseFetcherActor::StateFunc);
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev) {
        if (ev->Get()->Reason == NActors::TEvents::TEvUndelivered::ReasonActorUnknown && ScheduleRetry()) {
            return;
        }

        Reply("Scheme cache is unavailable");
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& results = ev->Get()->Request->ResultSet;
        Y_ABORT_UNLESS(results.size() == 1);

        const auto& result = results[0];
        if (result.DomainInfo) {
            Serverless = result.DomainInfo->IsServerless();
            Reply();
            return;
        }

        if (result.Status == NSchemeCache::TSchemeCacheNavigate::EStatus::LookupError && ScheduleRetry()) {
            return;
        }

        Reply(TStringBuilder() << "Failed to fetch database info: " << result.Status);
    }

    STRICT_STFUNC(StateFunc,
        sFunc(TEvents::TEvWakeup, StartRequest);
        hFunc(TEvents::TEvUndelivered, Handle);
        hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
    )

private:
    void StartRequest() {
        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        request->DatabaseName = CanonizePath(SplitPath(Database ? Database : AppData()->TenantName));
        request->UserToken = MakeIntrusive<NACLib::TUserToken>(BUILTIN_ACL_METADATA, TVector<NACLib::TSID>{});
        request->ResultSet[0].Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()), IEventHandle::FlagTrackDelivery);
    }

    bool ScheduleRetry() {
        if (!RetryState) {
            RetryState = TRetryPolicy::GetFixedIntervalPolicy(
                  [](){return ERetryErrorClass::ShortRetry;}
                , TDuration::MilliSeconds(100)
                , TDuration::MilliSeconds(500)
                , 100
            )->CreateRetryState();;
        }

        if (const auto delay = RetryState->GetNextRetryDelay()) {
            this->Schedule(*delay, new TEvents::TEvWakeup());
            return true;
        }

        return false;
    }

    void Reply(const std::optional<TString>& errorMessage = std::nullopt) {
        Send(Owner, new TEvFetchDatabaseResponse(Serverless, errorMessage));
        PassAway();
    }

private:
    const TString Database;
    TActorId Owner;
    TRetryPolicy::IRetryState::TPtr RetryState;
    bool Serverless = false;
};

}  // anonymous namespace

IActor* CreateDatabaseFetcherActor(const TString& database) {
    return new TDatabaseFetcherActor(database);
}

}  // NKikimr::NMetadata::NModifications
