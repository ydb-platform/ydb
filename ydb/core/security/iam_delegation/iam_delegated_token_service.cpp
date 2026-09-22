#include "iam_delegated_token_service.h"
#include "iam_actor_base.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/library/actors/async/event.h>
#include <ydb/library/actors/async/sleep.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/ycloud/api/iam_token_service.h>
#include <ydb/library/ycloud/impl/iam_token_service.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::IAM_DELEGATION

namespace NKikimr::NIamDelegation {

using namespace NActors;

class TIamDelegatedTokenService : public TIamActorBase<TIamDelegatedTokenService> {
private:
    using TBase = TIamActorBase<TIamDelegatedTokenService>;

    struct TMintResult {
        TString Token;
        TInstant ExpiresAt;
        Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::SUCCESS;
        grpc::StatusCode GrpcCode = grpc::StatusCode::OK; // of the last IAM answer, when there was one
        TString Error;

        bool IsSuccess() const {
            return Status == Ydb::StatusIds::SUCCESS;
        }
    };

    struct TEntry {
        // current token (empty when none could be obtained yet, the last one expired or IAM refused a new one)
        TString Token;
        TInstant ExpiresAt;
        TInstant NextRefreshAt;
        // last failure, reported when there is no token
        Ydb::StatusIds::StatusCode LastStatus = Ydb::StatusIds::SUCCESS;
        TString LastError;
        ui32 ConsecutiveFailures = 0;
        // incremented after every mint attempt; waiters compare it to detect a new result
        ui64 Generation = 0;
        TAsyncEvent Updated; // a mint attempt finished
        bool LoopRunning = false;
        TInstant LastUse;

        bool HasValidToken(TInstant now) const {
            return !Token.empty() && now < ExpiresAt;
        }
    };

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::IAM_DELEGATED_TOKEN_SERVICE_ACTOR;
    }

    TIamDelegatedTokenService(const TIamDelegationSettings& settings, const TActorId& systemTokenService)
        : TBase(settings, systemTokenService)
    {}

    void Bootstrap() {
        Become(&TThis::StateWork);

        // sensors of the node: mints of delegated tokens and the keys currently cached
        const auto counters = GetServiceCounters(AppData()->Counters, "iam_delegation")->GetSubgroup("component", "token_service");
        Mints = counters->GetCounter("Mints", true);
        MintErrors = counters->GetCounter("MintErrors", true);
        CachedKeys = counters->GetCounter("CachedKeys", false);

        NCloud::TIamTokenServiceSettings clientSettings(Settings.TokenServiceEndpoint, "ydb-iam-delegation");
        clientSettings.EnableSsl = Settings.EnableSsl;
        clientSettings.RequestTimeoutMs = Settings.RequestTimeout.MilliSeconds();
        IamTokenClient = Register(NCloud::CreateIamTokenService(clientSettings));

        YDB_LOG_INFO("Delegated token service started",
            {"tokenServiceEndpoint", Settings.TokenServiceEndpoint},
            {"refreshMargin", Settings.TokenRefreshMargin},
            {"maxCacheLifetime", Settings.MaxTokenCacheLifetime}
        );
    }

    STRICT_STFUNC(StateWork,
        hFunc(TEvIamDelegation::TEvGetToken, HandleGetToken);
        // late replies after a timeout
        IgnoreFunc(NCloud::TEvIamTokenService::TEvCreateForServiceResponse);
        IgnoreFunc(TEvIamDelegation::TEvSystemTokenReady);
        IgnoreFunc(TEvents::TEvUndelivered);
        cFunc(TEvents::TEvPoison::EventType, BeginShutdown);
    )

    STFUNC(StateDying) {
        Y_UNUSED(ev); // PassAway may be waiting for coroutine tasks
    }

private:
    void BeginShutdown() {
        Become(&TThis::StateDying);
        Send(IamTokenClient, new TEvents::TEvPoison());
        *CachedKeys = 0; // the entries die with the actor
        PassAway();
    }

    TEntry* FindEntry(const TTokenKey& key) {
        auto it = Entries.find(key);
        return it == Entries.end() ? nullptr : it->second.get();
    }

    TEntry& EnsureEntry(const TTokenKey& key) {
        auto& entry = Entries[key];
        if (!entry) {
            entry = std::make_unique<TEntry>();
            *CachedKeys = Entries.size();
        }
        entry->LastUse = TActivationContext::Now();
        if (!entry->LoopRunning) {
            entry->LoopRunning = true;
            Spawn(&TThis::RefreshLoop, this, key);
        }
        return *entry;
    }

    // top-level coroutine: one per request
    void HandleGetToken(TEvIamDelegation::TEvGetToken::TPtr ev) {
        const TActorId replyTo = ev->Sender;
        const ui64 cookie = ev->Cookie;
        const TTokenKey key = ev->Get()->Key;
        ev.Reset();

        TEntry& entry = EnsureEntry(key);
        if (!entry.HasValidToken(TActivationContext::Now())) {
            // Wait for the result of the next mint attempt of the refresh loop. A request that arrives while the
            // loop sleeps between failed attempts waits out that backoff (1 s growing to 1 min) too: it is
            // answered by the next attempt, not by the failure the loop already knows. This keeps one attempt
            // per backoff period however many requests arrive, at the price of a slower first answer.
            co_await WithTimeout(MintAttemptTimeout(), &TThis::WaitForNewResult, this, key, entry.Generation + 1);
        }

        auto result = MakeHolder<TEvIamDelegation::TEvGetTokenResult>();
        result->Key = key;
        if (TEntry* current = FindEntry(key); current && current->HasValidToken(TActivationContext::Now())) {
            result->Token = current->Token;
            result->ExpiresAt = current->ExpiresAt;
        } else if (current && current->LastStatus != Ydb::StatusIds::SUCCESS) {
            result->Status = current->LastStatus;
            result->Issues.AddIssue(current->LastError);
        } else {
            result->Status = Ydb::StatusIds::TIMEOUT;
            result->Issues.AddIssue(TStringBuilder() << "timeout while obtaining the IAM token for " << key.ToString());
        }
        Send(replyTo, result.Release(), 0, cookie);
    }

    // Upper bound of one mint attempt: the IAM call with all its retries and their backoff, plus slack.
    TDuration MintAttemptTimeout() const {
        return MaxCallDuration() + TDuration::Seconds(30);
    }

    async<void> WaitForNewResult(TTokenKey key, ui64 targetGeneration) {
        for (;;) {
            TEntry* entry = FindEntry(key);
            if (!entry || entry->Generation >= targetGeneration) {
                co_return;
            }
            if (!co_await entry->Updated.Wait()) {
                co_return; // the entry was destroyed
            }
        }
    }

    // Background refresh loop of one key, a task of the actor (Spawn): mints, publishes the result to the
    // requests parked on the entry, sleeps until the next refresh, and ends when the entry is dropped.
    async<void> RefreshLoop(TTokenKey key) {
        Y_DEFER {
            if (TEntry* entry = FindEntry(key)) {
                entry->LoopRunning = false;
            }
        };

        TBackoff errorBackoff(TDuration::Seconds(1), TDuration::Minutes(1));
        for (;;) {
            if (!FindEntry(key)) {
                co_return;
            }

            const TMintResult mint = co_await Mint(key);

            TEntry* entry = FindEntry(key);
            if (!entry) {
                co_return;
            }
            const TInstant now = TActivationContext::Now();
            TDuration delay;
            ++entry->Generation;
            if (mint.IsSuccess()) {
                Mints->Inc();
                entry->Token = mint.Token;
                entry->ExpiresAt = mint.ExpiresAt;
                entry->LastStatus = Ydb::StatusIds::SUCCESS;
                entry->LastError.clear();
                entry->ConsecutiveFailures = 0;
                errorBackoff.Reset();
                const TInstant refreshAt = Min(entry->ExpiresAt - Settings.TokenRefreshMargin, now + Settings.MaxTokenCacheLifetime);
                // never refresh in a tight loop: wait at least 1s, and at most 5s / half of the token lifetime
                // (tokens shorter than the refresh margin are refreshed once per second at most)
                const TDuration minDelay = Max(TDuration::Seconds(1), Min(TDuration::Seconds(5), (entry->ExpiresAt - now) / 2));
                entry->NextRefreshAt = Max(refreshAt, now + minDelay);
                delay = entry->NextRefreshAt - now;
                YDB_LOG_DEBUG("Token obtained",
                    {"key", key.ToString()},
                    {"expiresAt", entry->ExpiresAt},
                    {"nextRefreshAt", entry->NextRefreshAt}
                );
            } else {
                MintErrors->Inc();
                ++entry->ConsecutiveFailures;
                entry->LastStatus = mint.Status;
                entry->LastError = mint.Error;
                if (!entry->Token.empty() && (now >= entry->ExpiresAt || mint.GrpcCode == grpc::StatusCode::PERMISSION_DENIED)) {
                    // expired, or IAM refuses tokens for the key (the delegation was revoked): stop serving it.
                    // A rejected system token (UNAUTHENTICATED) is a problem of this node, not of the delegation:
                    // the cached token stays valid and is served while the loop retries.
                    entry->Token.clear();
                }
                delay = errorBackoff.Next();
                if (!entry->Token.empty()) {
                    // keep serving the cached token, but retry before it expires
                    delay = Min(delay, Max(entry->ExpiresAt - now - Settings.TokenRefreshMargin / 2, TDuration::Seconds(1)));
                }
                YDB_LOG_WARN("Cannot obtain token",
                    {"key", key.ToString()},
                    {"status", mint.Status},
                    {"error", mint.Error},
                    {"failures", entry->ConsecutiveFailures},
                    {"retryIn", delay}
                );
            }
            entry->Updated.NotifyAll();

            co_await AsyncSleepFor(delay);

            entry = FindEntry(key);
            if (!entry) {
                co_return;
            }
            if (TActivationContext::Now() - entry->LastUse >= Settings.IdleKeyTtl && !entry->Updated.HasAwaiters()) {
                // a request parked on the entry keeps it: it is answered by the next attempt, not by an eviction
                YDB_LOG_DEBUG("Dropping idle token entry", {"key", key.ToString()});
                Entries.erase(key);
                *CachedKeys = Entries.size();
                co_return;
            }
        }
    }

    async<TMintResult> Mint(TTokenKey key) {
        TMintResult result;
        try {
            auto response = co_await CallWithRetry<NCloud::TEvIamTokenService::TEvCreateForServiceRequest, NCloud::TEvIamTokenService::TEvCreateForServiceResponse>(
                IamTokenClient, "IamTokenService.CreateForService",
                [&](yandex::cloud::priv::iam::v1::CreateIamTokenForServiceRequest& request) {
                    request.set_service_id(Settings.ServiceId);
                    request.set_microservice_id(Settings.MicroserviceId);
                    request.set_resource_id(key.CloudId);
                    request.set_resource_type(Settings.ResourceType);
                    request.set_target_service_account_id(key.ServiceAccountId);
                });
            const auto& proto = response->Get()->Response;
            result.Token = proto.iam_token();
            if (result.Token.empty()) {
                result.Status = Ydb::StatusIds::INTERNAL_ERROR;
                result.Error = "IamTokenService returned an empty token";
                co_return result;
            }
            if (!proto.has_expires_at() || proto.expires_at().seconds() <= 0) {
                // IAM always says when a token expires; a token without that is not served, since nothing could
                // tell when it stops being valid
                result.Token.clear();
                result.Status = Ydb::StatusIds::INTERNAL_ERROR;
                result.Error = "IamTokenService returned a token without expires_at";
                co_return result;
            }
            result.ExpiresAt = TInstant::Seconds(proto.expires_at().seconds());
        } catch (const TIamCallError& e) {
            result.Status = e.Status;
            result.GrpcCode = e.GrpcCode;
            result.Error = e.what();
        } catch (const std::exception& e) {
            result.Status = Ydb::StatusIds::INTERNAL_ERROR;
            result.Error = TStringBuilder() << "unexpected exception: " << e.what();
        }
        co_return result;
    }

private:
    TActorId IamTokenClient;
    THashMap<TTokenKey, std::unique_ptr<TEntry>> Entries;
    ::NMonitoring::TDynamicCounters::TCounterPtr Mints;
    ::NMonitoring::TDynamicCounters::TCounterPtr MintErrors;
    ::NMonitoring::TDynamicCounters::TCounterPtr CachedKeys;
};

IActor* CreateIamDelegatedTokenService(const TIamDelegationSettings& settings, const TActorId& systemTokenService) {
    return new TIamDelegatedTokenService(settings, systemTokenService);
}

} // namespace NKikimr::NIamDelegation
