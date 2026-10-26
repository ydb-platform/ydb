#pragma once

#include "meta_cluster_info.h"
#include "mvp.h"

#include <ydb/mvp/core/core_ydb_impl.h>
#include <ydb/mvp/core/mvp_tokens.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <library/cpp/uri/uri.h>
#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/generic/vector.h>
#include <util/string/ascii.h>

namespace NMVP::NClusterRedirect {

inline bool IsValidUrlText(TStringBuf value) {
    for (size_t i = 0; i < value.size(); ++i) {
        const unsigned char c = value[i];
        if (c <= ' ' || c == 0x7f || c == '\\' || c == '#') {
            return false;
        }
        if (c == '%' && (i + 2 >= value.size() || !IsAsciiHex(value[i + 1]) || !IsAsciiHex(value[i + 2]))) {
            return false;
        }
    }
    return true;
}

inline TString NormalizeBalancer(TStringBuf balancer) {
    NUri::TUri uri;
    if (!IsValidUrlText(balancer)
            || uri.ParseUri(balancer, NUri::TFeature::FeaturesDefaultOrSchemeKnown) != NUri::TState::ParsedOK
            || (uri.GetScheme() != NUri::TScheme::SchemeHTTP && uri.GetScheme() != NUri::TScheme::SchemeHTTPS)
            || uri.GetField(NUri::TField::FieldHost).empty()
            || (uri.GetFieldMask() & (NUri::TField::FlagAuth | NUri::TField::FlagQuery | NUri::TField::FlagFragment))) {
        return {};
    }
    balancer.ChopSuffix("/");
    if (!balancer.ChopSuffix("/viewer/json")) {
        balancer.ChopSuffix("/viewer");
    }
    return TString(balancer);
}

// Caller credentials are part of the key only when meta has no service token.
using TCacheKey = std::pair<TString, TString>;

struct TBalancerResult {
    NYdb::TStatus Status;
    TString Balancer;
    bool Found = false;
};

inline TBalancerResult ExtractBalancer(const NYdb::NTable::TDataQueryResult& result) {
    if (!result.IsSuccess()) {
        return {result, {}};
    }
    THashMap<TString, TString> clusterInfo;
    if (!TryExtractClusterInfo(result.GetResultSet(0), clusterInfo)) {
        return {result, {}};
    }
    TString balancer = NormalizeBalancer(clusterInfo.Value("balancer", TString()));
    if (balancer.empty()) {
        return {NYdb::TStatus(NYdb::EStatus::UNAVAILABLE,
            {NYdb::NIssue::TIssue("Cluster balancer is not configured as an HTTP(S) URL")}), {}};
    }
    return {result, std::move(balancer), true};
}

struct TEvBalancer {
    enum EEv {
        EvGet = THandlerActorYdb::TEvPrivate::EvEnd,
        EvResult,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE));

    struct TEvGet : NActors::TEventLocal<TEvGet, EvGet> {
        TCacheKey Key;

        explicit TEvGet(TCacheKey key)
            : Key(std::move(key))
        {}
    };

    struct TEvResult : NActors::TEventLocal<TEvResult, EvResult> {
        TBalancerResult Result;
        TCacheKey Key;

        explicit TEvResult(TBalancerResult result, TCacheKey key = {})
            : Result(std::move(result))
            , Key(std::move(key))
        {}
    };
};

class TBalancerQueryActor : private THandlerActorYdb, public NActors::TActorBootstrapped<TBalancerQueryActor> {
    const TYdbLocation& Location;
    const NActors::TActorId Owner;
    const TCacheKey Key;
    TMaybe<NYdb::NTable::TSession> Session;

    void ReplyAndPassAway(TBalancerResult result) {
        Send(Owner, new TEvBalancer::TEvResult(std::move(result), Key));
        PassAway();
    }

public:
    TBalancerQueryActor(const TYdbLocation& location, NActors::TActorId owner, TCacheKey key)
        : Location(location)
        , Owner(owner)
        , Key(std::move(key))
    {}

    void Bootstrap() {
        Become(&TBalancerQueryActor::StateWork, GetTimeout(), new NActors::TEvents::TEvWakeup());
        TString token = Key.second;
        // Resolve the service token again on each refresh so token rotation is respected.
        if (!TMVP::MetaDatabaseTokenName.empty()) {
            if (auto* tokenator = MVPAppData()->Tokenator) {
                token = tokenator->GetToken(TMVP::MetaDatabaseTokenName);
            }
        }
        auto settings = NYdb::NTable::TClientSettings()
            .Database(Location.RootDomain).AuthToken(token).DiscoveryMode(NYdb::EDiscoveryMode::Async);
        auto* actorSystem = NActors::TActivationContext::ActorSystem();
        auto actorId = SelfId();
        Location.GetTableClient(settings).CreateSession().Subscribe(
            [actorId, actorSystem](NYdb::NTable::TAsyncCreateSessionResult result) {
                actorSystem->Send(actorId, new TEvPrivate::TEvCreateSessionResult(result.ExtractValue()));
            });
    }

    void Handle(TEvPrivate::TEvCreateSessionResult::TPtr event) {
        const auto& result = event->Get()->Result;
        if (!result.IsSuccess()) {
            ReplyAndPassAway({result, {}});
            return;
        }
        Session = result.GetSession();
        auto* actorSystem = NActors::TActivationContext::ActorSystem();
        auto actorId = SelfId();
        Session->ExecuteDataQuery(
            BuildClusterInfoQuery(Location.RootDomain),
            NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::OnlineRO()).CommitTx(),
            BuildClusterInfoQueryParams(Key.first))
            .Subscribe([actorId, actorSystem, session = Session](NYdb::NTable::TAsyncDataQueryResult result) {
                actorSystem->Send(actorId, new TEvPrivate::TEvDataQueryResult(result.ExtractValue()));
            });
    }

    void Handle(TEvPrivate::TEvDataQueryResult::TPtr event) {
        ReplyAndPassAway(ExtractBalancer(event->Get()->Result));
    }

    void HandleTimeout() {
        ReplyAndPassAway({NYdb::TStatus(NYdb::EStatus::TIMEOUT, {}), {}});
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvCreateSessionResult, Handle);
            hFunc(TEvPrivate::TEvDataQueryResult, Handle);
            cFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }
};

class TBalancerCacheActor : public NActors::TActorBootstrapped<TBalancerCacheActor> {
    struct TEntry {
        TMaybe<TBalancerResult> Value;
        NActors::TActorId QueryActor;
        TVector<NActors::TActorId> Waiters;
        TInstant RefreshAt;
        TInstant LastAccess;
    };

    const TYdbLocation& Location;
    THashMap<TCacheKey, TEntry> Entries;

    void StartQuery(const TCacheKey& key, TEntry& entry) {
        entry.QueryActor = Register(new TBalancerQueryActor(Location, SelfId(), key));
    }

public:
    explicit TBalancerCacheActor(const TYdbLocation& location)
        : Location(location)
    {}

    void Bootstrap() {
        Become(&TBalancerCacheActor::StateWork);
        Schedule(TDuration::Seconds(1), new NActors::TEvents::TEvWakeup());
    }

    void Handle(TEvBalancer::TEvGet::TPtr event) {
        const auto& key = event->Get()->Key;
        auto& entry = Entries[key];
        entry.LastAccess = NActors::TActivationContext::Now();
        if (entry.Value) {
            Send(event->Sender, new TEvBalancer::TEvResult(*entry.Value));
            return;
        }
        entry.Waiters.push_back(event->Sender);
        if (!entry.QueryActor) {
            StartQuery(key, entry);
        }
    }

    void Handle(TEvBalancer::TEvResult::TPtr event) {
        auto it = Entries.find(event->Get()->Key);
        if (it == Entries.end() || it->second.QueryActor != event->Sender) {
            return;
        }
        auto& entry = it->second;
        const auto& result = event->Get()->Result;
        entry.QueryActor = {};
        if (result.Status.IsSuccess()) {
            // A successful lookup of a deleted cluster replaces the old address with "not found".
            entry.Value = result;
        }
        for (const auto& waiter : entry.Waiters) {
            Send(waiter, new TEvBalancer::TEvResult(result));
        }
        entry.Waiters.clear();
        if (!entry.Value) {
            // An initial error must not become a cached result.
            Entries.erase(it);
            return;
        }
        entry.RefreshAt = NActors::TActivationContext::Now() + TDuration::Seconds(60);
    }

    void HandleRefresh() {
        const auto now = NActors::TActivationContext::Now();
        for (auto it = Entries.begin(); it != Entries.end();) {
            auto& entry = it->second;
            if (!entry.QueryActor && now - entry.LastAccess >= TDuration::Days(7)) {
                Entries.erase(it++);
                continue;
            }
            if (!entry.QueryActor && entry.Value && entry.RefreshAt <= now) {
                StartQuery(it->first, entry);
            }
            ++it;
        }
        Schedule(TDuration::Seconds(1), new NActors::TEvents::TEvWakeup());
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBalancer::TEvGet, Handle);
            hFunc(TEvBalancer::TEvResult, Handle);
            cFunc(NActors::TEvents::TSystem::Wakeup, HandleRefresh);
        }
    }
};

} // namespace NMVP::NClusterRedirect
