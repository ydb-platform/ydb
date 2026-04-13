#pragma once

#include <ydb/mvp/meta/support_links/source.h>
#include <ydb/mvp/meta/support_links/events.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorid.h>

namespace NMVP::NTest {

inline TVector<NSupportLinks::TResolvedLink> MakeMockLinks() {
    return {
        NSupportLinks::TResolvedLink{
            .Title = "Grafana Dashboard",
            .Url = "https://grafana.example.com/d/mock-dashboard",
        },
        NSupportLinks::TResolvedLink{
            .Title = "Runbook",
            .Url = "https://wiki.example.com/runbooks/mock-dashboard",
        }
    };
}

class TMockLinkSourceSync final : public ILinkSource {
public:
    explicit TMockLinkSourceSync(TSupportLinkEntryConfig config)
        : SourceConfig(std::move(config))
    {}

    TResolveOutput Resolve(const TLinkResolveInput&, const TResolveContext&) const override {
        TResolveOutput out;
        out.Name = SourceConfig.GetSource();
        out.Links = MakeMockLinks();
        return out;
    }

private:
    TSupportLinkEntryConfig SourceConfig;
};

class TMockReplyActor final : public NActors::TActorBootstrapped<TMockReplyActor> {
public:
    TMockReplyActor(NActors::TActorId owner, size_t place)
        : Owner(owner)
        , Place(place)
    {}

    void Bootstrap() {
        Send(Owner, new NSupportLinks::TEvPrivate::TEvSourceResponse(
            Place,
            MakeMockLinks(),
            {}));
        PassAway();
    }

private:
    NActors::TActorId Owner;
    size_t Place = 0;
};

class TMockLinkSourceAsync final : public ILinkSource {
public:
    explicit TMockLinkSourceAsync(TSupportLinkEntryConfig config)
        : SourceConfig(std::move(config))
    {}

    TResolveOutput Resolve(const TLinkResolveInput&, const TResolveContext& context) const override {
        TResolveOutput out;
        out.Name = SourceConfig.GetSource();
        auto actorId = NActors::TActivationContext::Register(
            new TMockReplyActor(context.Owner, context.Place),
            context.Owner);
        out.Actor = actorId;
        return out;
    }

private:
    TSupportLinkEntryConfig SourceConfig;
};

inline std::shared_ptr<ILinkSource> MakeMockLinkSourceSync(TSupportLinkEntryConfig config) {
    return std::make_shared<TMockLinkSourceSync>(std::move(config));
}

inline std::shared_ptr<ILinkSource> MakeMockLinkSourceAsync(TSupportLinkEntryConfig config) {
    return std::make_shared<TMockLinkSourceAsync>(std::move(config));
}

} // namespace NMVP::NTest
