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

    const TSupportLinkEntryConfig& Config() const override {
        return SourceConfig;
    }

    TResolveOutput Resolve(const TResolveInput&) const override {
        TResolveOutput out;
        out.Name = SourceConfig.GetSource();
        out.Ready = true;
        out.Links = MakeMockLinks();
        return out;
    }

private:
    TSupportLinkEntryConfig SourceConfig;
};

class TMockReplyActor final : public NActors::TActorBootstrapped<TMockReplyActor> {
public:
    TMockReplyActor(NActors::TActorId parent, size_t place)
        : Parent(parent)
        , Place(place)
    {}

    void Bootstrap() {
        Send(Parent, new NSupportLinks::TEvPrivate::TEvSourceResponse(
            Place,
            MakeMockLinks(),
            {}));
        PassAway();
    }

private:
    NActors::TActorId Parent;
    size_t Place = 0;
};

class TMockLinkSourceAsync final : public ILinkSource {
public:
    explicit TMockLinkSourceAsync(TSupportLinkEntryConfig config)
        : SourceConfig(std::move(config))
    {}

    const TSupportLinkEntryConfig& Config() const override {
        return SourceConfig;
    }

    TResolveOutput Resolve(const TResolveInput& input) const override {
        TResolveOutput out;
        out.Name = SourceConfig.GetSource();
        out.Ready = false;
        auto actorId = NActors::TActivationContext::Register(
            new TMockReplyActor(input.Parent, input.Place),
            input.Parent);
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

inline void RegisterMockLinkSources() {
    RegisterLinkSource("mock/sourceSync", &MakeMockLinkSourceSync);
    RegisterLinkSource("mock/sourceAsync", &MakeMockLinkSourceAsync);
}

} // namespace NMVP::NTest
