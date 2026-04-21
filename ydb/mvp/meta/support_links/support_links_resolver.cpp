#include "support_links_resolver.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>

#include <util/generic/yexception.h>
#include <utility>

namespace NMVP {

namespace {

TVector<std::shared_ptr<ILinkSource>> BuildSources(const TSupportLinksResolver::TParams& params) {
    if (!params.Settings) {
        ythrow yexception() << "support links settings are required";
    }
    const auto& linkConfigs = params.EntityType == TSupportLinksResolver::EEntityType::Database
        ? params.Settings->SupportLinks.DatabaseLinks
        : params.Settings->SupportLinks.ClusterLinks;
    TVector<std::shared_ptr<ILinkSource>> linkSources;
    linkSources.reserve(linkConfigs.size());
    for (const auto& linkConfig : linkConfigs) {
        linkSources.push_back(params.LinkSourceFactory(linkConfig, *params.Settings));
    }
    return linkSources;
}

} // namespace

TSupportLinksResolver::TSupportLinksResolver(TParams params)
    : Sources(BuildSources(params))
    , ClusterInfo(std::move(params.ClusterInfo))
    , UrlParameters(std::move(params.UrlParameters))
    , Owner(std::move(params.Owner))
    , HttpProxyId(std::move(params.HttpProxyId))
{}

ILinkSource::TLinkResolveInput TSupportLinksResolver::MakeResolveInput() const {
    return ILinkSource::TLinkResolveInput{
        .ClusterInfo = ClusterInfo,
        .UrlParameters = UrlParameters,
    };
}

ILinkSource::TResolveContext TSupportLinksResolver::MakeResolveContext(size_t place) const {
    return ILinkSource::TResolveContext{
        .Place = place,
        .Owner = Owner,
        .HttpProxyId = HttpProxyId,
    };
}

void TSupportLinksResolver::ResetState() {
    SourceOutputs.clear();
    SourceOutputs.resize(Sources.size());
    SourceActors.clear();
    SourceActors.resize(Sources.size());
}

TResolveOutput TSupportLinksResolver::ResolveSource(size_t place) const {
    return Sources[place]->Resolve(MakeResolveInput(), MakeResolveContext(place));
}

void TSupportLinksResolver::SaveSourceOutput(size_t place, TResolveOutput sourceOutput) {
    SourceActors[place] = sourceOutput.Actor;
    SourceOutputs[place] = std::move(sourceOutput);
}

void TSupportLinksResolver::ApplySourceResponse(size_t place, const NSupportLinks::TEvPrivate::TEvSourceResponse& response) {
    TResolveOutput& slot = SourceOutputs[place];
    slot.Links = response.Links;
    slot.Errors.insert(slot.Errors.end(), response.Errors.begin(), response.Errors.end());
    SourceActors[place].Clear();
}

void TSupportLinksResolver::HandleSourceTimeout(size_t place, NActors::TActorSystem* actorSystem) {
    auto& sourceOutput = SourceOutputs[place];
    actorSystem->Send(new NActors::IEventHandle(
        *SourceActors[place],
        Owner,
        new NActors::TEvents::TEvPoisonPill()));
    SourceActors[place].Clear();
    sourceOutput.Errors.emplace_back(NSupportLinks::TSupportError{
        .Source = sourceOutput.Name,
        .Message = "Timeout while resolving support links source"
    });
}

void TSupportLinksResolver::Start() {
    ResetState();

    for (size_t i = 0; i < Sources.size(); ++i) {
        TResolveOutput sourceOutput = ResolveSource(i);
        SaveSourceOutput(i, std::move(sourceOutput));
    }
}

void TSupportLinksResolver::OnSourceResponse(const NSupportLinks::TEvPrivate::TEvSourceResponse::TPtr& event) {
    const auto* msg = event->Get();
    if (msg->Place >= SourceOutputs.size()) {
        return;
    }
    ApplySourceResponse(msg->Place, *msg);
}

void TSupportLinksResolver::HandleTimeout() {
    auto* actorSystem = NActors::TActivationContext::ActorSystem();

    for (size_t place = 0; place < SourceOutputs.size(); ++place) {
        if (SourceActors[place]) {
            HandleSourceTimeout(place, actorSystem);
        }
    }
}

bool TSupportLinksResolver::IsFinished() const {
    for (const auto& sourceActor : SourceActors) {
        if (sourceActor) {
            return false;
        }
    }
    return true;
}

const TVector<TResolveOutput>& TSupportLinksResolver::GetSourceOutput() const {
    return SourceOutputs;
}

} // namespace NMVP
