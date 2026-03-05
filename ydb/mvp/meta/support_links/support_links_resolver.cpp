#include "support_links_resolver.h"
#include <ydb/mvp/meta/support_links/source.h>

#include <ydb/mvp/meta/mvp.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>

#include <utility>

namespace NMVP {

TSupportLinksResolver::TSupportLinksResolver(TParams params) {
    Y_ABORT_UNLESS(InstanceMVP, "InstanceMVP must be initialized");
    const auto& linkConfigs = params.EntityType == EEntityType::Database
        ? InstanceMVP->SupportLinksConfig.Database
        : InstanceMVP->SupportLinksConfig.Cluster;

    OwnedSources.clear();
    OwnedSources.reserve(linkConfigs.size());
    Sources.clear();
    Sources.reserve(linkConfigs.size());
    for (size_t i = 0; i < linkConfigs.size(); ++i) {
        OwnedSources.push_back(MakeLinkSource(i, linkConfigs[i]));
        Sources.push_back(OwnedSources.back().get());
    }

    ClusterColumns = std::move(params.ClusterColumns);
    QueryParams = std::move(params.QueryParams);
    Parent = std::move(params.Parent);
    HttpProxyId = std::move(params.HttpProxyId);
}

auto TSupportLinksResolver::MakeResolveInput(size_t place) const {
    return ILinkSource::TResolveInput{
        .Place = place,
        .ClusterColumns = ClusterColumns,
        .QueryParams = QueryParams,
        .Parent = Parent,
        .HttpProxyId = HttpProxyId,
    };
}

void TSupportLinksResolver::Start() {
    SourceOutputs.clear();
    SourceOutputs.reserve(Sources.size());
    SourceActors.clear();
    SourceActors.reserve(Sources.size());

    for (size_t i = 0; i < Sources.size(); ++i) {
        const ILinkSource* source = Sources[i];
        TResolveOutput sourceOutput = source->Resolve(MakeResolveInput(i));
        sourceOutput.Name = source->Config().Source;
        SourceActors.push_back(sourceOutput.Actors);
        SourceOutputs.push_back(std::move(sourceOutput));
    }
}

void TSupportLinksResolver::OnSourceResponse(const NSupportLinks::TEvPrivate::TEvSourceResponse::TPtr& event) {
    const auto* msg = event->Get();
    if (msg->Place < SourceOutputs.size()) {
        TResolveOutput& slot = SourceOutputs[msg->Place];
        slot.Ready = true;
        slot.Links = msg->Links;
        slot.Errors.insert(slot.Errors.end(), msg->Errors.begin(), msg->Errors.end());
        if (msg->Place < SourceActors.size()) {
            SourceActors[msg->Place].clear();
        }
    }
}

void TSupportLinksResolver::HandleTimeout() {
    auto* actorSystem = NActors::TActivationContext::ActorSystem();
    Y_ABORT_UNLESS(actorSystem, "ActorSystem is unavailable in activation context");

    for (size_t place = 0; place < SourceOutputs.size(); ++place) {
        auto& sourceOutput = SourceOutputs[place];
        if (!sourceOutput.Ready) {
            if (place < SourceActors.size()) {
                for (const auto& actorId : SourceActors[place]) {
                    actorSystem->Send(new NActors::IEventHandle(
                        actorId,
                        Parent,
                        new NActors::TEvents::TEvPoisonPill()));
                }
                SourceActors[place].clear();
            }
            sourceOutput.Errors.emplace_back(NSupportLinks::TSupportError{
                .Source = sourceOutput.Name,
                .Message = "Timeout while resolving support links source"
            });
            sourceOutput.Ready = true;
        }
    }
}

void TSupportLinksResolver::ReportResolved(
    size_t place,
    TVector<NSupportLinks::TResolvedLink> links,
    TVector<NSupportLinks::TSupportError> errors)
{
    if (place >= SourceOutputs.size()) {
        return;
    }
    TResolveOutput& slot = SourceOutputs[place];
    if (slot.Ready) {
        return;
    }
    slot.Links = std::move(links);
    slot.Errors = std::move(errors);
    slot.Ready = true;
}

bool TSupportLinksResolver::IsFinished() const {
    for (const auto& sourceOutput : SourceOutputs) {
        if (!sourceOutput.Ready) {
            return false;
        }
    }
    return true;
}

const TVector<TResolveOutput>& TSupportLinksResolver::GetSourceOutput() const {
    return SourceOutputs;
}

} // namespace NMVP
