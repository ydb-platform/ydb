#include "support_links_resolver.h"
#include <ydb/mvp/meta/support_links/source.h>

#include <ydb/mvp/meta/mvp.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>

#include <utility>

namespace NMVP {

TSupportLinksResolver::TSupportLinksResolver(TParams params)
    : ClusterColumns(std::move(params.ClusterColumns))
    , UrlParameters(std::move(params.UrlParameters))
    , Parent(std::move(params.Parent))
    , HttpProxyId(std::move(params.HttpProxyId))
{
    const auto& linkSources = params.EntityType == EEntityType::Database
        ? InstanceMVP->MetaSettings.DatabaseLinkSources
        : InstanceMVP->MetaSettings.ClusterLinkSources;
    Sources = linkSources;
}

auto TSupportLinksResolver::MakeResolveInput(size_t place) const {
    return ILinkSource::TResolveInput{
        .Place = place,
        .ClusterColumns = ClusterColumns,
        .UrlParameters = UrlParameters,
        .Parent = Parent,
        .HttpProxyId = HttpProxyId,
    };
}

void TSupportLinksResolver::Start() {
    SourceOutputs.clear();
    SourceOutputs.resize(Sources.size());
    SourceActors.clear();
    SourceActors.resize(Sources.size());

    for (size_t i = 0; i < Sources.size(); ++i) {
        const auto& source = Sources[i];
        TResolveOutput sourceOutput = source->Resolve(MakeResolveInput(i));
        sourceOutput.Name = source->Config().GetSource();
        SourceActors[i] = sourceOutput.Actor;
        SourceOutputs[i] = std::move(sourceOutput);
    }
}

void TSupportLinksResolver::OnSourceResponse(const NSupportLinks::TEvPrivate::TEvSourceResponse::TPtr& event) {
    const auto* msg = event->Get();
    if (msg->Place < SourceOutputs.size()) {
        TResolveOutput& slot = SourceOutputs[msg->Place];
        slot.Ready = true;
        slot.Links = msg->Links;
        slot.Errors.insert(slot.Errors.end(), msg->Errors.begin(), msg->Errors.end());
        SourceActors[msg->Place].Clear();
    }
}

void TSupportLinksResolver::HandleTimeout() {
    auto* actorSystem = NActors::TActivationContext::ActorSystem();

    for (size_t place = 0; place < SourceOutputs.size(); ++place) {
        auto& sourceOutput = SourceOutputs[place];
        if (!sourceOutput.Ready) {
            if (SourceActors[place]) {
                actorSystem->Send(new NActors::IEventHandle(
                    *SourceActors[place],
                    Parent,
                    new NActors::TEvents::TEvPoisonPill()));
            }
            SourceActors[place].Clear();
            sourceOutput.Errors.emplace_back(NSupportLinks::TSupportError{
                .Source = sourceOutput.Name,
                .Message = "Timeout while resolving support links source"
            });
            sourceOutput.Ready = true;
        }
    }
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
