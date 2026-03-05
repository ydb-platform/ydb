#include "support_links_resolver.h"
#include <ydb/mvp/meta/link_source.h>

#include <utility>

namespace NMVP {

TSupportLinksResolver::TSupportLinksResolver(TParams params) {
    Y_ABORT_UNLESS(InstanceMVP, "InstanceMVP must be initialized");
    const auto& linkSources = params.EntityType == EEntityType::Database
        ? InstanceMVP->MetaSettings.DatabaseLinkSources
        : InstanceMVP->MetaSettings.ClusterLinkSources;

    Sources.clear();
    Sources.reserve(linkSources.size());
    for (const auto& source : linkSources) {
        Sources.push_back(source.get());
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

    for (size_t i = 0; i < Sources.size(); ++i) {
        const ILinkSource* source = Sources[i];
        TSourceOutput sourceOutput = source->Resolve(MakeResolveInput(i));
        sourceOutput.Name = source->Config().GetSource();
        SourceOutputs.push_back(std::move(sourceOutput));
    }
}

void TSupportLinksResolver::OnSourceResponse(const NSupportLinks::TEvPrivate::TEvSourceResponse::TPtr& event) {
    const auto* msg = event->Get();
    if (msg->Place < SourceOutputs.size()) {
        TSourceOutput& slot = SourceOutputs[msg->Place];
        slot.Ready = true;
        slot.Links = msg->Links;
        slot.Errors.insert(slot.Errors.end(), msg->Errors.begin(), msg->Errors.end());
    }
}

void TSupportLinksResolver::HandleTimeout() {
    for (auto& sourceOutput : SourceOutputs) {
        if (!sourceOutput.Ready) {
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
    TSourceOutput& slot = SourceOutputs[place];
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

const TVector<TSourceOutput>& TSupportLinksResolver::GetSourceOutput() const {
    return SourceOutputs;
}

} // namespace NMVP
