#include "resolution_context.h"
#include <ydb/mvp/meta/link_source.h>

#include <utility>

namespace NMVP {

std::unique_ptr<TResolutionContext> TResolutionContext::Build(TParams params)
{
    auto context = std::unique_ptr<TResolutionContext>(new TResolutionContext());
    context->Initialize(std::move(params));
    return context;
}

void TResolutionContext::Initialize(TParams params) {
    Sources = std::move(params.Sources);
    ClusterColumns = std::move(params.ClusterColumns);
    QueryParams = std::move(params.QueryParams);
    Parent = std::move(params.Parent);
    HttpProxyId = std::move(params.HttpProxyId);
    ActorsToRegister.clear();
    SourceOutputs.clear();
    SourceOutputs.reserve(Sources.size());
    for (const ILinkSource* source : Sources) {
        SourceOutputs.push_back(TSourceOutput{
            .Name = source ? source->Config().GetSource() : TString{},
        });
    }
}

void TResolutionContext::Start() {
    ActorsToRegister.clear();
    for (size_t i = 0; i < Sources.size(); ++i) {
        const ILinkSource* source = Sources[i];
        if (!source) {
            continue;
        }
        SourceOutputs[i] = source->Resolve(ILinkSource::TResolveInput{
            .Place = i,
            .ClusterColumns = ClusterColumns,
            .QueryParams = QueryParams,
            .Parent = Parent,
            .HttpProxyId = HttpProxyId,
            .ActorsToRegister = &ActorsToRegister,
        });

        SourceOutputs[i].Name = source->Config().GetSource();
    }
}

TVector<NActors::IActor*> TResolutionContext::DetachActorsToRegister() {
    return std::exchange(ActorsToRegister, {});
}

void TResolutionContext::OnSourceResponse(const NSupportLinks::TEvPrivate::TEvSourceResponse::TPtr& event) {
    const auto* msg = event->Get();
    if (msg->Place < SourceOutputs.size()) {
        TSourceOutput& slot = SourceOutputs[msg->Place];
        slot.Waiting = false;
        slot.Links = msg->Links;
        slot.Errors.insert(slot.Errors.end(), msg->Errors.begin(), msg->Errors.end());
    }
}

void TResolutionContext::HandleTimeout() {
    for (auto& sourceOutput : SourceOutputs) {
        if (sourceOutput.Waiting) {
            sourceOutput.Errors.emplace_back(NSupportLinks::TSupportError{
                .Source = sourceOutput.Name,
                .Message = "Timeout while resolving support links source"
            });
            sourceOutput.Waiting = false;
        }
    }
}

void TResolutionContext::ReportResolved(
    size_t place,
    TVector<NSupportLinks::TResolvedLink> links,
    TVector<NSupportLinks::TSupportError> errors)
{
    if (place >= SourceOutputs.size()) {
        return;
    }
    TSourceOutput& slot = SourceOutputs[place];
    if (!slot.Waiting) {
        return;
    }
    slot.Links = std::move(links);
    slot.Errors = std::move(errors);
    slot.Waiting = false;
}

bool TResolutionContext::IsFinished() const {
    return !Waiting();
}

const TVector<TSourceOutput>& TResolutionContext::GetSourceOutput() const {
    return SourceOutputs;
}

bool TResolutionContext::Waiting() const {
    for (const auto& sourceOutput : SourceOutputs) {
        if (sourceOutput.Waiting) {
            return true;
        }
    }
    return false;
}

} // namespace NMVP
