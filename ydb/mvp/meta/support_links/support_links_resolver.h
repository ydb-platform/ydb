#pragma once

#include "events.h"
#include "types.h"

#include <memory>

namespace NMVP {

class ILinkSource;

struct TResolveOutput {
    TString Name;
    bool Ready = false;
    TVector<NSupportLinks::TResolvedLink> Links;
    TVector<NSupportLinks::TSupportError> Errors;
    TVector<NActors::TActorId> Actors;
};

class TSupportLinksResolver {
public:
    enum class EEntityType {
        Cluster,
        Database,
    };

    struct TParams {
        EEntityType EntityType = EEntityType::Cluster;
        THashMap<TString, TString> ClusterColumns;
        TVector<std::pair<TString, TString>> QueryParams;
        NActors::TActorId Parent;
        NActors::TActorId HttpProxyId;
    };

    explicit TSupportLinksResolver(TParams params);
    void Start();

    void OnSourceResponse(const NSupportLinks::TEvPrivate::TEvSourceResponse::TPtr& event);
    void HandleTimeout();
    void ReportResolved(size_t place, TVector<NSupportLinks::TResolvedLink> links, TVector<NSupportLinks::TSupportError> errors);
    const TVector<TResolveOutput>& GetSourceOutput() const;
    bool IsFinished() const;

private:
    auto MakeResolveInput(size_t place) const;

    TVector<const ILinkSource*> Sources;
    TVector<TResolveOutput> SourceOutputs;
    TVector<TVector<NActors::TActorId>> SourceActors;
    THashMap<TString, TString> ClusterColumns;
    TVector<std::pair<TString, TString>> QueryParams;
    NActors::TActorId Parent;
    NActors::TActorId HttpProxyId;
};

} // namespace NMVP
