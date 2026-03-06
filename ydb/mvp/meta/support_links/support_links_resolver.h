#pragma once

#include "events.h"

#include <ydb/mvp/meta/mvp.h>

#include <util/generic/maybe.h>
#include <memory>

namespace NMVP {

class ILinkSource;

struct TResolveOutput {
    TString Name;
    bool Ready = false;
    TVector<NSupportLinks::TResolvedLink> Links;
    TVector<NSupportLinks::TSupportError> Errors;
    TMaybe<NActors::TActorId> Actor;
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
        NHttp::TUrlParameters UrlParameters;
        NActors::TActorId Parent;
        NActors::TActorId HttpProxyId;
    };

    explicit TSupportLinksResolver(TParams params);
    void Start();

    void OnSourceResponse(const NSupportLinks::TEvPrivate::TEvSourceResponse::TPtr& event);
    void HandleTimeout();
    const TVector<TResolveOutput>& GetSourceOutput() const;
    bool IsFinished() const;

private:
    auto MakeResolveInput(size_t place) const;

    TVector<std::shared_ptr<ILinkSource>> Sources;
    TVector<TResolveOutput> SourceOutputs;
    TVector<TMaybe<NActors::TActorId>> SourceActors;
    THashMap<TString, TString> ClusterColumns;
    NHttp::TUrlParameters UrlParameters;
    NActors::TActorId Parent;
    NActors::TActorId HttpProxyId;
};

} // namespace NMVP
