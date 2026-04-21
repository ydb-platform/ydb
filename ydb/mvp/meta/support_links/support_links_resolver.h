#pragma once

#include "events.h"
#include "source.h"

#include <ydb/mvp/meta/meta_settings.h>
#include <ydb/library/actors/http/http.h>

#include <util/generic/hash.h>
#include <memory>

namespace NMVP {

class TSupportLinksResolver {
public:
    enum class EEntityType {
        Cluster,
        Database,
    };

    struct TParams {
        EEntityType EntityType = EEntityType::Cluster;
        const TMetaSettings* Settings = nullptr;
        THashMap<TString, TString> ClusterInfo;
        NHttp::TUrlParameters UrlParameters;
        NActors::TActorId Owner;
        NActors::TActorId HttpProxyId;
        TLinkSourceFactory LinkSourceFactory = MakeLinkSource;
    };

    explicit TSupportLinksResolver(TParams params);
    void Start();

    void OnSourceResponse(const NSupportLinks::TEvPrivate::TEvSourceResponse::TPtr& event);
    void HandleTimeout();
    const TVector<TResolveOutput>& GetSourceOutput() const;
    bool IsFinished() const;

private:
    void ResetState();
    ILinkSource::TLinkResolveInput MakeResolveInput() const;
    ILinkSource::TResolveContext MakeResolveContext(size_t place) const;
    TResolveOutput ResolveSource(size_t place) const;
    void SaveSourceOutput(size_t place, TResolveOutput sourceOutput);
    void ApplySourceResponse(size_t place, const NSupportLinks::TEvPrivate::TEvSourceResponse& response);
    void HandleSourceTimeout(size_t place, NActors::TActorSystem* actorSystem);

    TVector<std::shared_ptr<ILinkSource>> Sources;
    TVector<TResolveOutput> SourceOutputs;
    TVector<TMaybe<NActors::TActorId>> SourceActors;
    THashMap<TString, TString> ClusterInfo;
    NHttp::TUrlParameters UrlParameters;
    NActors::TActorId Owner;
    NActors::TActorId HttpProxyId;
};

} // namespace NMVP
