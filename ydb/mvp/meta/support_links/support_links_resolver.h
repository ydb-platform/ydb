#pragma once

#include "events.h"
#include "source.h"

#include <ydb/mvp/meta/meta_settings.h>
#include <ydb/library/actors/http/http.h>

#include <util/generic/hash.h>
#include <memory>

namespace NMVP::NSupportLinks {

class TSupportLinksResolver {
public:
    struct TParams {
        const TMetaSettings* Settings = nullptr;
        TRequestContext RequestContext;
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
    ILinkSource::TLinkResolveInput MakeResolveInput(size_t place) const;
    ILinkSource::TResolveContext MakeResolveContext(size_t place) const;
    TResolveOutput ResolveSource(size_t place) const;
    void SaveSourceOutput(size_t place, TResolveOutput sourceOutput);
    void ApplySourceResponse(size_t place, const NSupportLinks::TEvPrivate::TEvSourceResponse& response);
    void HandleSourceTimeout(size_t place, NActors::TActorSystem* actorSystem);

    TVector<std::shared_ptr<ILinkSource>> Sources;
    TVector<TEntityIdentity> SourceIdentities;
    TVector<TResolveOutput> SourceOutputs;
    TVector<TMaybe<NActors::TActorId>> SourceActors;
    THashMap<TString, TString> ClusterInfo;
    TCgiParameters AdditionalRequestParams;
    NActors::TActorId Owner;
    NActors::TActorId HttpProxyId;
};

} // namespace NMVP::NSupportLinks
