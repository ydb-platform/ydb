#pragma once

#include "events.h"
#include "types.h"

#include <ydb/mvp/meta/meta_settings.h>
#include <ydb/library/actors/http/http.h>

#include <util/generic/hash.h>
#include <memory>

namespace NMVP {

class ILinkSource;

class TSupportLinksResolver {
public:
    enum class EEntityType {
        Cluster,
        Database,
    };

    struct TParams {
        EEntityType EntityType = EEntityType::Cluster;
        const TMetaSettings* Settings = nullptr;
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
