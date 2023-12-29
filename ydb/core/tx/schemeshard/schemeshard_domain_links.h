#pragma once

#include "schemeshard.h"
#include "schemeshard_types.h"

#include <ydb/core/base/subdomain.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>

#include <ydb/library/actors/core/actorid.h>

namespace NKikimr {
namespace NSchemeShard {

class TSchemeShard;

class TParentDomainLink {
    TSchemeShard* Self;
    TActorId Pipe;
    NTabletPipe::TClientConfig PipeClientConfig;

public:
    TParentDomainLink(TSchemeShard* self);
    void Shutdown(const TActorContext& ctx);
    THolder<TEvSchemeShard::TEvSyncTenantSchemeShard> MakeSyncMsg() const;
    void SendSync(const TActorContext& ctx);
    void AtPipeError(const TActorContext& ctx);
    bool HasPipeTo(TTabletId tabletId, TActorId clientId);
};


class TSubDomainsLinks {
public:
    struct TLink {
        TPathId DomainKey = InvalidPathId;
        ui64 Generation = 0;
        TActorId ActorId;

        ui64 EffectiveACLVersion = 0;
        ui64 SubdomainVersion = 0;
        ui64 UserAttributesVersion = 0;
        TTabletId TenantHive = InvalidTabletId;
        TTabletId TenantSysViewProcessor = InvalidTabletId;
        TTabletId TenantStatisticsAggregator = InvalidTabletId;
        TTabletId TenantGraphShard = InvalidTabletId;
        TString TenantRootACL;

        TLink() = default;
        TLink& operator = (TLink&&) = default;
        TLink(const NKikimrScheme::TEvSyncTenantSchemeShard& record, const TActorId& actorId);
        void Out(IOutputStream& stream) const;
    };

private:
    TSchemeShard* Self;
    THashMap<TPathId, TLink> ActiveLink;

public:
    TSubDomainsLinks(TSchemeShard* self)
        : Self(self)
    {}

    bool IsActive(TPathId pathId) const {
        return ActiveLink.contains(pathId);
    }

    const TLink& GetLink(TPathId pathId) const {
        return ActiveLink.at(pathId);
    }

    bool Sync(TEvSchemeShard::TEvSyncTenantSchemeShard::TPtr& ev, const TActorContext& ctx);
};

}}


template<>
inline void Out<NKikimr::NSchemeShard::TSubDomainsLinks::TLink>(IOutputStream& o, const NKikimr::NSchemeShard::TSubDomainsLinks::TLink& x) {
    return x.Out(o);
}
