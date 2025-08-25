#include "schemeshard_domain_links.h"

#include "schemeshard_impl.h"

namespace NKikimr {
namespace NSchemeShard {

TParentDomainLink::TParentDomainLink(NKikimr::NSchemeShard::TSchemeShard *self)
    : Self(self)
{
    PipeClientConfig.RetryPolicy = {
        .MinRetryTime = TDuration::MilliSeconds(10),
        .MaxRetryTime = TDuration::Minutes(5),
    };
}

THolder<TEvSchemeShard::TEvSyncTenantSchemeShard> TParentDomainLink::MakeSyncMsg() const {
    Y_ABORT_UNLESS(Self->SubDomains.contains(Self->RootPathId()));
    auto& rootPath = Self->PathsById.at(Self->RootPathId());

    Y_ABORT_UNLESS(Self->PathsById.contains(Self->RootPathId()));
    auto& rootSubdomain = Self->SubDomains.at(Self->RootPathId());

    TEvSchemeShard::TEvSyncTenantSchemeShard* ptr = new TEvSchemeShard::TEvSyncTenantSchemeShard({
        .DomainKey = Self->ParentDomainId,
        .TabletId = Self->TabletID(),
        .Generation = Self->Generation(),
        .EffectiveACLVersion = Self->ParentDomainEffectiveACLVersion,
        .SubdomainVersion = rootSubdomain->GetVersion(),
        .UserAttrsVersion = rootPath->UserAttrs->AlterVersion,
        .TenantHive = ui64(rootSubdomain->GetTenantHiveID()),
        .TenantSysViewProcessor = ui64(rootSubdomain->GetTenantSysViewProcessorID()),
        .TenantStatisticsAggregator = ui64(rootSubdomain->GetTenantStatisticsAggregatorID()),
        .TenantGraphShard = ui64(rootSubdomain->GetTenantGraphShardID()),
        .RootACL = rootPath->ACL
    });
    return THolder<TEvSchemeShard::TEvSyncTenantSchemeShard>(ptr);
}

void TParentDomainLink::SendSync(const TActorContext &ctx) {
    if (Self->IsDomainSchemeShard) {
        return;
    }

    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
               "Send TEvSyncTenantSchemeShard"
               << ", to parent: " << Self->ParentDomainId
               << ", from: " << Self->TabletID());

    if (!Pipe) {
        Pipe = ctx.Register(NTabletPipe::CreateClient(ctx.SelfID, Self->ParentDomainId.OwnerId, PipeClientConfig));
    }

    auto ev = MakeSyncMsg();

    NTabletPipe::SendData(ctx, Pipe, ev.Release(), 0);
}

void TParentDomainLink::AtPipeError(const TActorContext &ctx) {
    if (Pipe) {
        NTabletPipe::CloseClient(ctx, Pipe);
        Pipe = TActorId();
    }

    SendSync(ctx);
}

bool TParentDomainLink::HasPipeTo(TTabletId tabletId, TActorId clientId) {
    return TTabletId(Self->ParentDomainId.OwnerId) == tabletId && Pipe == clientId;
}

void TParentDomainLink::Shutdown(const NActors::TActorContext &ctx) {
    if (Pipe) {
        NTabletPipe::CloseClient(ctx, Pipe);
        Pipe = TActorId();
    }
}

bool TSubDomainsLinks::Sync(TEvSchemeShard::TEvSyncTenantSchemeShard::TPtr &ev, const TActorContext &ctx) {
    Y_ABORT_UNLESS(Self->IsDomainSchemeShard);

    const auto& record = ev->Get()->Record;
    const TActorId actorId = ev->Sender;

    const TPathId pathId = Self->MakeLocalId(record.GetDomainPathId());
    const ui64 generation = record.GetGeneration();

    Y_ABORT_UNLESS(record.GetDomainSchemeShard() == Self->TabletID());

    if (ActiveLink.contains(pathId)) {
        TLink& link = ActiveLink.at(pathId);

        if (link.Generation > generation) {
            LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                       "Ignore TEvSyncTenantSchemeShard with obsolete generation"
                       << ", msg: " << record.ShortDebugString()
                       << ", at schemeshard: " << Self->TabletID());
            return false;
        }
    }

    ActiveLink[pathId] = TLink(record, actorId);
    return true;
}

void TSubDomainsLinks::TLink::Out(IOutputStream& stream) const {
    stream << "TSubDomainsLinks::TLink {"
           << " DomainKey: " << DomainKey
           << ", Generation: " << Generation
           << ", ActorId:" << ActorId
           << ", EffectiveACLVersion: " << EffectiveACLVersion
           << ", SubdomainVersion: " << SubdomainVersion
           << ", UserAttributesVersion: " << UserAttributesVersion
           << ", TenantHive: " << TenantHive
           << ", TenantSysViewProcessor: " << TenantSysViewProcessor
           << ", TenantStatisticsAggregator: " << TenantStatisticsAggregator
           << ", TenantGraphShard: " << TenantGraphShard
           << ", TenantRootACL: " << TenantRootACL
           << "}";
}

TSubDomainsLinks::TLink::TLink(const NKikimrScheme::TEvSyncTenantSchemeShard &record, const TActorId &actorId)
    : DomainKey(record.GetDomainSchemeShard(), record.GetDomainPathId())
    , Generation(record.GetGeneration())
    , ActorId(actorId)
    , EffectiveACLVersion(record.GetEffectiveACLVersion())
    , SubdomainVersion(record.GetSubdomainVersion())
    , UserAttributesVersion(record.GetUserAttributesVersion())
    , TenantHive(record.HasTenantHive() ? TTabletId(record.GetTenantHive()) : InvalidTabletId)
    , TenantSysViewProcessor(record.HasTenantSysViewProcessor() ?
        TTabletId(record.GetTenantSysViewProcessor()) : InvalidTabletId)
    , TenantStatisticsAggregator(record.HasTenantStatisticsAggregator() ?
        TTabletId(record.GetTenantStatisticsAggregator()) : InvalidTabletId)
    , TenantGraphShard(record.HasTenantGraphShard() ?
        TTabletId(record.GetTenantGraphShard()) : InvalidTabletId)
    , TenantRootACL(record.GetTenantRootACL())
{}

}}
