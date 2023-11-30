#include "logging.h"
#include "private_events.h"
#include "tenant_resolver.h"

#include <ydb/core/base/path.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NReplication::NController {

class TTenantResolver: public TActorBootstrapped<TTenantResolver> {
    void Resolve(const TPathId& pathId) {
        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();

        auto& entry = request->ResultSet.emplace_back();
        entry.TableId = pathId;
        entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByTableId;
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
        entry.RedirectRequired = false;

        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto* response = ev->Get()->Request.Get();

        Y_ABORT_UNLESS(response->ResultSet.size() == 1);
        const auto& entry = response->ResultSet.front();

        LOG_T("Handle " << ev->Get()->ToString()
            << ": entry# " << entry.ToString());

        switch (entry.Status) {
        case NSchemeCache::TSchemeCacheNavigate::EStatus::Ok:
            break;
        default:
            LOG_W("Unexpected status"
                << ": entry# " << entry.ToString());
            return Reply(false);
        }

        if (!DomainKey) {
            if (!entry.DomainInfo) {
                LOG_E("Empty domain info"
                    << ": entry# " << entry.ToString());
                return Reply(false);
            }

            DomainKey = entry.DomainInfo->DomainKey;
            Resolve(DomainKey);
        } else {
            return Reply(CanonizePath(entry.Path));
        }
    }

    template <typename... Args>
    void Reply(Args&&... args) {
        Send(Parent, new TEvPrivate::TEvResolveTenantResult(ReplicationId, std::forward<Args>(args)...));
        PassAway();
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::REPLICATION_CONTROLLER_TENANT_RESOLVER;
    }

    explicit TTenantResolver(const TActorId& parent, ui64 rid, const TPathId& pathId)
        : Parent(parent)
        , ReplicationId(rid)
        , PathId(pathId)
        , LogPrefix("TenantResolver", ReplicationId)
    {
    }

    void Bootstrap() {
        Resolve(PathId);
        Become(&TThis::StateWork);
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    const TActorId Parent;
    const ui64 ReplicationId;
    const TPathId PathId;
    const TActorLogPrefix LogPrefix;

    TPathId DomainKey;

}; // TTenantResolver

IActor* CreateTenantResolver(const TActorId& parent, ui64 rid, const TPathId& pathId) {
    return new TTenantResolver(parent, rid, pathId);
}

}
