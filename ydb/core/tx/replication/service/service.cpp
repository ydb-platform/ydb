#include "service.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/domain.h>
#include <ydb/core/base/statestorage.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NReplication {

namespace NService {

class TReplicationService: public TActorBootstrapped<TReplicationService> {
    void RunBoardPublisher() {
        const auto& tenant = AppData()->TenantName;

        auto* domainInfo = AppData()->DomainsInfo->GetDomainByName(ExtractDomain(tenant));
        if (!domainInfo) {
            return PassAway();
        }

        const auto boardPath = MakeDiscoveryPath(tenant);
        BoardPublisher = Register(CreateBoardPublishActor(boardPath, TString(), SelfId(), 0, true));
    }

    void PassAway() override {
        if (auto actorId = std::exchange(BoardPublisher, {})) {
            Send(actorId, new TEvents::TEvPoison());
        }

        TActorBootstrapped<TReplicationService>::PassAway();
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::REPLICATION_SERVICE;
    }

    void Bootstrap() {
        Become(&TThis::StateWork);
        RunBoardPublisher();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    TActorId BoardPublisher;

}; // TReplicationService

} // NService

IActor* CreateReplicationService() {
    return new NService::TReplicationService();
}

}
