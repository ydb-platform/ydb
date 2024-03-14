#include "service.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/domain.h>
#include <ydb/core/base/statestorage.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NReplication {

namespace NService {

class TSessionInfo {
public:
    explicit TSessionInfo(const TActorId& actorId)
        : ActorId(actorId)
        , Generation(0)
    {
    }

    operator TActorId() const {
        return ActorId;
    }

    ui64 GetGeneration() const {
        return Generation;
    }

    void Update(const TActorId& actorId, ui64 generation) {
        Y_ABORT_UNLESS(Generation <= generation);
        ActorId = actorId;
        Generation = generation;
    }

private:
    TActorId ActorId;
    ui64 Generation;

};

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

    void Handle(TEvService::TEvHandshake::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        auto it = Sessions.find(record.GetControllerId());
        if (it == Sessions.end()) {
            it = Sessions.emplace(record.GetControllerId(), ev->Sender).first;
        }

        auto& session = it->second;

        if (session.GetGeneration() > record.GetGeneration()) {
            // ignore stale controller
            return;
        }

        session.Update(ev->Sender, record.GetGeneration());
        Send(session, new TEvService::TEvStatus()); // TODO
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
            hFunc(TEvService::TEvHandshake, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    TActorId BoardPublisher;
    THashMap<ui64, TSessionInfo> Sessions;

}; // TReplicationService

} // NService

IActor* CreateReplicationService() {
    return new NService::TReplicationService();
}

}
