#include <ydb/core/base/statestorage.h>
#include <ydb/core/tx/replication/service/service.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NReplication::NTestHelpers {

class TMockService: public TActorBootstrapped<TMockService> {
    template <typename TEventPtr>
    void Forward(TEventPtr& ev) {
        Send(ev->Forward(Edge));
    }

    void PassAway() override {
        Send(BoardPublisher, new TEvents::TEvPoison());
        TActorBootstrapped<TMockService>::PassAway();
    }

public:
    explicit TMockService(const TActorId& edge)
        : Edge(edge)
    {}

    void Bootstrap() {
        Become(&TThis::StateWork);
        BoardPublisher = Register(CreateBoardPublishActor(NService::MakeDiscoveryPath("/Root"), TString(), SelfId(), 0, true));
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvService::TEvHandshake, Forward);
            hFunc(TEvService::TEvRunWorker, Forward);
            hFunc(TEvService::TEvStopWorker, Forward);
            hFunc(TEvService::TEvTxIdResult, Forward);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    const TActorId Edge;
    TActorId BoardPublisher;
};

IActor* CreateReplicationMockService(const TActorId& edge) {
    return new TMockService(edge);
}

}
