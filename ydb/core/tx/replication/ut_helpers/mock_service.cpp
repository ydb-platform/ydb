#include <ydb/core/base/statestorage.h>
#include <ydb/core/tx/replication/common/worker_id.h>
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

    void Handle(TEvService::TEvHandshake::TPtr& ev) {
        // Mirror the real service handshake: the controller does not boot
        // registered workers until the service session reports itself ready.
        // Keeping this in the shared mock makes controller tests exercise the
        // normal worker-registration path rather than hand-assembling session
        // state.
        auto status = MakeHolder<TEvService::TEvStatus>();
        for (const auto& worker : Workers) {
            worker.Serialize(*status->Record.AddWorkers());
        }
        Send(ev->Sender, status.Release());
        Forward(ev);
    }

    void Handle(TEvService::TEvRunWorker::TPtr& ev) {
        Workers.insert(TWorkerId::Parse(ev->Get()->Record.GetWorker()));
        Forward(ev);
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
            hFunc(TEvService::TEvHandshake, Handle);
            hFunc(TEvService::TEvRunWorker, Handle);
            hFunc(TEvService::TEvStopWorker, Forward);
            hFunc(TEvService::TEvSchemaChangeResult, Forward);
            hFunc(TEvService::TEvTxIdResult, Forward);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    const TActorId Edge;
    TActorId BoardPublisher;
    THashSet<TWorkerId> Workers;
};

IActor* CreateReplicationMockService(const TActorId& edge) {
    return new TMockService(edge);
}

}
