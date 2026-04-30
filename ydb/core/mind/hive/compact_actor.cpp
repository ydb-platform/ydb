#include "hive_impl.h"

#include <ydb/core/base/tablet.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NHive {

class TCompactActor
    : public TActorBootstrapped<TCompactActor>
    , public ISubActor
{
public:
    struct TPipeClient {
        TActorId Client;
        TTabletId Tablet;
    };

    std::vector<TTabletId> Tablets;
    std::vector<TTabletId>::const_iterator NextTablet;
    TString PoolName;
    std::vector<TPipeClient> PipeClients;
    i64 CompactsInFlight = 0;
    THive* Hive;

    TCompactActor(std::vector<TTabletId> tablets, const TString& poolName, ui64 maxInFlight, THive* hive)
        : Tablets(std::move(tablets))
        , NextTablet(Tablets.begin())
        , PoolName(poolName)
        , PipeClients(maxInFlight)
        , Hive(hive)
    {
    }

    void PassAway() override {
        Hive->RemoveSubActor(this);
        return IActor::PassAway();
    }

    void Cleanup() override {
        PassAway();
    }

    TSubActorId GetId() const override {
        return SelfId().LocalId();
    }

    void SendCompact(size_t index, TTabletId tablet) {
        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = {.RetryLimitCount = 13};
        pipeConfig.CheckAliveness = true;
        PipeClients[index] = {Register(NTabletPipe::CreateClient(SelfId(), tablet, pipeConfig)), tablet};
        NTabletPipe::SendData(SelfId(), PipeClients[index].Client, new TEvTablet::TEvCompactTables());
        ++CompactsInFlight;
    }

    void CheckCompletion() {
        if (CompactsInFlight == 0 && NextTablet == Tablets.end()) {
            Send(Hive->SelfId(), new TEvPrivate::TEvCompactComplete(PoolName, true));
            return PassAway();
        }
    }

    void Bootstrap() {
        Become(&TThis::StateWork);
        for (size_t i = 0; i < PipeClients.size() && NextTablet != Tablets.end(); ++i, ++NextTablet) {
            SendCompact(i, *NextTablet);
        }
        return CheckCompletion();
    }

    void Handle(TEvTablet::TEvCompactTablesResponse::TPtr& ev) {
        auto tablet = ev->Get()->Record.GetTabletId();
        for (size_t i = 0; i < PipeClients.size(); ++i) {
            if (PipeClients[i].Tablet == tablet) {
                NTabletPipe::CloseClient(SelfId(), PipeClients[i].Client);
                if (NextTablet != Tablets.end()) {
                    SendCompact(i, *(NextTablet++));
                }
            }
        }
        --CompactsInFlight;
        return CheckCompletion();
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            if (ev->Get()->Dead) {
                Send(Hive->SelfId(), new TEvPrivate::TEvCompactComplete(PoolName, false));
                return PassAway();
            } else {
                Retry(ev->Get()->TabletId);
            }
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
        Retry(ev->Get()->TabletId);
    }

    void Retry(TTabletId tablet) {
        for (size_t i = 0; i < PipeClients.size(); ++i) {
            if (PipeClients[i].Tablet == tablet) {
                NTabletPipe::CloseClient(SelfId(), PipeClients[i].Client);
                SendCompact(i, tablet);
            }
        }
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            cFunc(TEvents::TSystem::PoisonPill, PassAway);
            hFunc(TEvTablet::TEvCompactTablesResponse, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
        }
    }
};

void THive::StartCompactActor(std::vector<TTabletId> tablets, const TString& poolName) {
    auto* actor = new TCompactActor(std::move(tablets), poolName, 1, this);
    SubActors.emplace_back(actor);
    RegisterWithSameMailbox(actor);
}

} // NKikimr::NHive
