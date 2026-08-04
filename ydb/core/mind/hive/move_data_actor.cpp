#include "hive_impl.h"

#include <ydb/core/base/tablet.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NHive {

class TMoveDataActor
    : public TActorBootstrapped<TMoveDataActor>
    , public ISubActor
{
public:
    struct TPipeClient {
        TActorId Client;
        TTabletId Tablet;
    };

    std::vector<TTabletId> Tablets;
    std::vector<TTabletId>::const_iterator NextTablet;
    std::vector<TStorageGroupId> Groups;
    TString PoolName;
    std::vector<TPipeClient> PipeClients;
    i64 MoveDataInFlight = 0;
    THive* Hive;

    TMoveDataActor(std::vector<TTabletId> tablets, const std::vector<TStorageGroupId>& groups, const TString& poolName, ui64 maxInFlight, THive* hive)
        : Tablets(std::move(tablets))
        , NextTablet(Tablets.begin())
        , Groups(groups)
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

    TString GetDescription() const override {
        return TStringBuilder() << "MoveData(" << PoolName << ")";
    }

    void SendMoveData(size_t index, TTabletId tablet) {
        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = {.RetryLimitCount = 13};
        pipeConfig.CheckAliveness = true;
        PipeClients[index] = {Register(NTabletPipe::CreateClient(SelfId(), tablet, pipeConfig)), tablet};
        NTabletPipe::SendData(SelfId(), PipeClients[index].Client, new TEvTablet::TEvMoveData(Groups));
        ++MoveDataInFlight;
    }

    void CheckCompletion() {
        if (MoveDataInFlight == 0 && NextTablet == Tablets.end()) {
            Send(Hive->SelfId(), new TEvPrivate::TEvMoveDataComplete(PoolName, true));
            return PassAway();
        }
    }

    void Bootstrap() {
        Become(&TThis::StateWork);
        for (size_t i = 0; i < PipeClients.size() && NextTablet != Tablets.end(); ++i, ++NextTablet) {
            SendMoveData(i, *NextTablet);
        }
        return CheckCompletion();
    }

    void Handle(TEvTablet::TEvMoveDataResponse::TPtr& ev) {
        auto tablet = ev->Get()->Record.GetTabletId();
        for (size_t i = 0; i < PipeClients.size(); ++i) {
            if (PipeClients[i].Tablet == tablet) {
                NTabletPipe::CloseClient(SelfId(), PipeClients[i].Client);
                --MoveDataInFlight;
                Hive->Execute(Hive->CreateRestartTablet(ToFullTabletId(tablet)));
                if (NextTablet != Tablets.end()) {
                    SendMoveData(i, *(NextTablet++));
                    break;
                }
            }
        }
        return CheckCompletion();
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            if (ev->Get()->Dead) {
                Send(Hive->SelfId(), new TEvPrivate::TEvMoveDataComplete(PoolName, false));
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
                --MoveDataInFlight;
                SendMoveData(i, tablet);
                break;
            }
        }
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            cFunc(TEvents::TSystem::PoisonPill, PassAway);
            hFunc(TEvTablet::TEvMoveDataResponse, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
        }
    }
};

void THive::StartMoveDataActor(std::vector<TTabletId> tablets, const std::vector<TStorageGroupId>& groups, const TString& poolName) {
    auto* actor = new TMoveDataActor(std::move(tablets), groups, poolName, 1, this);
    SubActors.emplace_back(actor);
    RegisterWithSameMailbox(actor);
}

} // NKikimr::NHive
