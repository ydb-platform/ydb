#include "hive_impl.h"
#include "hive_log.h"

#include <ydb/core/base/tablet.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::HIVE

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
    const TActorId Source;
    const TString Description;
    std::unique_ptr<IMoveDataCallback> Callback;
    std::vector<TPipeClient> PipeClients;
    i64 MoveDataInFlight = 0;
    // Sends, not iterator position: NextTablet is advanced before SendMoveData in one caller and after in the other.
    size_t SentCount = 0;
    ui64 TabletsDone = 0;
    bool FastFail;
    THive* Hive;

    TMoveDataActor(std::vector<TTabletId> tablets, const std::vector<TStorageGroupId>& groups, const TActorId& source, ui64 maxInFlight, TString description, std::unique_ptr<IMoveDataCallback> callback, bool fastFail, THive* hive)
        : Tablets(std::move(tablets))
        , NextTablet(Tablets.begin())
        , Groups(groups)
        , Source(source)
        , Description(std::move(description))
        , Callback(std::move(callback))
        , PipeClients(std::max<ui64>(maxInFlight, 1))
        , FastFail(fastFail)
        , Hive(hive)
    {
    }

    void PassAway() override {
        Hive->OnShrinkMoveDataFinished();
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
        return TStringBuilder() << "MoveData(" << Description << "): " << TabletsDone << "/" << Tablets.size();
    }

    void ReplyAndPassAway(bool success) {
        if (Source && Callback) {
            Send(Source, Callback->MakeEvent(success, TabletsDone));
        }
        return PassAway();
    }

    size_t Queued() const {
        return std::distance(NextTablet, Tablets.cend());
    }

    void SendMoveData(size_t index, TTabletId tablet) {
        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = {.RetryLimitCount = 13};
        pipeConfig.CheckAliveness = true;
        PipeClients[index] = {Register(NTabletPipe::CreateClient(SelfId(), tablet, pipeConfig)), tablet};
        NTabletPipe::SendData(SelfId(), PipeClients[index].Client, new TEvTablet::TEvMoveData(Groups));
        ++MoveDataInFlight;
        ++SentCount;
        Hive->OnShrinkMoveDataSent(MoveDataInFlight, Queued());
        YDB_LOG_NOTICE("ShrinkPool: MoveData sent",
            {"description", Description},
            {"tablet", tablet},
            {"sent", SentCount},
            {"total", Tablets.size()},
            {"queued", Queued()},
            {"inFlight", MoveDataInFlight});
    }

    void CheckCompletion() {
        if (MoveDataInFlight == 0 && NextTablet == Tablets.end()) {
            return ReplyAndPassAway(true);
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
                PipeClients[i].Tablet = 0;
                --MoveDataInFlight;
                Hive->OnShrinkMoveDataAnswered(MoveDataInFlight, Queued());
                YDB_LOG_NOTICE("ShrinkPool: MoveData answered",
                    {"description", Description},
                    {"tablet", tablet},
                    {"status", (ui32)ev->Get()->Record.GetStatus()},
                    {"queued", Queued()},
                    {"inFlight", MoveDataInFlight});
                if (ev->Get()->Record.GetStatus() == NKikimrTabletBase::TEvMoveDataResponse::Success) {
                    ++TabletsDone;
                    Hive->Execute(Hive->CreateRestartTablet(ToFullTabletId(tablet)));
                } else if (FastFail) {
                    return ReplyAndPassAway(false);
                }
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
                if (FastFail) {
                    return ReplyAndPassAway(false);
                } else {
                    for (size_t i = 0; i < PipeClients.size(); ++i) {
                        if (PipeClients[i].Tablet == ev->Get()->TabletId) {
                            NTabletPipe::CloseClient(SelfId(), PipeClients[i].Client);
                            PipeClients[i].Tablet = 0;
                            --MoveDataInFlight;
                            if (NextTablet != Tablets.end()) {
                                SendMoveData(i, *(NextTablet++));
                            }
                            break;
                        }
                    }
                }
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
                Hive->OnShrinkMoveDataRetried();
                YDB_LOG_NOTICE("ShrinkPool: MoveData retried",
                    {"description", Description},
                    {"tablet", tablet});
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

void THive::StartMoveDataActor(std::vector<TTabletId> tablets, const std::vector<TStorageGroupId>& groups, const TActorId& source, ui32 maxInFlight, TString description, std::unique_ptr<IMoveDataCallback> callback, bool fastFail) {
    auto* actor = new TMoveDataActor(std::move(tablets), groups, source, maxInFlight, std::move(description), std::move(callback), fastFail, this);
    SubActors.emplace_back(actor);
    RegisterWithSameMailbox(actor);
}

} // NKikimr::NHive
