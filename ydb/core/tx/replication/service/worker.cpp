#include "logging.h"
#include "service.h"
#include "worker.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/tx/replication/ydb_proxy/topic_message.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/services/services.pb.h>

#include <util/generic/maybe.h>
#include <util/string/builder.h>
#include <util/string/join.h>

namespace NKikimr::NReplication::NService {

TEvWorker::TEvPoll::TEvPoll(bool skipCommit)
    : SkipCommit(skipCommit)
{
}

TString TEvWorker::TEvPoll::ToString() const {
    return TStringBuilder() << ToStringHeader() << " {"
        << " SkipCommit: " << SkipCommit
    << " }";
}

TEvWorker::TEvData::TEvData(ui32 partitionId, const TString& source, const TVector<TTopicMessage>& records)
    : PartitionId(partitionId)
    , Source(source)
    , Records(records)
{
}

TEvWorker::TEvData::TEvData(ui32 partitionId, const TString& source, TVector<TTopicMessage>&& records)
    : PartitionId(partitionId)
    , Source(source)
    , Records(std::move(records))
{
}

TString TEvWorker::TEvData::ToString() const {
    return TStringBuilder() << ToStringHeader() << " {"
        << " Source: " << Source
        << " Records [" << JoinSeq(",", Records) << "]"
    << " }";
}

TEvWorker::TEvGone::TEvGone(EStatus status, const TString& errorDescription)
    : Status(status)
    , ErrorDescription(errorDescription)
{
}

TString TEvWorker::TEvGone::ToString() const {
    return TStringBuilder() << ToStringHeader() << " {"
        << " Status: " << Status
        << " ErrorDescription: " << ErrorDescription
    << " }";
}

TEvWorker::TEvStatus::TEvStatus(TDuration lag)
    : Lag(lag)
{
}

TString TEvWorker::TEvStatus::ToString() const {
    return TStringBuilder() << ToStringHeader() << " {"
        << " Lag: " << Lag
    << " }";
}

TEvWorker::TEvDataEnd::TEvDataEnd(ui64 partitionId, TVector<ui64>&& adjacentPartitionsIds, TVector<ui64>&& childPartitionsIds)
    : PartitionId(partitionId)
    , AdjacentPartitionsIds(std::move(adjacentPartitionsIds))
    , ChildPartitionsIds(std::move(childPartitionsIds))
{
}

TString TEvWorker::TEvDataEnd::ToString() const {
    return TStringBuilder() << ToStringHeader() << " {"
        << " PartitionId: " << PartitionId
        << " AdjacentPartitionsIds: " << JoinSeq(", ", AdjacentPartitionsIds)
        << " ChildPartitionsIds: " << JoinSeq(", ", ChildPartitionsIds)
    << " }";
}

class TWorker: public TActorBootstrapped<TWorker> {
    class TActorInfo {
        std::function<IActor*(void)> CreateFn;
        TActorId ActorId;
        bool InitDone;
        ui32 CreateAttempt;

    public:
        explicit TActorInfo(std::function<IActor*(void)>&& createFn)
            : CreateFn(std::move(createFn))
            , InitDone(false)
            , CreateAttempt(0)
        {
        }

        operator TActorId() const {
            return ActorId;
        }

        explicit operator bool() const {
            return InitDone;
        }

        void Register(IActorOps* ops) {
            ActorId = ops->RegisterWithSameMailbox(CreateFn());
            ops->Send(ActorId, new TEvWorker::TEvHandshake());
            InitDone = false;
            ++CreateAttempt;
        }

        void Registered() {
            InitDone = true;
            CreateAttempt = 0;
        }

        ui32 GetCreateAttempt() const {
            return CreateAttempt;
        }
    };

    TStringBuf GetLogPrefix() const {
        if (!LogPrefix) {
            LogPrefix = TStringBuilder()
                << "[Worker]"
                << SelfId() << " ";
        }

        return LogPrefix.GetRef();
    }

    void Handle(TEvWorker::TEvHandshake::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());

        if (ev->Sender == Reader) {
            LOG_I("Handshake with reader"
                << ": sender# " << ev->Sender);

            Reader.Registered();
            if (!InFlightData) {
                Send(Reader, new TEvWorker::TEvPoll());
            }
        } else if (ev->Sender == Writer) {
            LOG_I("Handshake with writer"
                << ": sender# " << ev->Sender);

            Writer.Registered();
            if (InFlightData) {
                Send(Writer, new TEvWorker::TEvData(InFlightData->PartitionId, InFlightData->Source, InFlightData->Records));
            }
        } else {
            LOG_W("Handshake from unknown actor"
                << ": sender# " << ev->Sender);
            return;
        }
    }

    void Handle(TEvWorker::TEvPoll::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());

        if (ev->Sender != Writer) {
            LOG_W("Poll from unknown actor"
                << ": sender# " << ev->Sender);
            return;
        }

        if (InFlightData) {
            const auto& records = InFlightData->Records;
            auto it = MinElementBy(records, [](const auto& record) {
                return record.GetCreateTime();
            });

            if (it != records.end()) {
                Lag = TlsActivationContext->Now() - it->GetCreateTime();
            }
        }

        InFlightData.Reset();
        if (Reader) {
            Send(ev->Forward(Reader));
        }
    }

    void Handle(TEvWorker::TEvData::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());

        if (ev->Sender != Reader) {
            LOG_W("Data from unknown actor"
                << ": sender# " << ev->Sender);
            return;
        }

        Y_ABORT_UNLESS(!InFlightData);
        InFlightData = MakeHolder<TEvWorker::TEvData>(ev->Get()->PartitionId, ev->Get()->Source, ev->Get()->Records);

        if (Writer) {
            Send(ev->Forward(Writer));
        }
    }

    void Handle(TEvWorker::TEvGone::TPtr& ev) {
        if (ev->Sender == Reader) {
            LOG_I("Reader has gone"
                << ": sender# " << ev->Sender);
            MaybeRecreateActor(ev, Reader);
        } else if (ev->Sender == Writer) {
            LOG_I("Writer has gone"
                << ": sender# " << ev->Sender);
            MaybeRecreateActor(ev, Writer);
        } else {
            LOG_W("Unknown actor has gone"
                << ": sender# " << ev->Sender);
        }
    }

    void MaybeRecreateActor(TEvWorker::TEvGone::TPtr& ev, TActorInfo& info) {
        switch (ev->Get()->Status) {
        case TEvWorker::TEvGone::UNAVAILABLE:
            if (info.GetCreateAttempt() < MaxAttempts) {
                return info.Register(this);
            }
            [[fallthrough]];
        default:
            return Leave(ev);
        }
    }

    void Leave(TEvWorker::TEvGone::TPtr& ev) {
        LOG_I("Leave"
            << ": status# " << ev->Get()->Status
            << ", error# " << ev->Get()->ErrorDescription);

        ev->Sender = SelfId();
        Send(ev->Forward(Parent));

        PassAway();
    }

    void Handle(TEvService::TEvTxIdResult::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());
        Send(ev->Forward(Writer));
    }

    template <typename TEventPtr>
    void Forward(TEventPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());

        ev->Sender = SelfId();
        Send(ev->Forward(Parent));
    }

    void ScheduleLagReport() {
        const auto random = TDuration::MicroSeconds(TAppData::RandomProvider->GenRand64() % LagReportInterval.MicroSeconds());
        Schedule(LagReportInterval + random, new TEvents::TEvWakeup());
    }

    void ReportLag() {
        ScheduleLagReport();

        if (!Reader || !Writer) {
            return;
        }

        Send(Parent, new TEvWorker::TEvStatus(Lag));
        Lag = TDuration::Zero();
    }

    void PassAway() override {
        for (auto* actor : {&Reader, &Writer}) {
            Send(*actor, new TEvents::TEvPoison());
        }

        TActorBootstrapped::PassAway();
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::REPLICATION_WORKER;
    }

    explicit TWorker(
            const TActorId& parent,
            std::function<IActor*(void)>&& createReaderFn,
            std::function<IActor*(void)>&& createWriterFn)
        : Parent(parent)
        , Reader(std::move(createReaderFn))
        , Writer(std::move(createWriterFn))
        , Lag(TDuration::Zero())
    {
    }

    void Bootstrap() {
        for (auto* actor : {&Reader, &Writer}) {
            actor->Register(this);
        }

        Become(&TThis::StateWork);
        ScheduleLagReport();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvWorker::TEvHandshake, Handle);
            hFunc(TEvWorker::TEvPoll, Handle);
            hFunc(TEvWorker::TEvData, Handle);
            hFunc(TEvWorker::TEvDataEnd, Forward);
            hFunc(TEvWorker::TEvGone, Handle);
            hFunc(TEvService::TEvGetTxId, Forward);
            hFunc(TEvService::TEvTxIdResult, Handle);
            hFunc(TEvService::TEvHeartbeat, Forward);
            sFunc(TEvents::TEvWakeup, ReportLag);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    static constexpr ui32 MaxAttempts = 3;
    static constexpr TDuration LagReportInterval = TDuration::Seconds(7);

    const TActorId Parent;
    mutable TMaybe<TString> LogPrefix;
    TActorInfo Reader;
    TActorInfo Writer;
    THolder<TEvWorker::TEvData> InFlightData;
    TDuration Lag;
};

IActor* CreateWorker(
        const TActorId& parent,
        std::function<IActor*(void)>&& createReaderFn,
        std::function<IActor*(void)>&& createWriterFn)
{
    return new TWorker(parent, std::move(createReaderFn), std::move(createWriterFn));
}

}
