#include "logging.h"
#include "worker.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/services/services.pb.h>

#include <util/generic/maybe.h>
#include <util/string/builder.h>
#include <util/string/join.h>

namespace NKikimr::NReplication::NService {

TEvWorker::TEvData::TRecord::TRecord(ui64 offset, const TString& data)
    : Offset(offset)
    , Data(data)
{
}

TEvWorker::TEvData::TRecord::TRecord(ui64 offset, TString&& data)
    : Offset(offset)
    , Data(std::move(data))
{
}

TEvWorker::TEvData::TEvData(TVector<TRecord>&& records)
    : Records(std::move(records))
{
}

void TEvWorker::TEvData::TRecord::Out(IOutputStream& out) const {
    out << "{"
        << " Offset: " << Offset
        << " Data: " << Data
    << " }";
}

TString TEvWorker::TEvData::ToString() const {
    return TStringBuilder() << ToStringHeader() << " {"
        << " Records [" << JoinSeq(",", Records) << "]"
    << " }";
}

TEvWorker::TEvGone::TEvGone(EStatus status)
    : Status(status)
{
}

TString TEvWorker::TEvGone::ToString() const {
    return TStringBuilder() << ToStringHeader() << " {"
        << " Status: " << Status
    << " }";
}

class TWorker: public TActorBootstrapped<TWorker> {
    struct TActorInfo {
        THolder<IActor> Actor;
        TActorId ActorId;
        bool InitDone;

        explicit TActorInfo(THolder<IActor>&& actor)
            : Actor(std::move(actor))
            , InitDone(false)
        {
        }

        operator TActorId() const {
            return ActorId;
        }

        explicit operator bool() const {
            return InitDone;
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

    TActorId RegisterActor(TActorInfo& info) {
        Y_ABORT_UNLESS(info.Actor);
        info.ActorId = RegisterWithSameMailbox(info.Actor.Release());
        return info.ActorId;
    }

    void InitActor(TActorInfo& info) {
        Y_ABORT_UNLESS(info.ActorId);
        Send(info.ActorId, new TEvWorker::TEvHandshake());
        info.InitDone = false;
    }

    void Handle(TEvWorker::TEvHandshake::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());

        if (ev->Sender == Reader) {
            LOG_I("Handshake with reader"
                << ": sender# " << ev->Sender);
            Reader.InitDone = true;
        } else if (ev->Sender == Writer) {
            LOG_I("Handshake with writer"
                << ": sender# " << ev->Sender);
            Writer.InitDone = true;
        } else {
            LOG_W("Handshake from unknown actor"
                << ": sender# " << ev->Sender);
            return;
        }

        if (Reader && Writer) {
            LOG_N("Start working");
            Send(Reader, new TEvWorker::TEvPoll());
        }
    }

    void Handle(TEvWorker::TEvPoll::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());

        if (ev->Sender != Writer) {
            LOG_W("Poll from unknown actor"
                << ": sender# " << ev->Sender);
            return;
        }

        Send(ev->Forward(Reader));
    }

    void Handle(TEvWorker::TEvData::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());

        if (ev->Sender != Reader) {
            LOG_W("Data from unknown actor"
                << ": sender# " << ev->Sender);
            return;
        }

        Send(ev->Forward(Writer));
    }

    void Handle(TEvWorker::TEvGone::TPtr& ev) {
        // TODO: handle status
        if (ev->Sender == Reader) {
            LOG_I("Reader has gone"
                << ": sender# " << ev->Sender);
        } else if (ev->Sender == Writer) {
            LOG_I("Writer has gone"
                << ": sender# " << ev->Sender);
        } else {
            LOG_W("Unknown actor has gone"
                << ": sender# " << ev->Sender);
        }
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

    explicit TWorker(THolder<IActor>&& reader, THolder<IActor>&& writer)
        : Reader(std::move(reader))
        , Writer(std::move(writer))
    {
    }

    void Bootstrap() {
        for (auto* actor : {&Reader, &Writer}) {
            RegisterActor(*actor);
            InitActor(*actor);
        }

        Become(&TThis::StateWork);
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvWorker::TEvHandshake, Handle);
            hFunc(TEvWorker::TEvPoll, Handle);
            hFunc(TEvWorker::TEvData, Handle);
            hFunc(TEvWorker::TEvGone, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    mutable TMaybe<TString> LogPrefix;
    TActorInfo Reader;
    TActorInfo Writer;
};

IActor* CreateWorker(THolder<IActor>&& reader, THolder<IActor>&& writer) {
    return new TWorker(std::move(reader), std::move(writer));
}

}
