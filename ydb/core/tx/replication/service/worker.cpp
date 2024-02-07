#include "worker.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/services/services.pb.h>

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
        if (ev->Sender == Reader) {
            Reader.InitDone = true;
        } else if (ev->Sender == Writer) {
            Writer.InitDone = true;
        } else {
            // TODO: log warn
        }

        if (Reader && Writer) {
            Send(Reader, new TEvWorker::TEvPoll());
        }
    }

    void Handle(TEvWorker::TEvPoll::TPtr& ev) {
        if (ev->Sender != Writer) {
            // TODO: log warn
            return;
        }

        Send(ev->Forward(Reader));
    }

    void Handle(TEvWorker::TEvData::TPtr& ev) {
        if (ev->Sender != Reader) {
            // TODO: log warn
            return;
        }

        Send(ev->Forward(Writer));
    }

    void Handle(TEvents::TEvGone::TPtr& ev) {
        if (ev->Sender == Reader) {
            // TODO
        } else if (ev->Sender == Writer) {
            // TODO
        } else {
            // TODO: log warn
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
            hFunc(TEvents::TEvGone, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    TActorInfo Reader;
    TActorInfo Writer;
};

IActor* CreateWorker(THolder<IActor>&& reader, THolder<IActor>&& writer) {
    return new TWorker(std::move(reader), std::move(writer));
}

}
