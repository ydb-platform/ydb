#include "reader.h"
#include "reader_mock.h"

#include <functional>

#include <util/generic/vector.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/basics/appdata.h>


namespace NKikimr::NChangeMirroring {

using namespace NActors;

class TActorReader
    : public TActorBootstrapped<TActorReader>
{
    struct TEvPrivate {
        enum EEv {
            EvPoll = EventSpaceBegin(TEvents::ES_PRIVATE),
        };

        struct TEvPoll : public TEventLocal<TEvPoll, EEv::EvPoll> {
            TEvPoll(std::function<void(TActorReader&)> cb)
                : callback(cb)
            {}
            std::function<void(TActorReader&)> callback;
        };
    };
public:
    TActorReader(
        const TActorId& edge,
        const TVector<TRcBuf>& data)
        : ParentActorId(edge)
        , Data(data)
    {}

    bool HasNext() const {
        return Offset < MaxPolled;
    }

    ui64 Remaining() const {
        return MaxPolled - Offset;
    }

    TRcBuf ReadNext() {
        return Data[Offset++];
    }

    bool NeedPoll() const {
        return MaxPolled < Data.size();
    }

    void Poll(std::function<void(TActorReader&)> callback) {
        Y_VERIFY(NActors::TlsActivationContext && NActors::TlsActivationContext->ExecutorThread.ActorSystem, "Method must be called in actor system context");
        TActivationContext::ActorSystem()->Send(SelfId(), new TEvPrivate::TEvPoll(callback));
    }

    void PollDerived(std::function<void(TActorReader&)> callback) {
        Poll([cb = callback](TActorReader& reader) -> void {
            TActorReader& self = static_cast<TActorReader&>(reader);
            return cb(self);
        });
    }

    void Bootstrap() {
        Become(&TThis::StateWork);
        Send(ParentActorId, new TEvents::TEvWakeup);
    }

    void OnPollFinished(TEvPrivate::TEvPoll::TPtr &ev) {
        MaxPolled = std::max(Data.size(), MaxPolled + PollBatchSize);
        ev->Get()->callback(*this);
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvPoll, OnPollFinished);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }
private:
    const TActorId ParentActorId;
    TVector<TRcBuf> Data;
    ui64 Offset = 0;
    ui64 PollBatchSize = 4;
    ui64 MaxPolled = 0;
};

Y_UNIT_TEST_SUITE(Reader) {
    Y_UNIT_TEST(TestActorReader) {
        TTestActorRuntime runtime;
        runtime.Initialize(NKikimr::TAppPrepare().Unwrap());

        TString data = "Lorem ipsum dolor sit amet,"
            " consectetur adipiscing elit, sed do eiusmod tempor"
            " incididunt ut labore et dolore magna aliqua.";

        auto buf = TRcBuf(data);
        TVector<TRcBuf> slicedData;
        // slice data into pieces [1] [2, 3] [4, 5, 6] ...
        for(ui64 i = 1, offset = 0; offset < data.size(); ++i) {
            TRcBuf piece(buf);
            piece.TrimFront(data.size() - offset);
            ui64 size = std::min(i, data.size() - offset);
            piece.TrimBack(size);
            Y_ABORT_UNLESS(piece.size() == i || piece.size() == data.size() - offset);
            offset += i;
            slicedData.push_back(piece);
        }

        const auto edge = runtime.AllocateEdgeActor(0);
        auto* reader = new TActorReader(edge, slicedData);
        runtime.Register(reader);

        auto* readerClient = new TReaderClientMock(AIReader::TReaderActorHandle{edge});
        readerClient->OnBootstrap = [](auto& mock) {
            mock.BecomeStateWork();
            mock.Poll(AIReader::Tag{});
        };
        runtime.Register(readerClient);

        TAutoPtr<IEventHandle> handle;

        runtime.GrabEdgeEventRethrow<TEvents::TEvWakeup>(handle);

        TString result;

        while (reader->HasNext() || reader->NeedPoll()) {
            if (reader->NeedPoll()) {
                runtime.RunInActorContext(edge, [&](){
                    reader->PollDerived([&](TActorReader& reader) {
                        reader.ActorContext().Send(edge, new TEvents::TEvWakeup);
                    });
                });
                // wait poll
                runtime.GrabEdgeEvent<TEvents::TEvWakeup>(handle);
            }

            while (reader->HasNext()) {
                TRcBuf piece = reader->ReadNext();
                result += TString(piece.Data(), piece.size());
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(result, data);
    }
}

} // namespace NKikimr::NChangeMirroring
