#include "reader.h"

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
    , public IReader
{
public:
    TActorReader(
        TTestActorRuntime& runtime,
        std::function<void(const TActorContext& ctx)> onPoll,
        const TVector<TRcBuf>& data)
        : Runtime(runtime)
        , OnPoll(onPoll)
        , Data(data)
    {}

    bool HasNext() const override {
        return Offset < MaxPolled;
    }

    ui64 Remaining() const override {
        return MaxPolled - Offset;
    }

    TRcBuf ReadNext() override {
        return Data[Offset++];
    }

    bool NeedPoll() const override {
        return MaxPolled < Data.size();
    }
    void Poll() override {
        Runtime.Send(new IEventHandle(SelfId(), TActorId(), new TEvents::TEvWakeup()));
    }

    void Bootstrap() {
        Become(&TThis::StateWork);
        Poll();
    }

    void OnPollFinished() {
        MaxPolled = std::max(Data.size(), MaxPolled + PollBatchSize);
        OnPoll(ActorContext());
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            cFunc(TEvents::TEvWakeup::EventType, OnPollFinished);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }
private:
    TTestActorRuntime& Runtime;
    std::function<void(const TActorContext& ctx)> OnPoll;
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
        auto* reader = new TActorReader(runtime, [&] (const TActorContext& ctx) {
            ctx.Send(edge, new TEvents::TEvWakeup);
        }, slicedData);
        runtime.Register(reader);

        TAutoPtr<IEventHandle> handle;
        // wait initial read
        runtime.GrabEdgeEvent<TEvents::TEvWakeup>(handle);

        TString result;

        while (reader->HasNext() || reader->NeedPoll()) {
            if (reader->NeedPoll()) {
                reader->Poll();
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
