#include "actor_wrappers.h"
#include "responses.h"

#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/basics/appdata.h>

#include <library/cpp/actors/core/events.h>
#include <library/cpp/testing/gmock_in_unittest/gmock.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/system/thread.h>

using namespace testing;

namespace NPersQueue {

namespace TEvPQLibTests {
    enum EEv {
        EvPQLibObjectIsDead = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvPQLibProducerCreateResponse,
        EvPQLibProducerCommitResponse,
        EvEnd
    };
} // TEvPQLibTests

class TMockProducer: public IProducer {
public:
    NThreading::TFuture<TProducerCreateResponse> Start(TInstant) noexcept override {
        return NThreading::MakeFuture<TProducerCreateResponse>(MockStart());
    }

    NThreading::TFuture<TProducerCommitResponse> Write(TProducerSeqNo, TData data) noexcept override {
        return Write(std::move(data));
    }

    NThreading::TFuture<TProducerCommitResponse> Write(TData data) noexcept override {
        return NThreading::MakeFuture<TProducerCommitResponse>(MockWrite(data));
    }

    NThreading::TFuture<TError> IsDead() noexcept override {
        return MockIsDead();
    }

    MOCK_METHOD(TProducerCreateResponse, MockStart, (), ());
    MOCK_METHOD(NThreading::TFuture<TError>, MockIsDead, (), ());
    MOCK_METHOD(TProducerCommitResponse, MockWrite, (TData), ());
};

Y_UNIT_TEST_SUITE(TProducerActorWrapperTest) {
    Y_UNIT_TEST(PassesEvents) {
        NActors::TTestActorRuntime runtime;
        runtime.Initialize(NKikimr::TAppPrepare().Unwrap());
        auto edgeActorId = runtime.AllocateEdgeActor();
        THolder<TMockProducer> producerHolder = MakeHolder<TMockProducer>();
        TMockProducer& producer = *producerHolder;

        // expectations
        TWriteResponse startResponse;
        startResponse.MutableInit()->SetMaxSeqNo(42);
        EXPECT_CALL(producer, MockStart())
            .WillOnce(Return(TProducerCreateResponse(std::move(startResponse))));

        NThreading::TPromise<TError> deadPromise = NThreading::NewPromise<TError>();
        EXPECT_CALL(producer, MockIsDead())
            .WillOnce(Return(deadPromise.GetFuture()));

        TWriteResponse writeResponse;
        writeResponse.MutableAck()->SetSeqNo(100);
        TData data("data");
        EXPECT_CALL(producer, MockWrite(data))
            .WillOnce(Return(TProducerCommitResponse(100, data, std::move(writeResponse))));

        // wrapper creation
        using TProducerType = TProducerActorWrapper<
            TEvPQLibTests::EvPQLibObjectIsDead,
            TEvPQLibTests::EvPQLibProducerCreateResponse,
            TEvPQLibTests::EvPQLibProducerCommitResponse
        >;

        THolder<TProducerType> wrapper = MakeHolder<TProducerType>(runtime.GetAnyNodeActorSystem(), edgeActorId, std::move(producerHolder));

        // checks
        auto actorId = wrapper->GetActorID();
        UNIT_ASSERT(actorId);

        {
            wrapper->Start(TInstant::Max());
            TAutoPtr<NActors::IEventHandle> handle;
            auto* startEvent = runtime.GrabEdgeEvent<TProducerType::TCreateResponseEvent>(handle);
            UNIT_ASSERT(startEvent != nullptr);
            UNIT_ASSERT_VALUES_EQUAL(handle->Sender, actorId);
            UNIT_ASSERT_VALUES_EQUAL(startEvent->Response.Response.GetInit().GetMaxSeqNo(), 42);
        }

        {
            wrapper->Write(data);
            TAutoPtr<NActors::IEventHandle> handle;
            auto* writeEvent = runtime.GrabEdgeEvent<TProducerType::TCommitResponseEvent>(handle);
            UNIT_ASSERT(writeEvent != nullptr);
            UNIT_ASSERT_VALUES_EQUAL(handle->Sender, actorId);
            UNIT_ASSERT_VALUES_EQUAL(writeEvent->Response.Response.GetAck().GetSeqNo(), 100);
            UNIT_ASSERT_EQUAL(writeEvent->Response.Data, data);
            UNIT_ASSERT_VALUES_EQUAL(writeEvent->Response.SeqNo, 100);
        }

        {
            TError err;
            err.SetDescription("trololo");
            deadPromise.SetValue(err);

            TAutoPtr<NActors::IEventHandle> handle;
            auto* deadEvent = runtime.GrabEdgeEvent<TProducerType::TObjectIsDeadEvent>(handle);
            UNIT_ASSERT(deadEvent != nullptr);
            UNIT_ASSERT_VALUES_EQUAL(handle->Sender, actorId);
            UNIT_ASSERT_STRINGS_EQUAL(deadEvent->Response.GetDescription(), "trololo");
        }
    }
}
} // namespace NPersQueue
