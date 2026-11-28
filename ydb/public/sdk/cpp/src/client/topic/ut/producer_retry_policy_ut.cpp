#include <ydb/public/sdk/cpp/src/client/topic/impl/producer.h>
#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::inline Dev::NTopic {

struct TProducerRetryPolicyTestHelper {
    static void ReportOverload(IProducer& writer) {
        auto& producer = static_cast<TProducer&>(writer);
        TProducer::WrappedWriteSessionPtr session;
        {
            std::lock_guard guard(producer.GlobalLock);
            session = producer.SessionsWorker->GetWriteSession(0);
        }
        UNIT_ASSERT(session);

        auto impl = static_cast<TWriteSession&>(*session->Session).TryGetImpl();
        TWriteSessionImpl::THandleResult result;
        {
            std::lock_guard guard(impl->Lock);
            result = impl->OnErrorImpl({EStatus::OVERLOADED, "temporary overload"});
        }
        impl->ProcessHandleResult(result);
    }
};

namespace {

constexpr auto Timeout = TDuration::Seconds(15);

void WriteAndFlush(IProducer& producer) {
    UNIT_ASSERT(producer.Write(TWriteMessage("message")).IsQueued());
    auto flush = producer.Flush();
    UNIT_ASSERT_C(flush.Wait(Timeout), "Producer flush timed out");
    UNIT_ASSERT(flush.GetValue().IsSuccess());
}

std::shared_ptr<IProducer> StartProducer(const NTests::TTopicSdkTestSetup& setup, TProducerSettings settings) {
    settings.Path(setup.GetTopicPath()).Codec(ECodec::RAW);
    settings.ProducerIdPrefix("overload").AsyncExecutionMode(true);
    auto producer = setup.MakeClient().CreateProducer(settings);
    WriteAndFlush(*producer);
    return producer;
}

} // namespace

Y_UNIT_TEST_SUITE(ProducerRetryPolicy) {
    Y_UNIT_TEST(RetriesTemporaryOverload) {
        NTests::TTopicSdkTestSetup setup(TEST_CASE_NAME);
        TProducerSettings settings;
        settings.RetryPolicy(IRetryPolicy::GetFixedIntervalPolicy(TDuration::MilliSeconds(10)));
        auto producer = StartProducer(setup, settings);

        TProducerRetryPolicyTestHelper::ReportOverload(*producer);

        WriteAndFlush(*producer);
        UNIT_ASSERT_VALUES_EQUAL(producer->GetWriteStats().MessagesWritten, 2);
        UNIT_ASSERT(producer->Close(Timeout).IsSuccess());
    }

    Y_UNIT_TEST(OverloadWithoutRetryReturnsOriginalError) {
        NTests::TTopicSdkTestSetup setup(TEST_CASE_NAME);
        auto closed = NThreading::NewPromise<TSessionClosedEvent>();
        TProducerSettings settings;
        settings.RetryPolicy(IRetryPolicy::GetNoRetryPolicy());
        settings.EventHandlers_.SessionClosedHandler(
            [closed](const TSessionClosedEvent& event) mutable { closed.TrySetValue(event); });
        auto producer = StartProducer(setup, settings);

        TProducerRetryPolicyTestHelper::ReportOverload(*producer);

        auto event = closed.GetFuture();
        UNIT_ASSERT_C(event.Wait(Timeout), "Producer did not report the session error");
        UNIT_ASSERT_VALUES_EQUAL(event.GetValue().GetStatus(), EStatus::OVERLOADED);
        UNIT_ASSERT_STRING_CONTAINS(event.GetValue().GetIssues().ToString(), "temporary overload");
    }
}

} // namespace NYdb::NTopic
