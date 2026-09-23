#pragma once

#include <ydb/public/sdk/cpp/src/library/grpc/client/grpc_client_low.h>

#include <library/cpp/testing/unittest/registar.h>

#include <utility>
#include <variant>

namespace NYdb::inline Dev::NTopic::NTests {

template <typename TRequest, typename TResponse>
class TMemoryTestProcessor final : public NYdbGrpc::IStreamRequestReadWriteProcessor<TRequest, TResponse> {
public:
    using TBase = NYdbGrpc::IStreamRequestReadWriteProcessor<TRequest, TResponse>;
    using TReadCallback = typename TBase::TReadCallback;
    using TWriteCallback = typename TBase::TWriteCallback;

    explicit TMemoryTestProcessor(bool rejectInline)
        : RejectInline(rejectInline)
    {}

    void Write(TRequest&&, TWriteCallback callback) override {
        if (RejectInline) {
            callback(NYdbGrpc::TGrpcStatus(grpc::StatusCode::CANCELLED, "Write request dropped"));
        } else {
            Callback = std::move(callback);
        }
    }

    void Cancel() override {}
    void ReadInitialMetadata(std::unordered_multimap<std::string, std::string>*, TReadCallback) override {}
    void Read(TResponse*, TReadCallback) override {}
    void Finish(TReadCallback) override {}
    void AddFinishedCallback(TReadCallback) override {}

    TWriteCallback Callback;

private:
    const bool RejectInline;
};

template <typename TAdapter, typename TSession>
void CheckWriteRequestMemory(TAdapter& adapter, TSession& session) {
    auto assertSingleReady = [&] {
        const auto events = session.GetEvents();
        UNIT_ASSERT_VALUES_EQUAL(events.size(), 1);
        UNIT_ASSERT(std::holds_alternative<typename TAdapter::TReadyEvent>(events.front()));
    };

    // The initial token is still outstanding. Protobuf allocation alone must
    // not grant another token when its completion crosses the memory limit.
    auto complete = adapter.QueueRequest();
    UNIT_ASSERT_GT(adapter.MemoryUsage(), 1);
    complete(NYdbGrpc::TGrpcStatus());
    UNIT_ASSERT_VALUES_EQUAL(adapter.MemoryUsage(), 0);
    UNIT_ASSERT(session.GetEvents().empty());

    // Completion can be reordered with other writes and an application ACK.
    // Payload memory disappearing must not release the pending protobufs.
    adapter.ChangeMemoryUsage(32);
    auto first = adapter.QueueRequest();
    const auto firstUsage = adapter.MemoryUsage() - 32;
    auto second = adapter.QueueRequest();
    adapter.ConsumeToken();
    adapter.ChangeMemoryUsage(-32);
    second(NYdbGrpc::TGrpcStatus());
    UNIT_ASSERT_VALUES_EQUAL(adapter.MemoryUsage(), firstUsage);
    UNIT_ASSERT(session.GetEvents().empty());
    first(NYdbGrpc::TGrpcStatus());
    UNIT_ASSERT_VALUES_EQUAL(adapter.MemoryUsage(), 0);
    assertSingleReady();

    // A previous connection still owns its outstanding protobuf allocation.
    complete = adapter.QueueRequest();
    adapter.NextConnectionGeneration();
    complete(NYdbGrpc::TGrpcStatus());
    UNIT_ASSERT_VALUES_EQUAL(adapter.MemoryUsage(), 0);
    UNIT_ASSERT(session.GetEvents().empty());

    // The real gRPC processor rejects writes synchronously after cancellation.
    // QueueRequest holds the session lock, so taking it in that callback hangs.
    adapter.ConsumeToken();
    adapter.QueueRequest(true);
    UNIT_ASSERT_C(session.WaitEvent().Wait(TDuration::Seconds(10)), "rejected write did not release memory");
    UNIT_ASSERT_VALUES_EQUAL(adapter.MemoryUsage(), 0);
    assertSingleReady();

    // Init failures use the existing error handler. They can also be rejected
    // synchronously, and must close a no-retry session without a lock deadlock.
    complete = adapter.QueueRequest();
    adapter.QueueRequest(true, true);
    UNIT_ASSERT_C(session.WaitEvent().Wait(TDuration::Seconds(10)), "rejected init did not close session");
    const auto closed = session.GetEvent();
    UNIT_ASSERT(closed && std::holds_alternative<typename TAdapter::TClosedEvent>(*closed));

    // Closing must not prevent a late completion from releasing its charge,
    // and that completion must not issue another token.
    complete(NYdbGrpc::TGrpcStatus());
    UNIT_ASSERT_VALUES_EQUAL(adapter.MemoryUsage(), 0);
    // A credential refresh can abort SendImpl before WriteToProcessorImpl.
    // Such a request is never queued and must not leave a memory charge.
    UNIT_ASSERT(!adapter.QueueRequest());
    UNIT_ASSERT_VALUES_EQUAL(adapter.MemoryUsage(), 0);
    for (const auto& event : session.GetEvents()) {
        UNIT_ASSERT(!std::holds_alternative<typename TAdapter::TReadyEvent>(event));
    }
}

} // namespace NYdb::NTopic::NTests
