#include <ydb/public/sdk/cpp/src/library/grpc/client/grpc_client_low.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NYdbGrpc;

class TTestStub {
public:
    std::shared_ptr<grpc::ChannelInterface> ChannelInterface;
    TTestStub(std::shared_ptr<grpc::ChannelInterface> channelInterface)
        : ChannelInterface(channelInterface)
    {}
};

namespace NYdbGrpc::inline Dev {

struct TStreamRequestReadWriteProcessorTestAccess {
    template<typename TProcessor>
    static void SetStream(TProcessor& processor, typename TProcessor::TAsyncReaderWriterPtr stream) {
        processor.Stream = std::move(stream);
        processor.Started = true;
        processor.ConnectedCallback = nullptr;
    }
};

} // namespace NYdbGrpc::inline Dev

namespace {

struct TTestMessage {
    int Value = 0;

    void Swap(TTestMessage* other) {
        std::swap(Value, other->Value);
    }
};

class TTestAsyncReaderWriter final
    : public grpc::ClientAsyncReaderWriterInterface<TTestMessage, TTestMessage>
{
public:
    void StartCall(void*) override {
    }

    void ReadInitialMetadata(void*) override {
    }

    void Finish(grpc::Status* status, void* tag) override {
        FinishStatus = status;
        FinishTag = tag;
    }

    void Write(const TTestMessage& message, void* tag) override {
        WrittenValues.push_back(message.Value);
        WriteTag = tag;
    }

    void Write(const TTestMessage&, grpc::WriteOptions, void*) override {
        Y_ABORT("Unexpected Write with options");
    }

    void Read(TTestMessage*, void*) override {
        Y_ABORT("Unexpected Read");
    }

    void WritesDone(void* tag) override {
        WritesDoneTag = tag;
        ++WritesDoneCalls;
    }

    void CompleteWrite(bool ok) {
        Complete(WriteTag, ok);
    }

    void CompleteWritesDone(bool ok) {
        Complete(WritesDoneTag, ok);
    }

    void CompleteFinish(const grpc::Status& status, bool ok = true) {
        UNIT_ASSERT(FinishStatus);
        *FinishStatus = status;
        Complete(FinishTag, ok);
    }

    std::vector<int> WrittenValues;
    size_t WritesDoneCalls = 0;

private:
    static void Complete(void*& tag, bool ok) {
        UNIT_ASSERT(tag);
        auto* event = static_cast<IQueueClientEvent*>(std::exchange(tag, nullptr));
        event->Execute(ok);
        event->Destroy();
    }

private:
    grpc::Status* FinishStatus = nullptr;
    void* WriteTag = nullptr;
    void* WritesDoneTag = nullptr;
    void* FinishTag = nullptr;
};

using TTestProcessor = TStreamRequestReadWriteProcessor<TTestStub, TTestMessage, TTestMessage>;

struct TProcessorFixture {
    TProcessorFixture()
        : Processor(MakeIntrusive<TTestProcessor>([](TGrpcStatus&&, auto) {}))
    {
        auto stream = std::make_unique<TTestAsyncReaderWriter>();
        Stream = stream.get();
        TStreamRequestReadWriteProcessorTestAccess::SetStream(*Processor, std::move(stream));
    }

    TIntrusivePtr<TTestProcessor> Processor;
    TTestAsyncReaderWriter* Stream = nullptr;
};

} // anonymous namespace

Y_UNIT_TEST_SUITE(ChannelPoolTests) {
    Y_UNIT_TEST(UnusedStubsHoldersDeletion) {
        TGRpcClientConfig clientConfig("invalid_host:invalid_port");
        TTcpKeepAliveSettings tcpKeepAliveSettings =
        {
            true,
            30, // NYdb::TCP_KEEPALIVE_IDLE, unused in UT, but is necessary in constructor
            5, // NYdb::TCP_KEEPALIVE_COUNT, unused in UT, but is necessary in constructor
            10 // NYdb::TCP_KEEPALIVE_INTERVAL, unused in UT, but is necessary in constructor
        };
        auto channelPool = TChannelPool(tcpKeepAliveSettings, TDuration::MilliSeconds(250), true);
        std::vector<std::weak_ptr<grpc::ChannelInterface>> ChannelInterfacesWeak;

        {
            std::vector<std::shared_ptr<TTestStub>> stubsHoldersShared;
            auto storeStubsHolders = [&](TStubsHolder& stubsHolder) {
                stubsHoldersShared.emplace_back(stubsHolder.GetOrCreateStub<TTestStub>());
                ChannelInterfacesWeak.emplace_back((*stubsHoldersShared.rbegin())->ChannelInterface);
                return;
            };
            for (int i = 0; i < 10; ++i) {
                channelPool.GetStubsHolderLocked(
                    ToString(i),
                    clientConfig,
                    storeStubsHolders
                );
            }
        }

        auto now = Now();
        while (Now() < now + TDuration::MilliSeconds(500)){
            Sleep(TDuration::MilliSeconds(100));
        }

        channelPool.DeleteExpiredStubsHolders();

        bool allDeleted = true;
        for (auto i = ChannelInterfacesWeak.begin(); i != ChannelInterfacesWeak.end(); ++i) {
            allDeleted = allDeleted && i->expired();
        }

        // assertion is made for channel interfaces instead of stubs, because after stub deletion
        // TStubsHolder has the only shared_ptr for channel interface.
        UNIT_ASSERT_C(allDeleted, "expired stubsHolders were not deleted after timeout");

    }
} // ChannelPoolTests ut suite

Y_UNIT_TEST_SUITE(StreamRequestReadWriteProcessorTests) {
    Y_UNIT_TEST(RejectsOperationsQueuedAfterWritesDone) {
        TProcessorFixture fixture;
        std::vector<TGrpcStatus> firstWriteStatuses;
        std::vector<TGrpcStatus> writesDoneStatuses;
        std::vector<TGrpcStatus> lateWriteStatuses;
        std::vector<TGrpcStatus> repeatedWritesDoneStatuses;

        fixture.Processor->Write(TTestMessage{1}, [&](TGrpcStatus&& status) {
            firstWriteStatuses.push_back(std::move(status));
        });
        fixture.Processor->WritesDone([&](TGrpcStatus&& status) {
            writesDoneStatuses.push_back(std::move(status));
        });
        fixture.Processor->Write(TTestMessage{2}, [&](TGrpcStatus&& status) {
            lateWriteStatuses.push_back(std::move(status));
        });
        fixture.Processor->WritesDone([&](TGrpcStatus&& status) {
            repeatedWritesDoneStatuses.push_back(std::move(status));
        });

        UNIT_ASSERT_VALUES_EQUAL(fixture.Stream->WrittenValues.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Stream->WrittenValues.front(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Stream->WritesDoneCalls, 0);
        UNIT_ASSERT_VALUES_EQUAL(lateWriteStatuses.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(lateWriteStatuses.front().GRpcStatusCode, static_cast<int>(grpc::StatusCode::FAILED_PRECONDITION));
        UNIT_ASSERT_VALUES_EQUAL(repeatedWritesDoneStatuses.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(repeatedWritesDoneStatuses.front().GRpcStatusCode, static_cast<int>(grpc::StatusCode::FAILED_PRECONDITION));

        fixture.Stream->CompleteWrite(true);
        UNIT_ASSERT_VALUES_EQUAL(firstWriteStatuses.size(), 1);
        UNIT_ASSERT(firstWriteStatuses.front().Ok());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Stream->WritesDoneCalls, 1);

        fixture.Stream->CompleteWritesDone(true);
        UNIT_ASSERT_VALUES_EQUAL(writesDoneStatuses.size(), 1);
        UNIT_ASSERT(writesDoneStatuses.front().Ok());
    }

    Y_UNIT_TEST(DeliversWritesDoneFailureAfterFinish) {
        TProcessorFixture fixture;
        std::vector<TGrpcStatus> writesDoneStatuses;
        std::vector<TGrpcStatus> finishStatuses;

        fixture.Processor->WritesDone([&](TGrpcStatus&& status) {
            writesDoneStatuses.push_back(std::move(status));
        });
        fixture.Stream->CompleteWritesDone(false);
        UNIT_ASSERT(writesDoneStatuses.empty());

        fixture.Processor->Finish([&](TGrpcStatus&& status) {
            finishStatuses.push_back(std::move(status));
        });
        fixture.Stream->CompleteFinish(grpc::Status(grpc::StatusCode::INTERNAL, "half-close failed"));

        UNIT_ASSERT_VALUES_EQUAL(writesDoneStatuses.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(writesDoneStatuses.front().GRpcStatusCode, static_cast<int>(grpc::StatusCode::INTERNAL));
        UNIT_ASSERT_VALUES_EQUAL(finishStatuses.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(finishStatuses.front().GRpcStatusCode, static_cast<int>(grpc::StatusCode::INTERNAL));
    }

    Y_UNIT_TEST(CancelWhileWritesDoneIsInFlight) {
        TProcessorFixture fixture;
        std::vector<TGrpcStatus> writesDoneStatuses;

        fixture.Processor->WritesDone([&](TGrpcStatus&& status) {
            writesDoneStatuses.push_back(std::move(status));
        });
        fixture.Processor->Cancel();
        fixture.Stream->CompleteWritesDone(false);
        fixture.Stream->CompleteFinish(grpc::Status(grpc::StatusCode::CANCELLED, "cancelled"));

        UNIT_ASSERT_VALUES_EQUAL(writesDoneStatuses.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(writesDoneStatuses.front().GRpcStatusCode, static_cast<int>(grpc::StatusCode::CANCELLED));
    }
} // StreamRequestReadWriteProcessorTests suite
