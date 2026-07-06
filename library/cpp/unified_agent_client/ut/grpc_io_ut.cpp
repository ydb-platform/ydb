#include <library/cpp/unified_agent_client/grpc_io.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <contrib/libs/grpc/include/grpcpp/grpcpp.h>
#include <contrib/libs/grpc/include/grpc/grpc.h>

using namespace NUnifiedAgent;

namespace {
    struct TDummyCallback : public IIOCallback {
        std::atomic<bool> Completed{false};
        std::atomic<int> Refs{0};

        IIOCallback* Ref() override {
            Refs++;
            return this;
        }

        void OnIOCompleted(EIOStatus) override {
            Completed = true;
        }
    };
}

class TGrpcTimerTest : public ::testing::Test {
protected:
    void SetUp() override {
        EnsureGrpcConfigured();
        grpc_init();
    }

    void TearDown() override {
        grpc_shutdown();
    }
};

TEST_F(TGrpcTimerTest, TestSetCancelFromExternalThread) {
    grpc::CompletionQueue cq;
    TAsyncJoiner joiner;
    auto cb = MakeHolder<TDummyCallback>();
    auto* cbPtr = cb.Get();
    cbPtr->Refs = 1; // Base ref

    TGrpcTimer timer(cq, std::move(cb), joiner);

    timer.Set(TInstant::Now() + TDuration::Seconds(10));

    void* tag;
    bool ok;

    switch (cq.AsyncNext(&tag, &ok, gpr_time_add(gpr_now(GPR_CLOCK_REALTIME), gpr_time_from_seconds(1, GPR_TIMESPAN)))) {
        case grpc::CompletionQueue::GOT_EVENT: {
            EXPECT_TRUE(ok);
            auto* iocb = static_cast<IIOCallback*>(tag);
            iocb->OnIOCompleted(EIOStatus::Ok);
            break;
        }
        case grpc::CompletionQueue::TIMEOUT:
            FAIL() << "Timeout waiting for CQ event";
        case grpc::CompletionQueue::SHUTDOWN:
            FAIL() << "CQ is shutdown";
    }

    EXPECT_FALSE(cbPtr->Completed.load());

    // Now the alarm is set, and the lambda is destroyed. The joiner should have its ref decremented.

    // Cancel the alarm via external thread
    timer.Cancel();

    // Pull the Cancel posted message
    switch (cq.AsyncNext(&tag, &ok, gpr_time_add(gpr_now(GPR_CLOCK_REALTIME), gpr_time_from_seconds(1, GPR_TIMESPAN)))) {
        case grpc::CompletionQueue::GOT_EVENT: {
            EXPECT_TRUE(ok);
            auto* iocb = static_cast<IIOCallback*>(tag);
            iocb->OnIOCompleted(EIOStatus::Ok); // Executes ApplyCancel + UnRef
            break;
        }
        default:
            FAIL() << "Expected event in CQ";
    }

    // Pull the actual grpc::Alarm completion since we canceled it
    switch (cq.AsyncNext(&tag, &ok, gpr_time_add(gpr_now(GPR_CLOCK_REALTIME), gpr_time_from_seconds(1, GPR_TIMESPAN)))) {
        case grpc::CompletionQueue::GOT_EVENT: {
            // ok might be false because the alarm was canceled
            auto* iocb = static_cast<IIOCallback*>(tag);
            iocb->OnIOCompleted(ok ? EIOStatus::Ok : EIOStatus::Error);
            break;
        }
        default:
            FAIL() << "Expected alarm completion event in CQ";
    }

    EXPECT_TRUE(cbPtr->Completed.load());

    joiner.Join().Wait();
}
