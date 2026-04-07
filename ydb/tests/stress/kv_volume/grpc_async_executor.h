#pragma once

#include <grpcpp/client_context.h>
#include <grpcpp/completion_queue.h>
#include <grpcpp/support/status.h>

#include <util/generic/vector.h>
#include <util/system/types.h>

#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>

namespace NKvVolumeStress {

class TGrpcAsyncExecutor {
public:
    explicit TGrpcAsyncExecutor(ui32 threadCount);
    ~TGrpcAsyncExecutor();

    ui32 ThreadCount() const;

    template <typename TResponse, typename TStartCall>
    grpc::Status Unary(TStartCall&& startCall, grpc::ClientContext* context, TResponse* response) {
        grpc::Status status;
        TBlockingTag tag;

        auto rpc = startCall(context, NextQueue());
        rpc->Finish(response, &status, &tag);

        const bool ok = tag.Wait();
        if (!ok) {
            return grpc::Status(grpc::StatusCode::UNKNOWN, "gRPC completion queue returned !ok");
        }

        return status;
    }

    template <typename TResponse, typename TStartCall, typename TDone>
    void UnaryAsync(TStartCall&& startCall, std::unique_ptr<grpc::ClientContext> context, TDone&& done) {
        using TDoneType = std::decay_t<TDone>;

        struct TUnaryTag final : ICompletionTag {
            TUnaryTag(std::unique_ptr<grpc::ClientContext> context, TDoneType done)
                : Context(std::move(context))
                , Done(std::move(done))
            {
            }

            void OnDone(bool ok) override {
                Done(ok, Status, *Response);
                delete this;
            }

            std::unique_ptr<grpc::ClientContext> Context;
            std::shared_ptr<TResponse> Response = std::make_shared<TResponse>();
            grpc::Status Status;
            TDoneType Done;
        };

        auto* tag = new TUnaryTag(std::move(context), std::forward<TDone>(done));
        try {
            auto rpc = startCall(tag->Context.get(), NextQueue());
            rpc->Finish(tag->Response.get(), &tag->Status, tag);
        } catch (...) {
            delete tag;
            throw;
        }
    }

private:
    struct ICompletionTag {
        virtual ~ICompletionTag() = default;
        virtual void OnDone(bool ok) = 0;
    };

    struct TBlockingTag final : ICompletionTag {
        void OnDone(bool ok) override;
        bool Wait();

    private:
        std::mutex Mutex_;
        std::condition_variable Cv_;
        bool Done_ = false;
        bool Ok_ = false;
    };

    grpc::CompletionQueue* NextQueue();
    void PollLoop(grpc::CompletionQueue* completionQueue);

private:
    const ui32 ThreadCount_ = 1;
    std::atomic<ui64> NextQueueIndex_ = 0;
    TVector<std::unique_ptr<grpc::CompletionQueue>> CompletionQueues_;
    TVector<std::thread> PollerThreads_;
};

} // namespace NKvVolumeStress
