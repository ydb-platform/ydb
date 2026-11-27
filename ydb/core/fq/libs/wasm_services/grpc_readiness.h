#pragma once

#include <grpcpp/completion_queue.h>

#include <functional>
#include <thread>

namespace NFq::NWasmServices {

// One native waiter per executor. Tags only publish completion and wake the
// user-pool actor; neither response processing nor guest code runs here.
class TGrpcReadiness {
public:
    struct TTag {
        std::function<void(bool)> Notify;
    };

    TGrpcReadiness();
    ~TGrpcReadiness();

    grpc::CompletionQueue* Queue() {
        return &CompletionQueue;
    }

    // After user workers stop and all pending calls have been cancelled.
    // Drains native completions before joining; never called on the waiter.
    void Stop();

private:
    grpc::CompletionQueue CompletionQueue;
    std::thread Thread;
};

}   // namespace NFq::NWasmServices
