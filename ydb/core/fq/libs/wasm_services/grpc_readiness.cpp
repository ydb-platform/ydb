#include "grpc_readiness.h"

#include <memory>

namespace NFq::NWasmServices {

TGrpcReadiness::TGrpcReadiness()
    : Thread([this] {
        void* raw = nullptr;
        bool ok = false;
        while (CompletionQueue.Next(&raw, &ok)) {
            std::unique_ptr<TTag> tag(static_cast<TTag*>(raw));
            tag->Notify(ok);
        }
    })
{
}

TGrpcReadiness::~TGrpcReadiness() {
    Stop();
}

void TGrpcReadiness::Stop() {
    if (Thread.joinable()) {
        CompletionQueue.Shutdown();
        Thread.join();
    }
}

}   // namespace NFq::NWasmServices
