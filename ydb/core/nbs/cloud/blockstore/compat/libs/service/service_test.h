#pragma once

#include "public.h"

#include "context.h"
#include "service.h"

#include <library/cpp/threading/future/future.h>

#include <functional>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TTestService
    : public IBlockStore
{
    void Start() override {
        StartHandler();
    }

    void Stop() override {
        StopHandler();
    }

    std::function<void()> StartHandler = [] () {};
    std::function<void()> StopHandler = [] () {};

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        Y_UNUSED(bytesCount);
        return nullptr;
    }

#define BLOCKSTORE_DECLARE_METHOD(name, ...)                          \
    using T##name##Handler =                                          \
        std::function<NThreading::TFuture<NProto::T##name##Response>( \
            std::shared_ptr<NProto::T##name##Request> request)>;      \
                                                                      \
    T##name##Handler name##Handler;                                   \
                                                                      \
    void SetHandler(T##name##Handler handler)                         \
    {                                                                 \
        name##Handler = std::move(handler);                           \
    }                                                                 \
                                                                      \
    NThreading::TFuture<NProto::T##name##Response> name(              \
        TCallContextPtr callContext,                                  \
        std::shared_ptr<NProto::T##name##Request> request) override   \
    {                                                                 \
        Y_UNUSED(callContext);                                        \
        Y_DEBUG_ABORT_UNLESS(name##Handler);                          \
        return name##Handler(std::move(request));                     \
    }                                                                 \
    // BLOCKSTORE_DECLARE_METHOD

    BLOCKSTORE_SERVICE(BLOCKSTORE_DECLARE_METHOD)

#undef BLOCKSTORE_DECLARE_METHOD
};

}   // namespace NCloud::NBlockStore
