#pragma once

#include <ydb/core/nbs/cloud/storage/core/libs/common/thread.h>
#include <ydb/core/nbs/cloud/storage/core/libs/diagnostics/logging.h>
#include <ydb/core/nbs/cloud/storage/core/libs/grpc/request.h>

#include <ydb/library/actors/prof/tag.h>

#include <util/generic/maybe.h>
#include <util/generic/scope.h>
#include <util/string/builder.h>
#include <util/system/thread.h>

namespace NYdb::NBS::NGrpc {

namespace NInternal {

////////////////////////////////////////////////////////////////////////////////

struct TDummyExecutorScope
{
    int StartWait()
    {
        return {};
    }

    int StartExecute()
    {
        return {};
    }
};

}   // namespace NInternal

////////////////////////////////////////////////////////////////////////////////

template <typename TCompletionQueue, typename TRequestsInFlight>
struct TExecutorContext
{
    std::unique_ptr<TCompletionQueue> CompletionQueue;
    TRequestsInFlight RequestsInFlight;

    template <typename T>
    std::unique_ptr<T> EnqueueRequestHandler(std::unique_ptr<T> handler)
    {
        if (!RequestsInFlight.Register(handler.get())) {
            return handler;
        }
        Y_DEFER
        {
            if (handler) {
                RequestsInFlight.Unregister(handler.get());
            }
        };

        if (EnqueueCompletion(CompletionQueue.get(), handler.get())) {
            // ownership transferred to CompletionQueue
            Y_UNUSED(handler.release());
        }

        return handler;
    }

    template <typename THandler, typename... TArgs>
    void StartRequestHandler(TArgs&&... args)
    {
        auto handler =
            std::make_unique<THandler>(*this, std::forward<TArgs>(args)...);
        if (!this->RequestsInFlight.Register(handler.get())) {
            return;
        }
        Y_DEFER
        {
            if (handler) {
                this->RequestsInFlight.Unregister(handler.get());
            }
        };

        handler->PrepareRequest();
        // ownership transferred to CompletionQueue
        Y_UNUSED(handler.release());
    }
};

template <
    typename TCompletionQueue,
    typename TRequestsInFlight,
    typename TExecutorScope = NInternal::TDummyExecutorScope>
class TExecutor final
    : public ISimpleThread
    , public TExecutorContext<TCompletionQueue, TRequestsInFlight>
{
private:
    TString Name;
    TLog Log;
    TExecutorScope ExecutorScope;

public:
    TExecutor(
        TString name,
        std::unique_ptr<TCompletionQueue> completionQueue,
        TLog log,
        TExecutorScope executorScope = {})
        : Name(std::move(name))
        , Log(std::move(log))
        , ExecutorScope(std::move(executorScope))
    {
        this->CompletionQueue = std::move(completionQueue);
    }

    void Shutdown()
    {
        STORAGE_TRACE(Name << " executor's shutdown() started");

        this->RequestsInFlight.Shutdown();

        if (this->CompletionQueue) {
            this->CompletionQueue->Shutdown();
        }

        Join();

        this->CompletionQueue.reset();
    }

private:
    void* ThreadProc() override
    {
        ::NYdb::NBS::SetCurrentThreadName(Name);

        auto tagName = TStringBuilder() << "STORAGE_THREAD_" << Name;
        NProfiling::TMemoryTagScope tagScope(tagName.c_str());

        void* tag = nullptr;
        bool ok = false;
        while (WaitRequest(&tag, &ok)) {
            ProcessRequest(tag, ok);
        }

        return nullptr;
    }

    bool WaitRequest(void** tag, bool* ok)
    {
        [[maybe_unused]] auto activity = ExecutorScope.StartWait();

        return this->CompletionQueue->Next(tag, ok);
    }

    void ProcessRequest(void* tag, bool ok)
    {
        [[maybe_unused]] auto activity = ExecutorScope.StartExecute();

        auto handler = static_cast<TRequestHandlerBase*>(tag);
        Y_DEFER
        {
            handler->ReleaseCompletionTag();
        };
        handler->Process(ok);
    }
};

}   // namespace NYdb::NBS::NGrpc
