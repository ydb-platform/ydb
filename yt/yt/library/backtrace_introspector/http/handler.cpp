#include "handler.h"

#include <yt/yt/core/http/server.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/library/backtrace_introspector/introspect.h>

namespace NYT::NBacktraceIntrospector {

using namespace NHttp;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class THandlerBase
    : public IHttpHandler
{
public:
    void HandleRequest(const IRequestPtr& /*req*/, const IResponseWriterPtr& rsp) override
    {
        try {
            static const auto queue = New<TActionQueue>("BacktraceIntro");
            auto dumpFuture = BIND(&THandlerBase::Dump, MakeStrong(this))
                .AsyncVia(queue->GetInvoker())
                .Run();

            auto dump = WaitFor(dumpFuture)
                .ValueOrThrow();

            WaitFor(rsp->WriteBody(TSharedRef::FromString(dump)))
                .ThrowOnError();

            WaitFor(rsp->Close())
                .ThrowOnError();
        } catch (const std::exception& ex) {
            if (!rsp->AreHeadersFlushed()) {
                rsp->SetStatus(EStatusCode::InternalServerError);
                WaitFor(rsp->WriteBody(TSharedRef::FromString(ex.what())))
                    .ThrowOnError();
            }
            throw;
        }
    }

protected:
    virtual TString Dump() = 0;
};

class TThreadsHandler
    : public THandlerBase
{
private:
    TString Dump() override
    {
        return FormatIntrospectionInfos(IntrospectThreads());
    }
};

class TFibersHandler
    : public THandlerBase
{
private:
    TString Dump() override
    {
        return FormatIntrospectionInfos(IntrospectFibers());
    }
};

void Register(
    const IRequestPathMatcherPtr& handlers,
    const TString& prefix)
{
    handlers->Add(prefix + "/threads", New<TThreadsHandler>());
    handlers->Add(prefix + "/fibers", New<TFibersHandler>());
}

void Register(
    const IServerPtr& server,
    const TString& prefix)
{
    Register(server->GetPathMatcher(), prefix);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBacktraceIntrospector
