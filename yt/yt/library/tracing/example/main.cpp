#include <random>

#include <yt/yt/core/tracing/trace_context.h>

#include <util/generic/yexception.h>

#include <yt/yt/library/tracing/jaeger/tracer.h>

using namespace NYT;
using namespace NYT::NTracing;

void SubrequestExample(std::optional<TString> endpoint)
{
    auto traceContext = TTraceContext::NewRoot("Example");
    traceContext->SetSampled();
    traceContext->AddTag("user", "prime");
    traceContext->SetTargetEndpoint(endpoint);

    traceContext->AddLogEntry(GetCpuInstant(), "Request started");

    Sleep(TDuration::MilliSeconds(10));
    auto childTraceContext = traceContext->CreateChild("Subrequest");
    childTraceContext->AddTag("index", "0");

    Sleep(TDuration::MilliSeconds(2));
    childTraceContext->Finish();

    Sleep(TDuration::MilliSeconds(2));
    traceContext->AddLogEntry(GetCpuInstant(), "Request finished");
    traceContext->Finish();

    Cout << ToString(traceContext->GetTraceId()) << Endl;
}

void DelayedSamplingExample(std::optional<TString> endpoint)
{
    auto traceContext = TTraceContext::NewRoot("Job");
    traceContext->SetRecorded();
    traceContext->SetTargetEndpoint(endpoint);

    auto fastRequestContext = traceContext->CreateChild("FastRequest");
    fastRequestContext->Finish();

    auto startContext = traceContext->CreateChild("Start");
    startContext->Finish();

    auto slowRequestContext = startContext->CreateChild("SlowRequest");

    traceContext->SetSampled();
    YT_VERIFY(slowRequestContext->IsSampled());

    slowRequestContext->Finish();
    traceContext->Finish();
}

int main(int argc, char* argv[])
{
    try {
        if (argc < 2) {
            throw yexception() << "usage: " << argv[0] << " COLLECTOR_ENDPOINTS";
        }

        auto config = New<NTracing::TJaegerTracerConfig>();
        config->CollectorChannelConfig = New<NRpc::NGrpc::TChannelConfig>();
        config->CollectorChannelConfig->Address = argv[1];

        config->FlushPeriod = TDuration::MilliSeconds(100);

        config->ServiceName = "example";
        config->ProcessTags["host"] = "prime-dev.qyp.yandex-team.ru";

        auto jaeger = New<NTracing::TJaegerTracer>(config);
        SetGlobalTracer(jaeger);

        for (int i = 1; i < argc; ++i) {
            std::optional<TString> endpoint;
            if (i != 1) {
                endpoint = argv[i];
            }

            SubrequestExample(endpoint);

            DelayedSamplingExample(endpoint);
        }

        jaeger->WaitFlush().Get();
    } catch (const std::exception& ex) {
        Cerr << ex.what() << Endl;
        return 1;
    }

    return 0;
}
