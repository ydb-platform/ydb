#include <yt/yt/library/tracing/jaeger/tracer.h>

#include <yt/yt/library/tracing/batch_trace.h>

#include <yt/yt/core/tracing/trace_context.h>

#include <util/generic/yexception.h>

#include <util/system/env.h>

#include <random>

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

    TBatchTrace batchTrace;
    batchTrace.Join(traceContext);

    auto [asyncChildTraceContext, asyncChildSampled] = batchTrace.StartSpan("TestBatchTrace");
    YT_VERIFY(asyncChildSampled);

    traceContext->Finish();

    asyncChildTraceContext->Finish();

    Cout << ToString(traceContext->GetTraceId()) << '\t' << ToString(asyncChildTraceContext->GetTraceId()) << Endl;
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

    TBatchTrace batchTrace;
    batchTrace.Join(slowRequestContext);
    auto [asyncChildTraceContext, asyncChildSampled] = batchTrace.StartSpan("TestBatchTrace");

    YT_VERIFY(!asyncChildSampled);
    YT_VERIFY(!slowRequestContext->IsSampled());

    traceContext->SetSampled();
    YT_VERIFY(slowRequestContext->IsSampled());
    YT_VERIFY(!asyncChildTraceContext->IsSampled());

    slowRequestContext->Finish();
    traceContext->Finish();
}

NAuth::TTvmServiceConfigPtr GetTvmMockConfig()
{
    auto config = New<NAuth::TTvmServiceConfig>();
    config->EnableMock = true;
    config->ClientSelfSecret = "TestSecret-0";
    config->ClientDstMap["tracing"] = 10;
    config->ClientEnableServiceTicketFetching = true;

    return config;
}

NAuth::TTvmServiceConfigPtr GetTvmConfig()
{
    auto config = New<NAuth::TTvmServiceConfig>();
    config->ClientSelfId = FromString<NAuth::TTvmId>(GetEnv("TVM_ID"));
    config->ClientSelfSecretEnv = "TVM_SECRET";
    config->ClientDstMap["tracing"] = FromString<NAuth::TTvmId>(GetEnv("TRACING_TVM_ID"));
    config->ClientEnableServiceTicketFetching = true;

    return config;
}

int main(int argc, char* argv[])
{
    try {

        bool test = false;
        auto usage = Format("usage: %v [--test] COLLECTOR_ENDPOINTS", argv[0]);

        if (argc >= 2 && argv[1] == TString("--test")) {
            test = true;
            argv++;
            argc--;
        }

        if (argc < 2) {
            throw yexception() << usage;
        }

        static auto config = New<NTracing::TJaegerTracerConfig>();
        config->CollectorChannelConfig = New<NRpc::NGrpc::TChannelConfig>();
        config->CollectorChannelConfig->Address = argv[1];

        config->FlushPeriod = TDuration::MilliSeconds(test ? 100 : 1000);

        config->ServiceName = "example";
        config->ProcessTags["host"] = "prime-dev.qyp.yandex-team.ru";
        config->TvmService = test ? GetTvmMockConfig() : GetTvmConfig();

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
