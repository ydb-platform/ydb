#include "helpers.h"
#include "config.h"
#include "private.h"

#include <yt/yt/core/misc/lazy_ptr.h>
#include <yt/yt/core/misc/ref_counted_tracker.h>
#include <yt/yt/core/misc/ref_counted_tracker_profiler.h>

#include <yt/yt/core/bus/tcp/dispatcher.h>

#include <yt/yt/core/concurrency/fiber_manager.h>

#include <yt/yt/library/tracing/jaeger/tracer.h>

#include <yt/yt/library/profiling/perf/counters.h>

#include <yt/yt/library/profiling/resource_tracker/resource_tracker.h>

#include <yt/yt/library/tcmalloc/tcmalloc_manager.h>

#include <yt/yt/core/logging/log_manager.h>

#include <yt/yt/core/concurrency/execution_stack.h>
#include <yt/yt/core/concurrency/fiber_scheduler_thread.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/net/address.h>

#include <yt/yt/core/yson/protobuf_interop.h>

#include <yt/yt/core/rpc/dispatcher.h>
#include <yt/yt/core/rpc/grpc/dispatcher.h>

#include <yt/yt/core/service_discovery/yp/service_discovery.h>

#include <yt/yt/core/threading/spin_wait_slow_path_logger.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

#include <util/string/split.h>
#include <util/system/thread.h>

#include <mutex>
#include <thread>

namespace NYT {

using namespace NConcurrency;
using namespace NThreading;
using namespace NTCMalloc;

////////////////////////////////////////////////////////////////////////////////

void ConfigureSingletons(const TSingletonsConfigPtr& config)
{
    SetSpinWaitSlowPathLoggingThreshold(config->SpinWaitSlowPathLoggingThreshold);

    TFiberManager::Configure(config->FiberManager);

    NLogging::TLogManager::Get()->EnableReopenOnSighup();
    if (!NLogging::TLogManager::Get()->IsConfiguredFromEnv()) {
        NLogging::TLogManager::Get()->Configure(config->Logging);
    }

    NNet::TAddressResolver::Get()->Configure(config->AddressResolver);
    // By default, server components must have a reasonable FQDN.
    // Failure to do so may result in issues like YT-4561.
    NNet::TAddressResolver::Get()->EnsureLocalHostName();

    NBus::TTcpDispatcher::Get()->Configure(config->TcpDispatcher);

    NPipes::TIODispatcher::Get()->Configure(config->IODispatcher);

    NRpc::TDispatcher::Get()->Configure(config->RpcDispatcher);

    NRpc::NGrpc::TDispatcher::Get()->Configure(config->GrpcDispatcher);

    NRpc::TDispatcher::Get()->SetServiceDiscovery(
        NServiceDiscovery::NYP::CreateServiceDiscovery(config->YPServiceDiscovery));

    NTracing::SetGlobalTracer(New<NTracing::TJaegerTracer>(config->Jaeger));

    NProfiling::EnablePerfCounters();

    if (auto tracingConfig = config->TracingTransport) {
        NTracing::SetTracingTransportConfig(tracingConfig);
    }

    NTCMalloc::TTCMallocManager::Configure(config->TCMalloc);

    TStockpileManager::Reconfigure(*config->Stockpile);

    if (config->EnableRefCountedTrackerProfiling) {
        EnableRefCountedTrackerProfiling();
    }

    NProfiling::TResourceTracker::Configure(config->ResourceTracker);

    NYson::SetProtobufInteropConfig(config->ProtobufInterop);
}

void ReconfigureSingletons(const TSingletonsConfigPtr& config, const TSingletonsDynamicConfigPtr& dynamicConfig)
{
    SetSpinWaitSlowPathLoggingThreshold(dynamicConfig->SpinWaitSlowPathLoggingThreshold.value_or(config->SpinWaitSlowPathLoggingThreshold));

    TFiberManager::Configure(config->FiberManager->ApplyDynamic(dynamicConfig->FiberManager));

    if (!NLogging::TLogManager::Get()->IsConfiguredFromEnv()) {
        NLogging::TLogManager::Get()->Configure(
            config->Logging->ApplyDynamic(dynamicConfig->Logging),
            /*sync*/ false);
    }

    auto tracer = NTracing::GetGlobalTracer();
    if (auto jaeger = DynamicPointerCast<NTracing::TJaegerTracer>(tracer); jaeger) {
        jaeger->Configure(config->Jaeger->ApplyDynamic(dynamicConfig->Jaeger));
    }

    NBus::TTcpDispatcher::Get()->Configure(config->TcpDispatcher->ApplyDynamic(dynamicConfig->TcpDispatcher));

    NPipes::TIODispatcher::Get()->Configure(dynamicConfig->IODispatcher ? dynamicConfig->IODispatcher : config->IODispatcher);

    NRpc::TDispatcher::Get()->Configure(config->RpcDispatcher->ApplyDynamic(dynamicConfig->RpcDispatcher));

    if (dynamicConfig->TracingTransport) {
        NTracing::SetTracingTransportConfig(dynamicConfig->TracingTransport);
    } else if (config->TracingTransport) {
        NTracing::SetTracingTransportConfig(config->TracingTransport);
    }

    NTCMalloc::TTCMallocManager::Configure(dynamicConfig->TCMalloc
        ? config->TCMalloc->ApplyDynamic(dynamicConfig->TCMalloc)
        : config->TCMalloc);

    TStockpileManager::Reconfigure(*config->Stockpile->ApplyDynamic(dynamicConfig->Stockpile));

    NYson::SetProtobufInteropConfig(config->ProtobufInterop->ApplyDynamic(dynamicConfig->ProtobufInterop));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
