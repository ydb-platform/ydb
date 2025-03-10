#include <yt/yt/core/rpc/bus/server.h>
#include <yt/yt/core/rpc/server.h>

#include <yt/yt/core/bus/tcp/server.h>
#include <yt/yt/core/bus/tcp/config.h>

#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/rpc/unittests/lib/test_service.h>

using namespace NYT;
using namespace NYT::NBus;
using namespace NYT::NRpc;
using namespace NYT::NRpc::NBus;
using namespace NYT::NConcurrency;
using namespace NYT::NYson;
using namespace NYT::NYTree;

static const auto Logger = NLogging::TLogger("RpcTestServer");

int main(int argc, char* argv[])
{
    try {
        if (argc != 2) {
            THROW_ERROR_EXCEPTION("Config argument is missing. Pass empty string to get config schema.");
        }

        TYsonStringBuf configText(argv[1]);
        auto busConfig = New<TBusServerConfig>();

        if (configText.AsStringBuf().Empty()) {
            TYsonWriter writer(&Cout, EYsonFormat::Pretty);
            busConfig->WriteSchema(&writer);
            Cout << Endl;
            return 1;
        }

        // FIXME(khlebnikov): ConvertTo does not allow to set UnrecognizedStrategy.
        // busConfig = ConvertTo<TBusServerConfigPtr>(config);

        busConfig->SetUnrecognizedStrategy(EUnrecognizedStrategy::ThrowRecursive);
        Deserialize(*busConfig, ConvertToNode(configText));

        YT_LOG_INFO("Config: %v", ConvertToYsonString(busConfig, EYsonFormat::Text));

        auto busServer = CreateBusServer(busConfig);
        auto server = CreateBusServer(busServer);

        auto workerPool = CreateThreadPool(4, "Worker");
        auto service = CreateTestService(workerPool->GetInvoker(), false, /*createChannel*/ {}, GetNullMemoryUsageTracker());
        server->RegisterService(service);
        server->Start();

        Sleep(TDuration::Max());
    } catch (const std::exception& ex) {
        Cerr << ex.what() << Endl;
        return 1;
    }

    return 0;
}
