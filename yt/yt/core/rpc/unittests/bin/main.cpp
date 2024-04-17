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

int main(int argc, char* argv[])
{
    try {
        if (argc != 2) {
            THROW_ERROR_EXCEPTION("Port argument is missing");
        }

        auto port = FromString<int>(argv[1]);

        auto busConfig = TBusServerConfig::CreateTcp(port);
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
