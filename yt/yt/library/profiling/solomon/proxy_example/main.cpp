#include <yt/yt/library/profiling/solomon/proxy.h>

#include <yt/yt/core/concurrency/poller.h>
#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/concurrency/thread_pool_poller.h>

#include <yt/yt/core/http/server.h>

#include <util/generic/yexception.h>

using namespace NYT;
using namespace NYT::NHttp;
using namespace NYT::NConcurrency;
using namespace NYT::NProfiling;

////////////////////////////////////////////////////////////////////////////////

class TStaticEndpointProvider
    : public IEndpointProvider
{
public:
    TStaticEndpointProvider(std::vector<TString> addresses)
        : Addresses_(std::move(addresses))
    { }

    TString GetComponentName() const override
    {
        return "static";
    }

    std::vector<TEndpoint> GetEndpoints() const override
    {
        std::vector<TEndpoint> endpoints;
        for (const auto& address : Addresses_) {
            endpoints.push_back({.Address = address});
        }

        return endpoints;
    }

private:
    std::vector<TString> Addresses_;
};

int main(int argc, char* argv[])
{
    try {
        if (argc != 2) {
            throw yexception() << "usage: " << argv[0] << " PORT";
        }

        auto port = FromString<int>(argv[1]);
        auto poller = CreateThreadPoolPoller(1, "Example");
        auto server = CreateServer(port, poller);

        auto proxy = New<TSolomonProxy>(New<TSolomonProxyConfig>(), poller);

        std::vector<TString> addresses = {
            "list",
            "your",
            "hosts",
            "here"
        };
        auto staticEndpointProvider = New<TStaticEndpointProvider>(addresses);
        proxy->RegisterEndpointProvider(staticEndpointProvider);

        proxy->Register("/solomon", server);

        server->Start();

        TDelayedExecutor::WaitForDuration(TDuration::Max());
    } catch (const std::exception& ex) {
        Cerr << ex.what() << Endl;
        _exit(1);
    }

    return 0;
}

////////////////////////////////////////////////////////////////////////////////
