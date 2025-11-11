#include "dnsresolver.h"

#include <ydb/library/actors/testlib/test_runtime.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/string/builder.h>

#include <ares.h>

using namespace NActors;
using namespace NActors::NDnsResolver;

template <>
void Out<ares_status_t>(IOutputStream& out, ares_status_t status) {
    out << static_cast<int>(status);
}

Y_UNIT_TEST_SUITE(DnsResolver) {

    struct TSilentUdpServer {
        TInetDgramSocket Socket;
        ui16 Port;

        TSilentUdpServer() {
            TSockAddrInet addr("127.0.0.1", 0);
            int err = Socket.Bind(&addr);
            Y_ABORT_UNLESS(err == 0, "Cannot bind a udp socket");
            Port = addr.GetPort();
        }
    };

    Y_UNIT_TEST(ResolveLocalHost) {
        TTestActorRuntimeBase runtime;
        runtime.Initialize();
        auto sender = runtime.AllocateEdgeActor();
        auto resolver = runtime.Register(CreateSimpleDnsResolver());
        runtime.Send(new IEventHandle(resolver, sender, new TEvDns::TEvGetHostByName("localhost", AF_UNSPEC)),
                0, true);
        auto ev = runtime.GrabEdgeEventRethrow<TEvDns::TEvGetHostByNameResult>(sender);
        UNIT_ASSERT_VALUES_EQUAL_C(ev->Get()->Status, 0, ev->Get()->ErrorText);
        size_t addrs = ev->Get()->AddrsV4.size() + ev->Get()->AddrsV6.size();
        UNIT_ASSERT_C(addrs > 0, "Got " << addrs << " addresses");
    }

    Y_UNIT_TEST(ResolveYandexRu) {
        TTestActorRuntimeBase runtime;
        runtime.Initialize();
        auto sender = runtime.AllocateEdgeActor();
        auto resolver = runtime.Register(CreateSimpleDnsResolver());
        runtime.Send(new IEventHandle(resolver, sender, new TEvDns::TEvGetHostByName("yandex.ru", AF_UNSPEC)),
                0, true);
        auto ev = runtime.GrabEdgeEventRethrow<TEvDns::TEvGetHostByNameResult>(sender);
        UNIT_ASSERT_VALUES_EQUAL_C(ev->Get()->Status, 0, ev->Get()->ErrorText);
        size_t addrs = ev->Get()->AddrsV4.size() + ev->Get()->AddrsV6.size();
        UNIT_ASSERT_C(addrs > 0, "Got " << addrs << " addresses");
    }

    Y_UNIT_TEST(GetAddrYandexRu) {
        TTestActorRuntimeBase runtime;
        runtime.Initialize();
        auto sender = runtime.AllocateEdgeActor();
        auto resolver = runtime.Register(CreateSimpleDnsResolver());

        runtime.Send(new IEventHandle(resolver, sender, new TEvDns::TEvGetAddr("yandex.ru", AF_UNSPEC)),
                0, true);
        auto ev = runtime.GrabEdgeEventRethrow<TEvDns::TEvGetAddrResult>(sender);
        UNIT_ASSERT_VALUES_EQUAL_C(ev->Get()->Status, 0, ev->Get()->ErrorText);
        UNIT_ASSERT_C(ev->Get()->IsV4() || ev->Get()->IsV6(), "Expect v4 or v6 address");
    }

    Y_UNIT_TEST(ResolveTimeout) {
        TSilentUdpServer server;
        TTestActorRuntimeBase runtime;
        runtime.Initialize();
        auto sender = runtime.AllocateEdgeActor();
        TSimpleDnsResolverOptions options;
        options.Timeout = TDuration::MilliSeconds(250);
        options.Attempts = 2;
        options.Servers.emplace_back(TStringBuilder() << "127.0.0.1:" << server.Port);
        auto resolver = runtime.Register(CreateSimpleDnsResolver(options));
        runtime.Send(new IEventHandle(resolver, sender, new TEvDns::TEvGetHostByName("timeout.yandex.ru", AF_INET)),
                0, true);
        auto ev = runtime.GrabEdgeEventRethrow<TEvDns::TEvGetHostByNameResult>(sender);
        UNIT_ASSERT_VALUES_EQUAL_C(ev->Get()->Status, ARES_ETIMEOUT, ev->Get()->ErrorText);
    }

    Y_UNIT_TEST(ResolveGracefulStop) {
        TSilentUdpServer server;
        TTestActorRuntimeBase runtime;
        runtime.Initialize();
        auto sender = runtime.AllocateEdgeActor();
        TSimpleDnsResolverOptions options;
        options.Timeout = TDuration::Seconds(5);
        options.Attempts = 5;
        options.Servers.emplace_back(TStringBuilder() << "127.0.0.1:" << server.Port);
        auto resolver = runtime.Register(CreateSimpleDnsResolver(options));
        runtime.Send(new IEventHandle(resolver, sender, new TEvDns::TEvGetHostByName("timeout.yandex.ru", AF_INET)),
                0, true);
        runtime.Send(new IEventHandle(resolver, sender, new TEvents::TEvPoison), 0, true);
        auto ev = runtime.GrabEdgeEventRethrow<TEvDns::TEvGetHostByNameResult>(sender);
        UNIT_ASSERT_VALUES_EQUAL_C(ev->Get()->Status, ARES_ECANCELLED, ev->Get()->ErrorText);
    }

}
