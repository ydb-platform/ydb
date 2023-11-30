#include "dnsresolver.h"

#include <ydb/library/actors/testlib/test_runtime.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NActors;
using namespace NActors::NDnsResolver;

Y_UNIT_TEST_SUITE(OnDemandDnsResolver) {

    Y_UNIT_TEST(ResolveLocalHost) {
        TTestActorRuntimeBase runtime;
        runtime.Initialize();
        auto sender = runtime.AllocateEdgeActor();
        auto resolver = runtime.Register(CreateOnDemandDnsResolver());
        runtime.Send(new IEventHandle(resolver, sender, new TEvDns::TEvGetHostByName("localhost", AF_UNSPEC)),
                0, true);
        auto ev = runtime.GrabEdgeEventRethrow<TEvDns::TEvGetHostByNameResult>(sender);
        UNIT_ASSERT_VALUES_EQUAL_C(ev->Get()->Status, 0, ev->Get()->ErrorText);
        size_t addrs = ev->Get()->AddrsV4.size() + ev->Get()->AddrsV6.size();
        UNIT_ASSERT_C(addrs > 0, "Got " << addrs << " addresses");
    }

}
