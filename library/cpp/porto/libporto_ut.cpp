#include <library/cpp/testing/unittest/registar.h>
#include <libporto.hpp>

#include <signal.h>
#include <cassert>

#define Expect(a)   assert(a)
#define ExpectEq(a, b)   assert((a) == (b))
#define ExpectNeq(a, b)   assert((a) != (b))
#define ExpectSuccess(ret) assert((ret) == Porto::EError::Success)

const TString CT_NAME = "test-a";

void test_porto() {
    TVector<TString> list;
    TString str, path;

    signal(SIGPIPE, SIG_IGN);

    Porto::TPortoApi api;

    Expect(!api.Connected());
    Expect(api.GetFd() < 0);

    // Connect
    ExpectSuccess(api.Connect());

    Expect(api.Connected());
    Expect(api.GetFd() >= 0);

    // Disconnect
    api.Disconnect();

    Expect(!api.Connected());
    Expect(api.GetFd() < 0);

    // Auto connect
    ExpectSuccess(api.GetVersion(str, str));
    Expect(api.Connected());

    // Auto reconnect
    api.Disconnect();
    ExpectSuccess(api.GetVersion(str, str));
    Expect(api.Connected());

    // No auto reconnect
    api.Disconnect();
    api.SetAutoReconnect(false);
    ExpectEq(api.GetVersion(str, str), Porto::EError::SocketError);
    api.SetAutoReconnect(true);

    uint64_t val = api.GetTimeout();
    ExpectNeq(val, 0);
    ExpectSuccess(api.SetTimeout(5));

    ExpectSuccess(api.List(list));

    ExpectSuccess(api.ListProperties(list));

    ExpectSuccess(api.ListVolumes(list));

    ExpectSuccess(api.ListVolumeProperties(list));

    ExpectSuccess(api.ListLayers(list));

    ExpectSuccess(api.ListStorages(list));

    ExpectSuccess(api.Call("Version {}", str));

    ExpectSuccess(api.GetProperty("/", "state", str));
    ExpectEq(str, "meta");

    ExpectSuccess(api.GetProperty("/", "controllers", "memory", str));
    ExpectEq(str, "true");

    ExpectSuccess(api.GetProperty("/", "memory_usage", str));
    ExpectNeq(str, "0");

    val = 0;
    ExpectSuccess(api.GetInt("/", "memory_usage", val));
    ExpectNeq(val, 0);

    Porto::TContainer ct;
    ExpectSuccess(api.GetContainerSpec("/", ct));
    ExpectEq(ct.spec().name(), "/");

    ExpectEq(api.GetInt("/", "__wrong__", val), Porto::EError::InvalidProperty);
    ExpectEq(api.Error(), Porto::EError::InvalidProperty);
    ExpectEq(api.GetLastError(str), Porto::EError::InvalidProperty);

    ExpectSuccess(api.GetContainerSpec(CT_NAME, ct));
    ExpectEq(ct.error().error(), Porto::EError::ContainerDoesNotExist);

    ExpectSuccess(api.CreateWeakContainer(CT_NAME));

    ExpectSuccess(api.SetProperty(CT_NAME, "memory_limit", "20M"));
    ExpectSuccess(api.GetProperty(CT_NAME, "memory_limit", str));
    ExpectEq(str, "20971520");

    ExpectSuccess(api.SetInt(CT_NAME, "memory_limit", 10<<20));
    ExpectSuccess(api.GetInt(CT_NAME, "memory_limit", val));
    ExpectEq(val, 10485760);

    ExpectSuccess(api.SetLabel(CT_NAME, "TEST.a", "."));

    ExpectSuccess(api.GetContainerSpec(CT_NAME, ct));
    ExpectEq(ct.status().state(), "stopped");
    ExpectEq(ct.spec().memory_limit(), 10 << 20);

    ExpectSuccess(api.WaitContainer(CT_NAME, str));
    ExpectEq(str, "stopped");

    ExpectSuccess(api.CreateVolume(path, {
                {"containers", CT_NAME},
                {"backend", "native"},
                {"space_limit", "1G"}}));
    ExpectNeq(path, "");

    [[maybe_unused]] auto vd = api.GetVolumeDesc(path);
    Expect(vd != nullptr);
    ExpectEq(vd->path(), path);

    [[maybe_unused]] auto vs = api.GetVolume(path);
    Expect(vs != nullptr);
    ExpectEq(vs->path(), path);

    ExpectSuccess(api.SetProperty(CT_NAME, "command", "sleep 1000"));
    ExpectSuccess(api.Start(CT_NAME));

    ExpectSuccess(api.GetProperty(CT_NAME, "state", str));
    ExpectEq(str, "running");

    ExpectSuccess(api.Destroy(CT_NAME));

    TMap<TString, uint64_t> values;
    auto CT_NAME_0 = CT_NAME + "abcd";
    auto CT_NAME_CHILD = CT_NAME + "/b";

    ExpectSuccess(api.Create(CT_NAME_0));
    ExpectSuccess(api.SetProperty(CT_NAME_0, "command", "sleep 15"));
    ExpectSuccess(api.Start(CT_NAME_0));

    ExpectSuccess(api.Create(CT_NAME));
    ExpectSuccess(api.SetProperty(CT_NAME, "command", "sleep 10"));
    ExpectSuccess(api.GetProcMetric(TVector<TString>{CT_NAME, CT_NAME_0}, "ctxsw", values));
    ExpectEq(values[CT_NAME], 0);
    ExpectNeq(values[CT_NAME_0], 0);

    ExpectSuccess(api.Start(CT_NAME));
    ExpectSuccess(api.GetProcMetric(TVector<TString>{CT_NAME}, "ctxsw", values));
    ExpectNeq(values[CT_NAME], 0);

    ExpectSuccess(api.Create(CT_NAME_CHILD));
    ExpectSuccess(api.SetProperty(CT_NAME_CHILD, "command", "sleep 10"));
    ExpectSuccess(api.GetProcMetric(TVector<TString>{CT_NAME_CHILD}, "ctxsw", values));
    ExpectEq(values[CT_NAME_CHILD], 0);

    ExpectSuccess(api.Start(CT_NAME_CHILD));
    ExpectSuccess(api.GetProcMetric(TVector<TString>{CT_NAME, CT_NAME_CHILD}, "ctxsw", values));
    ExpectNeq(values[CT_NAME_CHILD], 0);
    Expect(values[CT_NAME] > values[CT_NAME_CHILD]);

    ExpectSuccess(api.Stop(CT_NAME_CHILD));
    ExpectSuccess(api.GetProcMetric(TVector<TString>{CT_NAME_CHILD}, "ctxsw", values));
    ExpectEq(values[CT_NAME_CHILD], 0);

    ExpectSuccess(api.Stop(CT_NAME));
    ExpectSuccess(api.GetProcMetric(TVector<TString>{CT_NAME}, "ctxsw", values));
    ExpectEq(values[CT_NAME], 0);

    ExpectSuccess(api.Destroy(CT_NAME_CHILD));
    ExpectSuccess(api.Destroy(CT_NAME));
    ExpectSuccess(api.Destroy(CT_NAME_0));

#ifdef __linux__
    // test TAsyncWaiter
    Porto::TAsyncWaiter waiter([](const TString &error, int ret) {
        Y_UNUSED(error);
        Y_UNUSED(ret);

        Expect(false);
    });

    TString result;
    waiter.Add("abc", "starting", [&result](const Porto::TWaitResponse &event) {
        result += event.name() + "-" + event.state();
    });

    TString name = "abcdef";
    ExpectSuccess(api.Create(name));
    ExpectSuccess(api.SetProperty(name, "command", "sleep 1"));
    ExpectSuccess(api.Start(name));
    ExpectSuccess(api.Destroy(name));
    ExpectEq(result, "");

    // callback work only once
    for (int i = 0; i < 3; i++) {
        name = "abc";
        ExpectSuccess(api.Create(name));
        ExpectSuccess(api.SetProperty(name, "command", "sleep 1"));
        ExpectSuccess(api.Start(name));
        ExpectSuccess(api.Destroy(name));
        ExpectEq(result, "abc-starting");
    }

    waiter.Add("abc", "starting", [&result](const Porto::TWaitResponse &event) {
        result += event.name() + "-" + event.state();
    });
    waiter.Remove("abc");

    name = "abc";
    ExpectSuccess(api.Create(name));
    ExpectSuccess(api.SetProperty(name, "command", "sleep 1"));
    ExpectSuccess(api.Start(name));
    ExpectSuccess(api.Destroy(name));
    ExpectEq(result, "abc-starting");
#endif

    api.Disconnect();
}

Y_UNIT_TEST_SUITE(Porto) {
    Y_UNIT_TEST(All) {
        test_porto();
    }
}
