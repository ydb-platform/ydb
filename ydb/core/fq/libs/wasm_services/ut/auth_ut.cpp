#include "helpers.h"

#include <ydb/core/fq/libs/wasm_services/auth_cache.h>
#include <ydb/core/fq/libs/wasm_services/auth_module.h>

#include <ydb/udfs/wasm/sdk/services/auth.h>

using namespace NFq::NWasmServices::NTestHelpers;

namespace {
struct TLoader {
    struct TRequest {
        TString Key;
        TAuthCache::TCompletion Completion;
        bool Cancelled = false;
    };

    TVector<std::shared_ptr<TRequest>> Requests;
    TAuthCache Cache;

    explicit TLoader(TAuthCacheLimits limits = {})
        : Cache(
              [this](const TString& key, TInstant, TAuthCache::TCompletion completion) {
                  auto request = std::make_shared<TRequest>(TRequest{ key, std::move(completion) });
                  Requests.push_back(request);
                  return [request] {
                      request->Cancelled = true;
                  };
              }, limits)
    {
    }

    void Reply(size_t index, TString ticket = "ticket") {
        Requests.at(index)->Completion({ { ECredentialStatus::Ready, std::move(ticket) }, TInstant::Now() + TDuration::Hours(1) });
    }
};

TInstant Deadline() {
    return TInstant::Now() + TDuration::Minutes(1);
}
}   // namespace

Y_UNIT_TEST_SUITE(TWasmAuthCacheTest) {
    Y_UNIT_TEST(SharedRefreshIndependentCancellationAndCacheHit) {
        TLoader env;
        ui32 first = 0, second = 0, cached = 0;
        auto a = env.Cache.Request("binding", Deadline(), [&](auto) {
            ++first;
        });
        auto b = env.Cache.Request("binding", Deadline(), [&](auto r) {
            UNIT_ASSERT(r.Status == ECredentialStatus::Ready);
            ++second;
        });
        UNIT_ASSERT_VALUES_EQUAL(env.Requests.size(), 1);
        a->Cancel();
        UNIT_ASSERT(!env.Requests[0]->Cancelled);
        env.Reply(0);
        UNIT_ASSERT_VALUES_EQUAL(first, 0);
        UNIT_ASSERT_VALUES_EQUAL(second, 1);
        auto c = env.Cache.Request("binding", Deadline(), [&](auto r) {
            UNIT_ASSERT_VALUES_EQUAL(r.Ticket, "ticket");
            ++cached;
        });
        UNIT_ASSERT_VALUES_EQUAL(cached, 1);
        UNIT_ASSERT_VALUES_EQUAL(env.Requests.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(env.Cache.Stats().Waiters, 0);
    }

    Y_UNIT_TEST(LateRefreshCannotReplaceNewGeneration) {
        TLoader env;
        ui32 completed = 0, cancelledCompletions = 0;
        auto a = env.Cache.Request("binding", Deadline(), [&](auto) {
            ++cancelledCompletions;
        });
        a->Cancel();
        UNIT_ASSERT(env.Requests[0]->Cancelled);
        auto b = env.Cache.Request("binding", Deadline(), [&](auto r) {
            UNIT_ASSERT_VALUES_EQUAL(r.Ticket, "new");
            ++completed;
        });
        env.Reply(0, "old");
        UNIT_ASSERT_VALUES_EQUAL(completed, 0);
        env.Reply(1, "new");
        UNIT_ASSERT_VALUES_EQUAL(completed, 1);
        env.Reply(0, "late");
        auto c = env.Cache.Request("binding", Deadline(), [&](auto r) {
            UNIT_ASSERT_VALUES_EQUAL(r.Ticket, "new");
            ++completed;
        });
        UNIT_ASSERT_VALUES_EQUAL(completed, 2);
        UNIT_ASSERT_VALUES_EQUAL(cancelledCompletions, 0);
    }

    Y_UNIT_TEST(DeadlineAndShutdownDetachWithoutWaitingForLoader) {
        TLoader env;
        ui32 failed = 0, good = 0;
        const auto now = TInstant::Now();
        auto a = env.Cache.Request("binding", now + TDuration::Seconds(1), [&](auto r) {
            UNIT_ASSERT(r.Status == ECredentialStatus::Failed);
            ++failed;
        });
        auto b = env.Cache.Request("binding", now + TDuration::Seconds(10), [&](auto r) {
            UNIT_ASSERT(r.Status == ECredentialStatus::Ready);
            ++good;
        });
        env.Cache.Expire(now + TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(failed, 1);
        UNIT_ASSERT(!env.Requests[0]->Cancelled);
        env.Reply(0);
        UNIT_ASSERT_VALUES_EQUAL(good, 1);
        auto pending = env.Cache.Request("other", Deadline(), [&](auto r) {
            UNIT_ASSERT(r.Status == ECredentialStatus::Failed);
            ++failed;
        });
        env.Cache.Close();
        UNIT_ASSERT(env.Requests[1]->Cancelled);
        UNIT_ASSERT_VALUES_EQUAL(failed, 2);
        env.Reply(1);
        UNIT_ASSERT_VALUES_EQUAL(failed, 2);
        UNIT_ASSERT_VALUES_EQUAL(env.Cache.Stats().Bytes, 0);
        UNIT_ASSERT_VALUES_EQUAL(env.Cache.Stats().Entries, 0);
    }

    Y_UNIT_TEST(KeysAndCompartmentsAreIsolated) {
        TLoader one, two;
        TVector<std::shared_ptr<ICredentialRequest>> requests;
        for (const auto& key : { "source/dst/secret-a", "source/dst/secret-b", "source/other-dst/secret-a" }) {
            requests.push_back(one.Cache.Request(key, Deadline(), [](auto) {
            }));
        }
        requests.push_back(two.Cache.Request("source/dst/secret-a", Deadline(), [](auto) {
        }));
        UNIT_ASSERT_VALUES_EQUAL(one.Requests.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(two.Requests.size(), 1);
        one.Reply(0);
        UNIT_ASSERT_VALUES_EQUAL(two.Cache.Stats().Waiters, 1);
    }

    Y_UNIT_TEST(ExpiryInvalidTicketAndLimits) {
        TAuthCacheLimits limits;
        limits.MaxEntries = 1;
        limits.MaxWaiters = 2;
        TLoader env(limits);
        ui32 failed = 0;
        auto callback = [&](auto r) {
            if (r.Status == ECredentialStatus::Failed) {
                ++failed;
            }
        };
        auto a = env.Cache.Request("a", Deadline(), callback);
        auto b = env.Cache.Request("a", Deadline(), callback);
        auto rejected = env.Cache.Request("a", Deadline(), callback);
        auto other = env.Cache.Request("b", Deadline(), callback);
        UNIT_ASSERT_VALUES_EQUAL(failed, 2);
        env.Reply(0, "injected\r\nheader");
        UNIT_ASSERT_VALUES_EQUAL(failed, 4);
        UNIT_ASSERT_VALUES_EQUAL(env.Cache.Stats().Bytes, 0);
        auto expired = env.Cache.Request("a", Deadline(), callback);
        env.Requests[1]->Completion({ { ECredentialStatus::Ready, "expired" }, TInstant::Now() });
        UNIT_ASSERT_VALUES_EQUAL(failed, 5);
        auto valid = env.Cache.Request("a", Deadline(), callback);
        env.Reply(2);
        env.Cache.Expire(TInstant::Now() + TDuration::Hours(2));
        auto renewed = env.Cache.Request("a", Deadline(), callback);
        UNIT_ASSERT_VALUES_EQUAL(env.Requests.size(), 4);
        UNIT_ASSERT(env.Cache.Stats().Bytes <= limits.MaxBytes);
    }

    Y_UNIT_TEST(InlineCompletionAndReentrancy) {
        TAuthCache cache([](const TString&, TInstant, TAuthCache::TCompletion completion) {
            completion({ { ECredentialStatus::Ready, "inline" }, TInstant::Now() + TDuration::Minutes(1) });
            return [] {
            };
        });
        auto request = cache.Request("binding", Deadline(), [&](auto r) {
            UNIT_ASSERT_VALUES_EQUAL(r.Ticket, "inline");
            cache.Close();
        });
        UNIT_ASSERT_VALUES_EQUAL(cache.Stats().Bytes, 0);
    }
}

Y_UNIT_TEST_SUITE(TWasmAuthModuleTest) {
    Y_UNIT_TEST(SdkDependenciesAreOrderedRequiredAndIsolated) {
        const TString sdk = R"((module
            (import "env" "memory" (memory i64 32 2097152))
            (global $heap (mut i64) (i64.const 131072))
            (func $sbrk (export "sbrk") (param $size i64) (result i64)
                (local $old i64) (local.set $old (global.get $heap))
                (global.set $heap (i64.add (local.get $old) (local.get $size))) (local.get $old))
            (func (export "malloc") (param i64) (result i64) (call $sbrk (local.get 0)))
            (func (export "free") (param i64))
            (func (export "sdk_value") (result i32) (i32.const 42))))";
        const TString auxiliary = R"((module
            (import "env" "sdk_value" (func $value (result i32)))
            (func (export "answer") (result i32) (call $value))))";
        const TString auth = R"((module
            (import "env" "memory" (memory i64 8 2097152))
            (import "env" "answer" (func $answer (result i32)))
            (import "env" "WasmAsyncCallComplete" (func $complete (param i64 i64 i32)))
            (data (i64.const 4096) "\01\00\00\00\02\00\00\00\60\ea\00\00\00\00\00\00ok")
            (func (export "WasmAsyncAbiVersion") (result i32) (i32.const 1))
            (func (export "WasmAsyncCallStart") (param i64 i64) (result i64) (i64.const 1))
            (func (export "WasmAsyncCallPoll") (param i64)
                (if (i32.ne (call $answer) (i32.const 42)) (then unreachable))
                (call $complete (i64.const 4096) (i64.const 18) (i32.const 1)))
            (func (export "WasmAsyncCallCancel") (param i64))
            (func (export "WasmAsyncCallDrop") (param i64))))";
        auto compile = [](const TString& wat) {
            return MakeModuleBytecode(wat, CompileModuleObjectCode(wat, EBytecodeFormat::HumanReadable), EBytecodeFormat::HumanReadable);
        };
        auto artifact = std::make_shared<TWasmModuleArtifact>();
        artifact->Manifest = ParseManifest(R"({"module_name":"DependentAuth","module_type":"module","module_kind":"wasm",
            "required_libraries":["sdk","helpers"],
            "auth_provider":{"name":"TEST_AUTH","abi_version":1,"header":"x-auth-test","parameters":[]}})");
        artifact->ModuleName = artifact->Manifest.ModuleName;
        artifact->ModuleBytecode = compile(auth);
        // Deliberately reversed catalog order; declared order must win.
        artifact->Libraries = { { "helpers", compile(auxiliary) }, { "sdk", compile(sdk) } };
        auto parent = CreateRegistryCompartment({});
        TAuthModuleConfig config;
        config.Module = artifact;
        config.ParametersJson = "{}";
        config.ConnectionIdentity = "connection";
        config.Bootstrap.Endpoint = "http://localhost:1";
        auto module = std::make_unique<TAuthModule>(*parent, config, TestExecutor(), [] {
        });
        ui32 completed = 0;
        auto request = module->Request(Deadline(), [&](auto r) {
            UNIT_ASSERT_VALUES_EQUAL(r.Ticket, "ok");
            ++completed;
        });
        module->Process();
        UNIT_ASSERT_VALUES_EQUAL(completed, 1);
        UNIT_ASSERT(!parent->GetFunction("answer"));
        module->Close();
        // An incomplete catalog must fail before execution, without SDK fallback.
        artifact->Libraries.pop_back();
        UNIT_ASSERT_EXCEPTION(TAuthModule(*parent, config, TestExecutor(), [] {
        }), yexception);
    }

    Y_UNIT_TEST(AsyncSecretNetworkAndCompartmentCache) {
        auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        auto metrics = std::make_shared<TRequestMetrics>(counters);
        TExecutorFixture executor({}, metrics);
        THttpMock issuer;
        auto application = CreateRegistryCompartment({});
        auto artifact = std::make_shared<TWasmModuleArtifact>();
        artifact->Manifest = ParseManifest(NResource::Find("/auth_test_manifest.json"));
        artifact->ModuleName = artifact->Manifest.ModuleName;
        const auto bytes = NResource::Find("/auth_test.wasm");
        artifact->ModuleBytecode = MakeModuleBytecode(bytes, CompileModuleObjectCode(bytes, EBytecodeFormat::Binary), EBytecodeFormat::Binary);
        ITransport::TCompletion secretCompletion;
        ui32 secretReads = 0;
        bool secretCancelled = false;
        TAuthModuleConfig config;
        config.Module = artifact;
        config.ParametersJson = "{}";
        config.SecretReferences = { "secret-reference" };
        config.ConnectionIdentity = "connection/identity";
        config.Bootstrap.Endpoint = issuer.Url();
        config.ResolveSecret = [&](const TString& ref, TInstant, auto completion) {
            UNIT_ASSERT_VALUES_EQUAL(ref, "secret-reference");
            ++secretReads;
            secretCompletion = std::move(completion);
            return [&] {
                secretCancelled = true;
            };
        };
        auto module = std::make_shared<TAuthModule>(*application, config, executor.Executor, [] {
        });
        ui32 completed = 0;
        auto done = [&](auto r) {
            UNIT_ASSERT_VALUES_EQUAL(r.Ticket, "fresh-ticket");
            ++completed;
        };
        auto first = module->Request(Deadline(), done);
        auto second = module->Request(Deadline(), done);
        module->Process();
        UNIT_ASSERT_VALUES_EQUAL(secretReads, 1);
        UNIT_ASSERT_VALUES_EQUAL(issuer.State->Count(), 0);
        UNIT_ASSERT_VALUES_EQUAL(completed, 0);
        secretCompletion(EOperationStatus::Ready, "new-secret-value");
        module->Process();
        UNIT_ASSERT_VALUES_EQUAL(issuer.State->Wait()->Payload, "new-secret-value");
        NYdb::NWasm::NAuth::TResult result;
        result.TicketBytes = 12;
        result.LifetimeMs = 60000;
        issuer.State->Reply(0, TString(reinterpret_cast<const char*>(&result), sizeof(result)) + "fresh-ticket", 200);
        Eventually([&] {
            module->Process();
            return completed == 2;
        });
        auto cached = module->Request(Deadline(), done);
        UNIT_ASSERT_VALUES_EQUAL(completed, 3);
        UNIT_ASSERT_VALUES_EQUAL(secretReads, 1);
        UNIT_ASSERT_VALUES_EQUAL(issuer.State->Count(), 1);
        // The same provider feeds the existing native application transport.
        THttpMock service;
        TBinding binding;
        binding.Endpoint = service.Url();
        binding.AuthProvider = module;
        binding.AuthHeader = "x-auth-test";
        binding.AuthAlias = "auth.test";
        TTransport transport({ binding }, executor.Executor);
        std::promise<EOperationStatus> finished;
        auto future = finished.get_future();
        auto cancel = transport.Start(
            1, EOperationKind::Request, MakeRequest(0, "user-body", "POST", "/call", "app.call"), Deadline(), [&](auto status, TString) {
                finished.set_value(status);
                return true;
            });
        const auto sent = service.State->Wait();
        UNIT_ASSERT(HasHeader(*sent, "x-auth-test", "fresh-ticket"));
        UNIT_ASSERT_VALUES_EQUAL(sent->Payload, "user-body");
        UNIT_ASSERT_VALUES_EQUAL(issuer.State->Count(), 1);
        service.State->Reply(0, "answer", 200);
        UNIT_ASSERT(future.wait_for(std::chrono::seconds(10)) == std::future_status::ready);
        UNIT_ASSERT(future.get() == EOperationStatus::Ready);
        cancel();
        Eventually([&] {
            return metrics->Get("auth.test", EMetricKind::Http)->InFlight->Val() == 0 &&
                   metrics->Get("auth.test", EMetricKind::Internal)->InFlight->Val() == 0;
        });
        UNIT_ASSERT_VALUES_EQUAL(metrics->Get("auth.test", EMetricKind::Http)->Started->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(metrics->Get("auth.test", EMetricKind::Internal)->Started->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(metrics->Get("app.call", EMetricKind::Http)->Started->Val(), 1);
        UNIT_ASSERT(secretCancelled);   // Ready secret operation also releases its owner.
        module->Close();
        UNIT_ASSERT_VALUES_EQUAL(module->Stats().Bytes, 0);
        // A callback retained by the resolver is harmless after realm teardown.
        UNIT_ASSERT(!secretCompletion(EOperationStatus::Ready, "late-secret"));
    }
}
