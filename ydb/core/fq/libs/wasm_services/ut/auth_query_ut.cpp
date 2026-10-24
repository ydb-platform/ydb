#include "query_helpers.h"

#include <ydb/udfs/wasm/sdk/services/auth.h>

#include <util/generic/scope.h>
using namespace NFq::NWasmServices::NTestHelpers;

namespace {
struct TAuthFixture {
    TTempFile Module{ MakeTempName() }, Manifest{ MakeTempName() }, Secrets{ MakeTempName() };
    THttpMock Issuer;

    TAuthFixture() {
        TFileOutput(Module.Name()).Write(NResource::Find("/auth_test.wasm"));
        TFileOutput(Manifest.Name()).Write(NResource::Find("/auth_test_manifest.json"));
        Secret("first-secret");
    }

    void Secret(TString value) {
        TFileOutput(Secrets.Name()).Write("{\"key\":\"" + value + "\"}");
    }

    void Configure(NFq::NConfig::TWasmServicesConfig& config) {
        auto* module = config.AddModules();
        module->SetModulePath(Module.Name());
        module->SetManifestPath(Manifest.Name());
        auto* provider = config.AddAuthProviders();
        provider->SetProvider("TEST_AUTH");
        provider->SetModule("TestAuth");
        provider->SetEndpoint(Issuer.Url());
        config.SetAuthSecretsFile(Secrets.Name());
    }

    auto Configure() {
        return [this](auto& config) {
            Configure(config);
        };
    }

    TBinding Bind(TBinding binding) {
        binding.Headers.clear();
        binding.AuthConnection = std::make_shared<TAuthConnection>();
        binding.AuthConnection->Provider = "TEST_AUTH";
        binding.AuthConnection->Parameters = R"({"secret":"key"})";
        return binding;
    }

    void Reply(size_t index, TString ticket, ui64 ttl = 60000) {
        NYdb::NWasm::NAuth::TResult header;
        header.TicketBytes = ticket.size();
        header.LifetimeMs = ttl;
        Issuer.State->Reply(index, TString(reinterpret_cast<const char*>(&header), sizeof(header)) + ticket, 200);
    }
};

void WaitRows(TQueryTransformEnv& env, size_t count) {
    Eventually([&] {
        std::lock_guard lock(env.Output->Mutex);
        return env.Output->Rows.size() == count;
    });
}
}   // namespace

Y_UNIT_TEST_SUITE(TWasmAuthQueryTest) {
    Y_UNIT_TEST(LocalAuthDependenciesAreExplicitAndNotExposedAsServices) {
        TAuthFixture auth;
        TTempFile sdkManifest(MakeTempName()), sdkBinary(MakeTempName());
        NJson::TJsonValue json;
        NJson::ReadJsonTree(NResource::Find("/auth_test_manifest.json"), &json, true);
        json["required_libraries"].AppendValue("sdk");
        TFileOutput(auth.Manifest.Name()).Write(NJson::WriteJson(json, false));
        TFileOutput(sdkManifest.Name()).Write(R"({"module_name":"sdk","module_type":"library","module_kind":"wasm"})");
        // Loading tests artifact plumbing, not execution of this stand-in library.
        TFileOutput(sdkBinary.Name()).Write(NResource::Find("/auth_test.wasm"));
        NFq::NConfig::TWasmServicesConfig config;
        config.SetEnabled(true);
        config.SetModuleSource(NFq::NConfig::TWasmServicesConfig::LOCAL_FILES);
        auth.Configure(config);
        UNIT_ASSERT_EXCEPTION(CreateServiceModuleSource(config), yexception);
        auto* library = config.AddModules();
        library->SetManifestPath(sdkManifest.Name());
        library->SetModulePath(sdkBinary.Name());
        auto local = CreateServiceModuleSource(config);
        UNIT_ASSERT(local->Describe().empty());
        const auto module = local->Load("TestAuth");
        UNIT_ASSERT_VALUES_EQUAL(module->Libraries.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(module->Libraries[0].Name, "sdk");
        UNIT_ASSERT(module->Libraries[0].Bytecode.ObjectCode);
        UNIT_ASSERT_EXCEPTION(local->Load("sdk"), yexception);
        // Store mode returns exactly the same dependency-bearing artifact.
        auto& catalog = GetWasmModuleCatalog();
        catalog.Register(module);
        Y_DEFER {
            catalog.Unregister("TestAuth");
        };
        NFq::NConfig::TWasmServicesConfig storeConfig;
        storeConfig.SetEnabled(true);
        auto store = CreateServiceModuleSource(storeConfig);
        UNIT_ASSERT(store->Load("TestAuth") == module);
    }

    Y_UNIT_TEST(RegistrationAndPrivateParameters) {
        TAuthFixture auth;
        NFq::NConfig::TWasmServicesConfig config;
        config.SetEnabled(true);
        config.SetModuleSource(NFq::NConfig::TWasmServicesConfig::LOCAL_FILES);
        auth.Configure(config);
        auto source = CreateServiceModuleSource(config);
        ValidateAuthProviders(config, *source);
        UNIT_ASSERT(source->Describe().empty());   // auth is never an SQL service
        auto binding = auth.Bind({});
        binding.AuthConnection->Identity = "execution/connection";
        auto prepared = PrepareAuthModule(config, *source, *binding.AuthConnection);
        UNIT_ASSERT_VALUES_EQUAL(prepared.ParametersJson, "{}");
        UNIT_ASSERT_VALUES_EQUAL(prepared.SecretReferences, (TVector<TString>{ "key" }));
        binding.AuthConnection->Provider = "unapproved";
        UNIT_ASSERT_EXCEPTION(PrepareAuthModule(config, *source, *binding.AuthConnection), yexception);
        *config.AddAuthProviders() = config.GetAuthProviders(0);
        UNIT_ASSERT_EXCEPTION(ValidateAuthProviders(config, *source), yexception);
    }
    Y_UNIT_TEST(RefreshReadsRotatedSecretAndDoesNotFallBackAfterRemoval) {
        TAuthFixture auth;
        THttpMock http;
        TGrpcMock grpc;
        TQueryTransformEnv env(auth.Bind(Bindings(http, grpc)[2]), 10000, 1, 0, 65536, 0, true, false, false, auth.Configure());
        env.Start({ 42 }, false);
        UNIT_ASSERT_VALUES_EQUAL(auth.Issuer.State->Wait()->Payload, "first-secret");
        auth.Reply(0, "first-ticket", 500);
        UNIT_ASSERT(HasHeader(*http.State->Wait(), "x-auth-test", "first-ticket"));
        http.State->Reply(0, R"({"id":42,"name":"Ada","score":97,"version":1})", 200);
        WaitRows(env, 1);
        Sleep(TDuration::MilliSeconds(550));
        auth.Secret("rotated-secret");
        env.Start({ 43 }, false);
        UNIT_ASSERT_VALUES_EQUAL(auth.Issuer.State->Wait(1)->Payload, "rotated-secret");
        auth.Reply(1, "second-ticket", 500);
        UNIT_ASSERT(HasHeader(*http.State->Wait(1), "x-auth-test", "second-ticket"));
        http.State->Reply(1, R"({"id":43,"name":"Ada","score":97,"version":1})", 200);
        WaitRows(env, 2);
        Sleep(TDuration::MilliSeconds(550));
        TFileOutput(auth.Secrets.Name()).Write("{}");
        const auto error = env.Setup.AsyncOutputPromises->Issue.GetFuture();
        env.Start({ 44 });
        UNIT_ASSERT(error.Wait(TDuration::Seconds(10)));
        UNIT_ASSERT(!error.GetValue().Empty());
        UNIT_ASSERT_VALUES_EQUAL(http.State->Count(), 2);
        UNIT_ASSERT_VALUES_EQUAL(auth.Issuer.State->Count(), 2);
        UNIT_ASSERT(!error.GetValue().ToString().Contains("secret"));
    }
    Y_UNIT_TEST(GrpcUsesProtectedHeaderAndCachesAcrossRows) {
        TAuthFixture auth;
        THttpMock http;
        TGrpcMock grpc;
        TQueryTransformEnv env(auth.Bind(Bindings(http, grpc)[3]), 10000, 1, 0, 65536, 0, true, false, false, auth.Configure());
        env.Start({ 42, 42 });
        auth.Issuer.State->Wait();
        auth.Reply(0, "grpc-ticket");
        for (size_t i = 0; i < 2; ++i) {
            UNIT_ASSERT(HasHeader(*grpc.State->Wait(i), "x-auth-test", "grpc-ticket"));
            grpc.State->Reply(i, GrpcProfilePayload(), 0);
        }
        Eventually([&] {
            return env.Output->Finished.load();
        });
        UNIT_ASSERT_VALUES_EQUAL(auth.Issuer.State->Count(), 1);
        UNIT_ASSERT_VALUES_EQUAL(env.Output->Rows.size(), 2);
    }
    Y_UNIT_TEST(CheckpointWaitsForAuthAndRecoveryUsesFreshCache) {
        TAuthFixture auth;
        THttpMock http;
        TGrpcMock grpc;
        auto binding = auth.Bind(Bindings(http, grpc)[2]);
        {
            TQueryTransformEnv env(binding, 10000, 1, 0, 65536, 0, true, false, false, auth.Configure());
            env.Start({ 42 }, false);
            auth.Issuer.State->Wait();
            env.Output->Blocked = true;
            env.Setup.AsyncOutputWrite([](auto&) {
                return NKikimr::NMiniKQL::TUnboxedValueBatch{};
            }, NYql::NDq::CreateCheckpoint(1));
            {
                std::lock_guard lock(env.Output->Mutex);
                UNIT_ASSERT(env.Output->Events.empty());
            }
            auth.Reply(0, "before-checkpoint");
            http.State->Wait();
            http.State->Reply(0, R"({"id":42,"name":"Ada","score":97,"version":1})", 200);
            env.Setup.Execute([](auto&) {
            });
            {
                std::lock_guard lock(env.Output->Mutex);
                UNIT_ASSERT(env.Output->Events.empty());
            }
            env.Output->Blocked = false;
            env.Setup.Execute([](auto& actor) {
                actor.DqAsyncOutput->OnOutputConsumerReady();
            });
            Eventually([&] {
                std::lock_guard lock(env.Output->Mutex);
                return env.Output->Events.size() == 2;
            });
            UNIT_ASSERT_VALUES_EQUAL(env.Output->Events, (TVector<TString>{ "row:42", "checkpoint:1" }));
            env.Setup.Terminate();
        }
        auth.Secret("after-restart");
        TQueryTransformEnv restored(binding, 10000, 1, 0, 65536, 0, true, false, false, auth.Configure());
        restored.Setup.LoadSink({}, NYql::NDq::CreateCheckpoint(1));
        restored.Start({ 43 });
        UNIT_ASSERT_VALUES_EQUAL(auth.Issuer.State->Wait(1)->Payload, "after-restart");
        auth.Reply(1, "after-checkpoint");
        UNIT_ASSERT(HasHeader(*http.State->Wait(1), "x-auth-test", "after-checkpoint"));
        http.State->Reply(1, R"({"id":43,"name":"Ada","score":97,"version":1})", 200);
        Eventually([&] {
            return restored.Output->Finished.load();
        });
        UNIT_ASSERT_VALUES_EQUAL(restored.Output->Events, (TVector<TString>{ "row:43", "finish" }));
    }
}
