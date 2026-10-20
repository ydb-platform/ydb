#include "query_helpers.h"

using namespace NFq::NWasmServices::NTestHelpers;

Y_UNIT_TEST_SUITE(TFqWasmQueryTest) {
    Y_UNIT_TEST(QueryBatchSizeCheckpointAndRestore200Rows) {
        THttpMock http;
        TGrpcMock grpc;
        TVector<ui64> delivered;
        size_t requestIndex = 0;
        for (ui32 epoch = 0; epoch < 2; ++epoch) {
            TQueryTransformEnv env(Bindings(http, grpc)[2], 5000, 1, 0, 65536, 0, true, false, false, {}, 8);
            if (epoch) {
                env.Setup.LoadSink({}, NYql::NDq::CreateCheckpoint(1));
            }
            TVector<ui64> ids;
            for (ui64 i = 1000 + epoch * 100; i < 1100 + epoch * 100; ++i) {
                ids.push_back(i);
            }
            env.Output->BlockAfterRows = 1;
            env.Start(ids, false);
            env.Setup.AsyncOutputWrite([](auto&) {
                return NKikimr::NMiniKQL::TUnboxedValueBatch{};
            }, NYql::NDq::CreateCheckpoint(epoch + 1));
            for (size_t offset = 0; offset < ids.size(); offset += 8) {
                TVector<ui64> batch(ids.begin() + offset, ids.begin() + std::min(offset + 8, ids.size()));
                const auto request = http.State->Wait(requestIndex);
                NJson::TJsonValue payload;
                UNIT_ASSERT(NJson::ReadJsonTree(request->Payload, &payload));
                const auto& actual = payload["ids"].GetArraySafe();
                UNIT_ASSERT_VALUES_EQUAL(actual.size(), batch.size());
                for (size_t i = 0; i < batch.size(); ++i) {
                    UNIT_ASSERT_VALUES_EQUAL(actual[i].GetUIntegerSafe(), batch[i]);
                }
                http.State->Reply(requestIndex++, HttpBatchPayload(batch), 200);
                if (!offset) {
                    Eventually([&] {
                        return env.Output->Blocked.load();
                    });
                    {
                        std::lock_guard lock(env.Output->Mutex);
                        UNIT_ASSERT_VALUES_EQUAL(env.Output->Events, (TVector<TString>{ TStringBuilder() << "row:" << ids.front() }));
                    }
                    env.Output->BlockAfterRows = 0;
                    env.Output->Blocked = false;
                    env.Setup.Execute([](auto& actor) {
                        actor.DqAsyncOutput->OnOutputConsumerReady();
                    });
                }
            }
            Eventually([&] {
                std::lock_guard lock(env.Output->Mutex);
                return env.Output->Events.size() == 101;
            });
            {
                std::lock_guard lock(env.Output->Mutex);
                UNIT_ASSERT_VALUES_EQUAL(env.Output->Events.back(), TStringBuilder() << "checkpoint:" << epoch + 1);
                for (const auto& row : env.Output->Rows) {
                    delivered.push_back(row.Id);
                }
            }
            env.Setup.Execute([&](auto& actor) {
                actor.DqAsyncOutput->CommitState(NYql::NDq::CreateCheckpoint(epoch + 1));
            });
            env.Setup.Terminate();
        }
        UNIT_ASSERT_VALUES_EQUAL(delivered.size(), 200);
        for (size_t i = 0; i < delivered.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(delivered[i], 1000 + i);
        }
    }

    Y_UNIT_TEST(QueryBatchSizeOverridesDefaultAndByteLimitStillApplies) {
        for (const bool useStore : { false, true }) {
            for (const ui32 requested : { 1, 8, 64 }) {
                THttpMock http;
                TGrpcMock grpc;
                // A deliberately different default makes ignoring the SQL
                // setting observable in both directions.
                TQueryTransformEnv env(Bindings(http, grpc)[2], 5000, requested == 1 ? 64 : 1,
                    sizeof(TProfileBatchHeader) + 2 * sizeof(TProfileResult), 65536, 0, true, useStore, false, {}, requested);
                env.Start({ 42, 43, 44 });
                const TVector<TVector<ui64>> batches =
                    requested == 1 ? TVector<TVector<ui64>>{ { 42 }, { 43 }, { 44 } } : TVector<TVector<ui64>>{ { 42, 43 }, { 44 } };
                for (size_t i = 0; i < batches.size(); ++i) {
                    const auto request = http.State->Wait(i);
                    if (requested == 1) {
                        UNIT_ASSERT_VALUES_EQUAL(request->Payload, TStringBuilder() << "{\"id\":" << batches[i][0] << "}");
                        http.State->Reply(
                            i, TStringBuilder() << "{\"id\":" << batches[i][0] << ",\"name\":\"Ada\",\"score\":97,\"version\":1}", 200);
                    } else {
                        UNIT_ASSERT_VALUES_EQUAL(request->Payload, i == 0 ? "{\"ids\":[42,43]}" : "{\"ids\":[44]}");
                        http.State->Reply(i, HttpBatchPayload(batches[i]), 200);
                    }
                }
                Eventually([&] {
                    return env.Output->Finished.load();
                });
                std::lock_guard lock(env.Output->Mutex);
                UNIT_ASSERT_VALUES_EQUAL(env.Output->Rows.size(), 3);
                UNIT_ASSERT_VALUES_EQUAL(http.State->Count(), batches.size());
            }
        }
    }

    Y_UNIT_TEST(StreamingSlowV1DownstreamCheckpointAndRestore) {
        THttpMock http;
        TGrpcMock grpc;
        auto binding = Bindings(http, grpc)[2];
        TVector<ui64> checkpointedRows;
        bool repliedBeforeBarrier = false;
        auto drainRows = [&](TQueryTransformEnv& env) {
            env.Setup.Execute([&](auto& actor) {
                NYql::NDq::TDqSerializedBatch data;
                while (env.RealChannel->Pop(data)) {
                    auto* type = env.RealChannel->GetOutputType();
                    NKikimr::NMiniKQL::TUnboxedValueBatch rows(type);
                    NYql::NDq::TDqDataSerializer serializer(actor.TypeEnv, actor.HolderFactory, NYql::NDqProto::DATA_TRANSPORT_UV_PICKLE_1_0,
                        NKikimr::NMiniKQL::EValuePackerVersion::V0, NYql::DefaultDatumTestValidationMode);
                    serializer.Deserialize(std::move(data), type, rows);
                    rows.ForEachRow([&](const auto& row) {
                        checkpointedRows.push_back(row.GetElement(0).template Get<ui64>());
                    });
                }
            });
        };
        {
            TQueryTransformEnv env(binding, 5000, 1, 0, 65536, 0, true, false, true);
            env.Start({ 42 }, false);
            http.State->Wait();
            // DQ snapshots the upstream offset before injecting C. Its replay
            // starts at row 43; the downstream snapshot must contain only 42.
            env.Setup.AsyncOutputWrite([](auto&) {
                return NKikimr::NMiniKQL::TUnboxedValueBatch{};
            }, NYql::NDq::CreateCheckpoint(1));
            http.State->Reply(0, R"({"id":42,"name":"Ada","score":97,"version":1})", 200);
            Eventually([&] {
                bool ready = false;
                env.Setup.Execute([&](auto& actor) {
                    ready = env.RealChannel->GetValuesCount() == 1 && actor.DqAsyncOutput->GetFreeSpace() > 0;
                });
                return ready;
            });
            // Withhold Pop, as a peer with no network credit does. There is lots
            // of local capacity, but the pending barrier must block more output.
            env.Start({ 43 }, false);
            Eventually([&] {
                bool blocked = false;
                env.Setup.Execute([&](auto&) {
                    blocked = env.RealChannel->GetFillLevel() == NYql::NDq::HardLimit;
                });
                // Wait for an observable result, not a timeout asserting absence
                // of an RPC. If admission stayed open, complete the later call
                // before allowing the downstream to read its checkpoint.
                return blocked || http.State->Count() >= 2;
            });
            if (http.State->Count() >= 2) {
                repliedBeforeBarrier = true;
                http.State->Reply(1, R"({"id":43,"name":"Ada","score":97,"version":1})", 200);
                Eventually([&] {
                    bool ready = false;
                    env.Setup.Execute([&](auto&) {
                        ready = env.RealChannel->GetValuesCount() == 2;
                    });
                    return ready;
                });
            }
            drainRows(env);
            bool popped = false;
            env.Setup.Execute([&](auto& actor) {
                NYql::NDqProto::TCheckpoint checkpoint;
                popped = env.RealChannel->Pop(checkpoint) && checkpoint.GetId() == 1;
                actor.DqAsyncOutput->CommitState(NYql::NDq::CreateCheckpoint(1));
                actor.DqAsyncOutput->OnOutputConsumerReady();
            });
            UNIT_ASSERT(popped);
            // The peer is ready again: execution must resume without polling.
            http.State->Wait(1);
            // Crash with post-C work in flight. Only the saved upstream offset
            // and downstream rows survive; the transform has no private state.
            env.Setup.Terminate();
        }
        TQueryTransformEnv restored(binding, 5000, 1, 0, 65536, 0, true, false, true);
        restored.Setup.LoadSink({}, NYql::NDq::CreateCheckpoint(1));
        restored.Start({ 43 }, false);
        http.State->Wait(2);
        if (!repliedBeforeBarrier) {
            http.State->Reply(1, R"({"id":43,"name":"old","score":1,"version":1})", 200);
        }
        http.State->Reply(2, R"({"id":43,"name":"Ada","score":97,"version":1})", 200);
        Eventually([&] {
            bool ready = false;
            restored.Setup.Execute([&](auto&) {
                ready = restored.RealChannel->GetValuesCount() == 1;
            });
            return ready;
        });
        drainRows(restored);
        UNIT_ASSERT_VALUES_EQUAL_C(checkpointedRows, (TVector<ui64>{ 42, 43 }),
            "A post-checkpoint row must not be saved downstream and then applied again on source replay");
    }

    Y_UNIT_TEST(StreamingBarriersWaitForNetworkAndOutputBackpressure) {
        THttpMock http;
        TGrpcMock grpc;
        for (const auto bindingIndex : { 2, 3 }) {
            TQueryTransformEnv env(Bindings(http, grpc)[bindingIndex]);
            auto state = bindingIndex == 2 ? http.State : grpc.State;
            env.Start({ 42 }, false);
            state->Wait();
            env.Setup.Execute([](NYql::NDq::TFakeActor& actor) {
                NYql::NDqProto::TWatermark watermark;
                watermark.SetTimestampUs(123);
                actor.DqAsyncOutput->SendDataWithWatermark(
                    NKikimr::NMiniKQL::TUnboxedValueBatch{}, 0, watermark, NYql::NDq::CreateCheckpoint(7), false);
                UNIT_ASSERT_VALUES_EQUAL(actor.DqAsyncOutput->GetFreeSpace(), 0);
            });
            {
                std::lock_guard lock(env.Output->Mutex);
                UNIT_ASSERT(env.Output->Events.empty());
            }
            env.Output->Blocked = true;
            state->Reply(0, bindingIndex == 2 ? TString(R"({"id":42,"name":"Ada","score":97,"version":1})") : GrpcProfilePayload(),
                bindingIndex == 2 ? 200 : 0);
            // A blocked consumer cannot receive either rows or control records.
            env.Setup.Execute([](NYql::NDq::TFakeActor& actor) {
                actor.DqAsyncOutput->OnOutputConsumerReady();
            });
            {
                std::lock_guard lock(env.Output->Mutex);
                UNIT_ASSERT(env.Output->Events.empty());
            }
            env.Output->Blocked = false;
            env.Setup.Execute([](NYql::NDq::TFakeActor& actor) {
                actor.DqAsyncOutput->OnOutputConsumerReady();
            });
            Eventually([&] {
                std::lock_guard lock(env.Output->Mutex);
                return env.Output->Events.size() == 3;
            });
            env.Setup.Execute([](NYql::NDq::TFakeActor& actor) {
                UNIT_ASSERT(actor.DqAsyncOutput->GetFreeSpace() > 0);
                actor.DqAsyncOutput->CommitState(NYql::NDq::CreateCheckpoint(7));
                actor.DqAsyncOutput->CommitState(NYql::NDq::CreateCheckpoint(7));
            });
            env.Start({}, true);
            Eventually([&] {
                return env.Output->Finished.load();
            });
            // DQ also propagates subsequent checkpoints through finished branches.
            env.Setup.AsyncOutputWrite([](auto&) {
                return NKikimr::NMiniKQL::TUnboxedValueBatch{};
            }, NYql::NDq::CreateCheckpoint(8), true);
            Eventually([&] {
                std::lock_guard lock(env.Output->Mutex);
                return env.Output->Events.size() == 5;
            });
            std::lock_guard lock(env.Output->Mutex);
            UNIT_ASSERT_VALUES_EQUAL(
                env.Output->Events, (TVector<TString>{ "row:42", "watermark:123", "checkpoint:7", "finish", "checkpoint:8" }));
        }
    }

    Y_UNIT_TEST(StreamingZeroCheckpointAndFreshCompartmentOnReplay) {
        THttpMock http;
        TGrpcMock grpc;
        auto binding = Bindings(http, grpc)[2];
        {
            TQueryTransformEnv env(binding);
            env.Setup.AsyncOutputWrite([](auto&) {
                return NKikimr::NMiniKQL::TUnboxedValueBatch{};
            }, NYql::NDq::CreateCheckpoint(0));
            Eventually([&] {
                std::lock_guard lock(env.Output->Mutex);
                return env.Output->Events.size() == 1;
            });
            env.Start({ 42 }, false);
            http.State->Wait();
            env.Setup.AsyncOutputWrite([](auto&) {
                return NKikimr::NMiniKQL::TUnboxedValueBatch{};
            }, NYql::NDq::CreateCheckpoint(1));
            // Kill the compartment with a pending RPC and barrier.
            env.Setup.Terminate();
            std::lock_guard lock(env.Output->Mutex);
            UNIT_ASSERT_VALUES_EQUAL(env.Output->Events, (TVector<TString>{ "checkpoint:0" }));
        }
        TQueryTransformEnv restored(binding);
        restored.Setup.LoadSink({}, NYql::NDq::CreateCheckpoint(0));
        restored.Start({ 42 }, false);
        http.State->Wait(1);
        // The response to the previous execution cannot publish into the new one.
        http.State->Reply(0, R"({"id":42,"name":"old","score":1,"version":1})", 200);
        http.State->Reply(1, R"({"id":42,"name":"Ada","score":97,"version":1})", 200);
        restored.Setup.AsyncOutputWrite([](auto&) {
            return NKikimr::NMiniKQL::TUnboxedValueBatch{};
        }, NYql::NDq::CreateCheckpoint(1), true);
        Eventually([&] {
            return restored.Output->Finished.load();
        });
        std::lock_guard lock(restored.Output->Mutex);
        UNIT_ASSERT_VALUES_EQUAL(restored.Output->Rows.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(restored.Output->Rows[0].Score, 97);
        UNIT_ASSERT_VALUES_EQUAL(restored.Output->Events, (TVector<TString>{ "row:42", "checkpoint:1", "finish" }));
    }

    Y_UNIT_TEST(StoreAcceptsModulesLoadedAfterFactoryStartup) {
        THttpMock http;
        TGrpcMock grpc;
        for (const auto bindingIndex : { 2, 3 }) {
            auto binding = Bindings(http, grpc)[bindingIndex];
            TQueryTransformEnv env(binding, 5000, 1, 0, 65536, 0, true, true);
            NFq::NConfig::TWasmServicesConfig config;
            config.SetEnabled(true);   // Store is the ydbd default; no local paths.
            auto source = CreateServiceModuleSource(config);
            const auto first = source->Load(TString(ProfileTransformType));
            auto replacement = std::make_shared<NKikimr::NUdfStore::NWasm::TWasmModuleArtifact>(*first);
            NKikimr::NUdfStore::NWasm::GetWasmModuleCatalog().Register(replacement);
            UNIT_ASSERT(source->Load(TString(ProfileTransformType)) == replacement);
            // Running transforms retain their own loaded module, not a lookup of the
            // current name. Removal affects only future module acquisition.
            NKikimr::NUdfStore::NWasm::GetWasmModuleCatalog().Unregister(TString(ProfileTransformType));
            UNIT_ASSERT_EXCEPTION(source->Load(TString(ProfileTransformType)), yexception);
            UNIT_ASSERT(!CreateServiceGatewayFactory(config, TestServiceConnections())->IsKnownFunctionType(TString(ProfileTransformType)));
            env.Start();
            auto state = bindingIndex == 2 ? http.State : grpc.State;
            state->Wait();
            state->Reply(0, bindingIndex == 2 ? TString(R"({"id":42,"name":"Ada","score":97,"version":1})") : GrpcProfilePayload(),
                bindingIndex == 2 ? 200 : 0);
            Eventually([&] {
                return env.Output->Finished.load();
            });
            UNIT_ASSERT_VALUES_EQUAL(env.Output->Rows.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(env.Output->Rows[0].Id, 42);
            config.AddModules()->SetModulePath("must-not-be-read");
            UNIT_ASSERT_EXCEPTION(CreateServiceModuleSource(config), yexception);
        }
    }

    Y_UNIT_TEST(ServiceConnectionResolutionIsAsynchronousAndOwnsItsState) {
        TTempFile manifest(MakeTempName());
        TFileOutput(manifest.Name()).Write(NResource::Find("/ydb/udfs/wasm/profile/manifest.json"));
        NFq::NConfig::TWasmServicesConfig config;
        config.SetEnabled(true);
        config.SetModuleSource(NFq::NConfig::TWasmServicesConfig::LOCAL_FILES);
        auto* module = config.AddModules();
        module->SetModulePath("unused");
        module->SetManifestPath(manifest.Name());
        auto promise = NThreading::NewPromise<TResolvedServiceConnection>();
        auto factory = CreateServiceGatewayFactoryWithResolver(config, [promise](const TString& name) {
            UNIT_ASSERT_VALUES_EQUAL(name, "/Root/profiles");
            return promise.GetFuture();
        });
        auto gateway = factory->CreateDqFunctionGateway("WASM_PROFILE", {}, "/Root/profiles");
        auto pending = gateway->ResolveFunction({}, "Profile");
        UNIT_ASSERT(!pending.HasValue());
        gateway.reset();
        factory.Reset();
        promise.SetValue(TResolvedServiceConnection{ "/Root/profiles", "public settings" });
        const auto description = pending.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(description.Connection, "/Root/profiles");
        UNIT_ASSERT_VALUES_EQUAL(ServiceInvocationConnection(description.InvokeUrl), "/Root/profiles");
        UNIT_ASSERT_VALUES_EQUAL(ServiceInvocationSettings(description.InvokeUrl), "public settings");
    }

    Y_UNIT_TEST(ServiceRegistrySelectsModuleAndMethod) {
        TTempFile profile(MakeTempName()), echo(MakeTempName());
        TFileOutput(profile.Name()).Write(NResource::Find("/ydb/udfs/wasm/profile/manifest.json"));
        TFileOutput(echo.Name()).Write(NResource::Find("/ydb/udfs/wasm/echo/manifest.json"));
        NFq::NConfig::TWasmServicesConfig config;
        config.SetEnabled(true);
        config.SetModuleSource(NFq::NConfig::TWasmServicesConfig::LOCAL_FILES);
        for (const auto& manifest : { profile.Name(), echo.Name() }) {
            auto* module = config.AddModules();
            module->SetModulePath("unused");
            module->SetManifestPath(manifest);
        }
        const auto factory = CreateServiceGatewayFactory(config, TestServiceConnections());
        auto gateway = factory->CreateDqFunctionGateway("Echo", {}, "service");
        const auto description = gateway->ResolveFunction({}, "Length").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(description.Type, "Echo");
        UNIT_ASSERT_STRING_CONTAINS(description.InvokeUrl, "Length");
        UNIT_ASSERT_EXCEPTION(gateway->ResolveFunction({}, "Profile"), yexception);
        UNIT_ASSERT_EXCEPTION(factory->CreateDqFunctionGateway("Echo", {}, "missing"), yexception);
        const auto hidden = CreateServiceGatewayFactory(config, {});
        UNIT_ASSERT_EXCEPTION(hidden->CreateDqFunctionGateway("Echo", {}, "service"), yexception);
        auto wrongType = TestServiceConnections();
        wrongType.at("service").mutable_content()->mutable_setting()->mutable_monitoring();
        UNIT_ASSERT_EXCEPTION(CreateServiceGatewayFactory(config, wrongType)->CreateDqFunctionGateway("Echo", {}, "service"), yexception);
        UNIT_ASSERT(!description.InvokeUrl.Contains("http://"));
        config.MutableModules(1)->SetManifestPath(profile.Name());
        UNIT_ASSERT_EXCEPTION(CreateServiceGatewayFactory(config, TestServiceConnections()), yexception);
        config.MutableModules(1)->SetManifestPath(echo.Name());
        config.SetMaxBufferedBytes(1);
        UNIT_ASSERT_EXCEPTION(
            CreateServiceGatewayFactory(config, TestServiceConnections())->CreateDqFunctionGateway("Echo", {}, "service")->ResolveFunction({},
                "Echo"), yexception);
        config.SetMaxBufferedBytes(0);
        config.SetMaxBatchBytes(592);
        const auto limited = CreateServiceGatewayFactory(config, TestServiceConnections());
        UNIT_ASSERT(limited->CreateDqFunctionGateway("WASM_PROFILE", {}, "service")->ResolveFunction({}, "Profile").GetValueSync().Type ==
                    "WASM_PROFILE");
        UNIT_ASSERT_EXCEPTION(limited->CreateDqFunctionGateway("Echo", {}, "service")->ResolveFunction({}, "Echo"), yexception);
        config.SetMaxBatchBytes(0);
        config.SetModulePath("ambiguous");
        UNIT_ASSERT_EXCEPTION(CreateServiceGatewayFactory(config, TestServiceConnections()), yexception);
    }

    Y_UNIT_TEST(ExternalServiceValidationAndCredentialIsolation) {
        auto connection = TestServiceConnection("profiles");
        auto& service = *connection.mutable_content()->mutable_setting()->mutable_external_service();
        UNIT_ASSERT(NFq::ValidateExternalService(service, false).Empty());
        service.set_insecure(false);
        UNIT_ASSERT(!NFq::ValidateExternalService(service, false).Empty());
        service.set_endpoint("https://service.test");
        UNIT_ASSERT(NFq::ValidateExternalService(service, false).Empty());
        for (const auto& url : { "https://user:secret@service.test/profile", "https:///profile", "file:///etc/passwd",
                 "https://service.test/#fragment", "https://service.test/\r\n" }) {
            service.set_endpoint(url);
            UNIT_ASSERT_C(!NFq::ValidateExternalService(service, false).Empty(), url);
        }
        service.set_endpoint("https://service.test");
        service.mutable_auth()->mutable_current_iam();
        UNIT_ASSERT(!NFq::ValidateExternalService(service, true).Empty());
        UNIT_ASSERT_EXCEPTION(PrepareServiceConnection(connection, {}), yexception);
        FederatedQuery::ExternalService resolved;
        service.set_ca_certificate(TString(4096, 'A'));
        const auto prepared = PrepareServiceConnection(connection, "host-secret");
        UNIT_ASSERT(IsUtf(prepared));
        google::protobuf::StringValue envelope;
        envelope.set_value(prepared);
        google::protobuf::StringValue received;
        UNIT_ASSERT(received.ParseFromString(envelope.SerializeAsString()));
        UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(received.value(), &resolved).ok());
        UNIT_ASSERT_VALUES_EQUAL(resolved.ca_certificate(), service.ca_certificate());
        UNIT_ASSERT_VALUES_EQUAL(resolved.auth().token().token(), "host-secret");
        UNIT_ASSERT(service.auth().has_current_iam());
        service.mutable_auth()->mutable_token()->set_token("host-secret");
        (*service.mutable_headers())["X-Api-Key"] = "header-secret";
        const auto printed = NKikimr::SecureDebugString(connection);
        UNIT_ASSERT(!printed.Contains("host-secret"));
        UNIT_ASSERT(!printed.Contains("header-secret"));
        (*service.mutable_headers())["Authorization"] = "override";
        UNIT_ASSERT(!NFq::ValidateExternalService(service, false).Empty());
        service.clear_headers();
        service.mutable_auth()->mutable_service_account()->set_id("service-account");
        UNIT_ASSERT(!NFq::ValidateExternalService(service, false).Empty());
        service.mutable_auth()->mutable_none();
        service.set_protocol(FederatedQuery::ExternalService::GRPC);
        service.set_endpoint("localhost:443");
        UNIT_ASSERT(NFq::ValidateExternalService(service, false).Empty());
        service.set_endpoint("dns:///localhost:443");
        UNIT_ASSERT(!NFq::ValidateExternalService(service, false).Empty());
    }

    Y_UNIT_TEST(ServiceSingleModuleShorthandUsesAdjacentManifest) {
        TTempFile artifact(MakeTempName());
        TTempFile manifest(artifact.Name() + ".manifest.json");
        TFileOutput(manifest.Name()).Write(NResource::Find("/ydb/udfs/wasm/profile/manifest.json"));
        NFq::NConfig::TWasmServicesConfig config;
        config.SetEnabled(true);
        config.SetModuleSource(NFq::NConfig::TWasmServicesConfig::LOCAL_FILES);
        config.SetModulePath(artifact.Name());
        const auto factory = CreateServiceGatewayFactory(config, TestServiceConnections());
        UNIT_ASSERT_VALUES_EQUAL(
            factory->CreateDqFunctionGateway("WASM_PROFILE", {}, "service")->ResolveFunction({}, "Profile").GetValueSync().Type, "WASM_PROFILE");
    }

    Y_UNIT_TEST(DqBatchByteQuotaReservesOutputRows) {
        THttpMock http;
        TGrpcMock grpc;
        TQueryTransformEnv env(Bindings(http, grpc)[2], 5000, 2, 0, 10, 2 * sizeof(TProfileResult));
        env.Output->BlockAfterRows = 1;
        env.Setup.Execute([](NYql::NDq::TFakeActor& actor) {
            UNIT_ASSERT_VALUES_EQUAL(actor.DqAsyncOutput->GetFreeSpace(), 2);
        });
        env.Start({ 42, 43 }, false);
        http.State->Wait();
        http.State->Reply(0, HttpBatchPayload({ 42, 43 }), 200);
        Eventually([&] {
            return env.Output->Blocked.load();
        });
        env.Setup.Execute([](NYql::NDq::TFakeActor& actor) {
            UNIT_ASSERT_VALUES_EQUAL(actor.DqAsyncOutput->GetFreeSpace(), 1);
        });
        env.Start({ 44 });
        env.Output->BlockAfterRows = 0;
        env.Output->Blocked = false;
        env.Setup.Execute([](NYql::NDq::TFakeActor& actor) {
            actor.DqAsyncOutput->OnOutputConsumerReady();
        });
        UNIT_ASSERT_VALUES_EQUAL(http.State->Wait(1)->Payload, "{\"ids\":[44]}");
        http.State->Reply(1, HttpBatchPayload({ 44 }), 200);
        Eventually([&] {
            return env.Output->Finished.load();
        });
        std::lock_guard lock(env.Output->Mutex);
        UNIT_ASSERT_VALUES_EQUAL(env.Output->Rows.size(), 3);
    }

    Y_UNIT_TEST(DqBatchQuotaCountsActiveAndUndeliveredRows) {
        THttpMock http;
        TGrpcMock grpc;
        TQueryTransformEnv env(Bindings(http, grpc)[2], 5000, 2, 0, 5);
        env.Output->BlockAfterRows = 1;
        env.Start({ 42, 43, 44 }, false);
        UNIT_ASSERT_VALUES_EQUAL(http.State->Wait()->Payload, "{\"ids\":[42,43]}");
        env.Setup.Execute([](NYql::NDq::TFakeActor& actor) {
            UNIT_ASSERT_VALUES_EQUAL(actor.DqAsyncOutput->GetFreeSpace(), 2);
        });
        http.State->Reply(0, HttpBatchPayload({ 42, 43 }), 200);
        Eventually([&] {
            return env.Output->Blocked.load();
        });
        env.Setup.Execute([](NYql::NDq::TFakeActor& actor) {
            UNIT_ASSERT_VALUES_EQUAL(actor.DqAsyncOutput->GetFreeSpace(), 3);
        });
        env.Start({ 45, 46, 47 });
        UNIT_ASSERT_VALUES_EQUAL(http.State->Count(), 1);
        env.Output->BlockAfterRows = 0;
        env.Output->Blocked = false;
        env.Setup.Execute([](NYql::NDq::TFakeActor& actor) {
            actor.DqAsyncOutput->OnOutputConsumerReady();
        });
        UNIT_ASSERT_VALUES_EQUAL(http.State->Wait(1)->Payload, "{\"ids\":[44,45]}");
        http.State->Reply(1, HttpBatchPayload({ 44, 45 }), 200);
        UNIT_ASSERT_VALUES_EQUAL(http.State->Wait(2)->Payload, "{\"ids\":[46,47]}");
        http.State->Reply(2, HttpBatchPayload({ 46, 47 }), 200);
        Eventually([&] {
            return env.Output->Finished.load();
        });
        std::lock_guard lock(env.Output->Mutex);
        UNIT_ASSERT_VALUES_EQUAL(env.Output->Rows.size(), 6);
        for (size_t i = 0; i < env.Output->Rows.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(env.Output->Rows[i].Id, 42 + i);
        }
        UNIT_ASSERT_VALUES_EQUAL(http.State->Count(), 3);
    }

    Y_UNIT_TEST(DqHttpBatchSplitsAndHonorsBackpressure) {
        THttpMock http;
        TGrpcMock grpc;
        TQueryTransformEnv env(Bindings(http, grpc)[2], 5000, 2);
        env.Output->BlockAfterRows = 1;
        env.Start({ 42, 42, 43, 44, 45 });
        auto request = http.State->Wait();
        UNIT_ASSERT_VALUES_EQUAL(request->Payload, "{\"ids\":[42,42]}");
        UNIT_ASSERT(HasHeader(*request, "Authorization", "Bearer host-secret"));
        http.State->Reply(0, HttpBatchPayload({ 42, 42 }), 200);
        Eventually([&] {
            return env.Output->Blocked.load();
        });
        {
            std::lock_guard lock(env.Output->Mutex);
            UNIT_ASSERT_VALUES_EQUAL(env.Output->Rows.size(), 1);
        }
        UNIT_ASSERT_VALUES_EQUAL(http.State->Count(), 1);
        env.Output->BlockAfterRows = 0;
        env.Output->Blocked = false;
        env.Setup.Execute([](NYql::NDq::TFakeActor& actor) {
            actor.DqAsyncOutput->OnOutputConsumerReady();
        });
        UNIT_ASSERT_VALUES_EQUAL(http.State->Wait(1)->Payload, "{\"ids\":[43,44]}");
        http.State->Reply(1, HttpBatchPayload({ 43, 44 }), 200);
        UNIT_ASSERT_VALUES_EQUAL(http.State->Wait(2)->Payload, "{\"ids\":[45]}");
        http.State->Reply(2, HttpBatchPayload({ 45 }), 200);
        Eventually([&] {
            return env.Output->Finished.load();
        });
        std::lock_guard lock(env.Output->Mutex);
        UNIT_ASSERT_VALUES_EQUAL(env.Output->Rows.size(), 5);
        for (size_t i = 0; i < 5; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(env.Output->Rows[i].Id, (TVector<ui64>{ 42, 42, 43, 44, 45 })[i]);
        }
        UNIT_ASSERT_VALUES_EQUAL(http.State->Count(), 3);
    }

    Y_UNIT_TEST(DqGrpcBatchByteLimitAndFinalPartial) {
        THttpMock http;
        TGrpcMock grpc;
        TQueryTransformEnv env(Bindings(http, grpc)[3], 5000, 64, sizeof(TProfileBatchHeader) + 2 * sizeof(TProfileResult));
        env.Start({ 42, 43, 44, 45, 46 });
        const TVector<TVector<ui64>> batches{ { 42, 43 }, { 44, 45 }, { 46 } };
        for (size_t i = 0; i < batches.size(); ++i) {
            auto request = grpc.State->Wait(i);
            UNIT_ASSERT(HasHeader(*request, "authorization", "Bearer host-secret"));
            NFq::NWasmServices::NTest::ProfileBatchRequest parsed;
            UNIT_ASSERT(parsed.ParseFromString(request->Payload));
            UNIT_ASSERT_VALUES_EQUAL(parsed.ids_size(), batches[i].size());
            for (size_t j = 0; j < batches[i].size(); ++j) {
                UNIT_ASSERT_VALUES_EQUAL(parsed.ids(j), batches[i][j]);
            }
            grpc.State->Reply(i, GrpcBatchPayload(batches[i]), 0);
        }
        Eventually([&] {
            return env.Output->Finished.load();
        });
        std::lock_guard lock(env.Output->Mutex);
        UNIT_ASSERT_VALUES_EQUAL(env.Output->Rows.size(), 5);
        UNIT_ASSERT_VALUES_EQUAL(grpc.State->Count(), 3);
    }

    Y_UNIT_TEST(DqHttpInvalidBatchDoesNotPublishPartialRows) {
        const TVector<TString> replies{
            HttpBatchPayload({ 42 }),
            HttpBatchPayload({ 42, 43, 44 }),
            HttpBatchPayload({ 43, 42 }),
            "[{\"id\":42,\"name\":\"Ada\",\"score\":97,\"version\":1},"
            "{\"id\":43,\"name\":\"Ada\",\"score\":101,\"version\":1}]",
            "[{\"id\":42,\"name\":\"Ada\",\"score\":97,\"version\":1},{}]",
            TString(40000, 'x'),
        };
        for (const auto& reply : replies) {
            THttpMock http;
            TGrpcMock grpc;
            TQueryTransformEnv env(Bindings(http, grpc)[2], 5000, 2);
            const auto error = env.Setup.AsyncOutputPromises->Issue.GetFuture();
            env.Start({ 42, 43 });
            http.State->Wait();
            http.State->Reply(0, reply, 200);
            UNIT_ASSERT(error.Wait(TDuration::Seconds(10)));
            std::lock_guard lock(env.Output->Mutex);
            UNIT_ASSERT(env.Output->Rows.empty());
            UNIT_ASSERT(!env.Output->Finished);
        }
    }

    Y_UNIT_TEST(DqGrpcInvalidBatchDoesNotPublishPartialRows) {
        const TVector<TString> replies{
            GrpcBatchPayload({ 42 }),
            GrpcBatchPayload({ 42, 43, 44 }),
            GrpcBatchPayload({ 43, 42 }),
            GrpcBatchPayload({ 42, 43 }, "Ada", 101),
            GrpcBatchPayload({ 42, 43 }, TString("\xc0\x80", 2)),
            "not protobuf",
        };
        for (const auto& reply : replies) {
            THttpMock http;
            TGrpcMock grpc;
            TQueryTransformEnv env(Bindings(http, grpc)[3], 5000, 2);
            const auto error = env.Setup.AsyncOutputPromises->Issue.GetFuture();
            env.Start({ 42, 43 });
            grpc.State->Wait();
            grpc.State->Reply(0, reply, 0);
            UNIT_ASSERT(error.Wait(TDuration::Seconds(10)));
            std::lock_guard lock(env.Output->Mutex);
            UNIT_ASSERT(env.Output->Rows.empty());
        }
    }

    Y_UNIT_TEST(DqBatchMaximumSizeAndLargeIds) {
        for (const auto binding : { 2, 3 }) {
            THttpMock http;
            TGrpcMock grpc;
            TQueryTransformEnv env(Bindings(http, grpc)[binding], 5000, MaxProfileBatchRows);
            TVector<ui64> ids;
            for (ui32 i = 0; i < MaxProfileBatchRows; ++i) {
                ids.push_back(Max<ui64>() - i);
            }
            env.Start(ids);
            auto state = binding == 2 ? http.State : grpc.State;
            state->Wait();
            state->Reply(0, binding == 2 ? HttpBatchPayload(ids) : GrpcBatchPayload(ids), binding == 2 ? 200 : 0);
            Eventually([&] {
                return env.Output->Finished.load();
            });
            std::lock_guard lock(env.Output->Mutex);
            UNIT_ASSERT_VALUES_EQUAL(env.Output->Rows.size(), MaxProfileBatchRows);
            UNIT_ASSERT_VALUES_EQUAL(env.Output->Rows.back().Id, ids.back());
            UNIT_ASSERT_VALUES_EQUAL(state->Count(), 1);
        }
    }

    Y_UNIT_TEST(DqBatchDeadlineAndCancellation) {
        THttpMock http;
        TGrpcMock grpc;
        {
            TQueryTransformEnv env(Bindings(http, grpc)[2], 100, 2);
            const auto error = env.Setup.AsyncOutputPromises->Issue.GetFuture();
            env.Start({ 42, 43 });
            http.State->Wait();
            UNIT_ASSERT(error.Wait(TDuration::Seconds(10)));
            UNIT_ASSERT(!env.Output->Finished);
        }
        {
            TQueryTransformEnv env(Bindings(http, grpc)[3], 5000, 2);
            env.Start({ 42, 43 });
            grpc.State->Wait();
            env.Setup.Terminate();
            Eventually([&] {
                std::lock_guard lock(grpc.State->Mutex);
                return grpc.State->Requests[0]->Cancelled;
            });
            UNIT_ASSERT(!env.Output->Finished);
        }
    }

    Y_UNIT_TEST(DqBatchRejectsInvalidConfig) {
        TTempFile manifest(MakeTempName());
        TFileOutput(manifest.Name()).Write(NResource::Find("/ydb/udfs/wasm/profile/manifest.json"));
        NFq::NConfig::TWasmServicesConfig config;
        config.SetEnabled(true);
        config.SetModuleSource(NFq::NConfig::TWasmServicesConfig::LOCAL_FILES);
        auto* module = config.AddModules();
        module->SetModulePath("unused");
        module->SetManifestPath(manifest.Name());
        config.SetMaxBatchRows(MaxProfileBatchRows + 1);
        UNIT_ASSERT_EXCEPTION(CreateServiceGatewayFactory(config, TestServiceConnections("profiles")), yexception);
        config.SetMaxBatchRows(2);
        config.SetMaxBatchBytes(sizeof(TProfileBatchHeader) + sizeof(TProfileResult) - 1);
        UNIT_ASSERT_EXCEPTION(CreateServiceGatewayFactory(config, TestServiceConnections("profiles"))
                                  ->CreateDqFunctionGateway("WASM_PROFILE", {}, "profiles")
                                  ->ResolveFunction({}, "Profile"), yexception);
        config.SetMaxBatchBytes(MaxProfileBatchBytes + 1);
        UNIT_ASSERT_EXCEPTION(CreateServiceGatewayFactory(config, TestServiceConnections("profiles")), yexception);
    }

    Y_UNIT_TEST(DqProfileHttpBackpressureAndTypedRows) {
        THttpMock http;
        TGrpcMock grpc;
        TQueryTransformEnv env(Bindings(http, grpc)[2]);
        env.Output->BlockAfterRows = 1;
        env.Start({ 42, 43 });
        UNIT_ASSERT_VALUES_EQUAL(http.State->Wait()->Payload, "{\"id\":42}");
        http.State->Reply(0, "{\"id\":42,\"name\":\"Ada\",\"score\":97,\"version\":1}", 200);
        Sleep(TDuration::MilliSeconds(50));
        UNIT_ASSERT_VALUES_EQUAL(http.State->Count(), 1);
        env.Output->BlockAfterRows = 0;
        env.Output->Blocked = false;
        env.Setup.Execute([](NYql::NDq::TFakeActor& actor) {
            actor.DqAsyncOutput->OnOutputConsumerReady();
        });
        UNIT_ASSERT_VALUES_EQUAL(http.State->Wait(1)->Payload, "{\"id\":43}");
        http.State->Reply(1, "{\"id\":43,\"name\":\"Grace\",\"score\":99,\"version\":1}", 200);
        Eventually([&] {
            return env.Output->Finished.load();
        });
        std::lock_guard lock(env.Output->Mutex);
        UNIT_ASSERT_VALUES_EQUAL(env.Output->Rows.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(env.Output->Rows[1].Id, 43);
    }

    Y_UNIT_TEST(DqProfileGrpcTypedRow) {
        THttpMock http;
        TGrpcMock grpc;
        TQueryTransformEnv env(Bindings(http, grpc)[3]);
        env.Start();
        auto request = grpc.State->Wait();
        UNIT_ASSERT(HasHeader(*request, "authorization", "Bearer host-secret"));
        grpc.State->Reply(0, GrpcProfilePayload(), 0);
        Eventually([&] {
            return env.Output->Finished.load();
        });
        std::lock_guard lock(env.Output->Mutex);
        UNIT_ASSERT_VALUES_EQUAL(env.Output->Rows.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(env.Output->Rows[0].Id, 42);
    }

    Y_UNIT_TEST(DqProfileDeadlineAndMalformedResponse) {
        THttpMock http;
        TGrpcMock grpc;
        {
            TQueryTransformEnv env(Bindings(http, grpc)[2], 100);
            const auto error = env.Setup.AsyncOutputPromises->Issue.GetFuture();
            env.Start();
            http.State->Wait();
            UNIT_ASSERT(error.Wait(TDuration::Seconds(10)));
            UNIT_ASSERT(!error.GetValue().Empty());
        }
        {
            TQueryTransformEnv env(Bindings(http, grpc)[2]);
            const auto error = env.Setup.AsyncOutputPromises->Issue.GetFuture();
            env.Start();
            http.State->Wait(1);
            http.State->Reply(1, "not json", 200);
            UNIT_ASSERT(error.Wait(TDuration::Seconds(10)));
            UNIT_ASSERT_STRING_CONTAINS(error.GetValue().ToString(), "service failed");
            UNIT_ASSERT(!env.Output->Finished);
        }
    }

    Y_UNIT_TEST(DqProfileCancellationCancelsNativeGrpc) {
        THttpMock http;
        TGrpcMock grpc;
        TQueryTransformEnv env(Bindings(http, grpc)[3]);
        env.Start();
        grpc.State->Wait();
        env.Setup.Terminate();
        Eventually([&] {
            std::lock_guard lock(grpc.State->Mutex);
            return grpc.State->Requests[0]->Cancelled;
        });
        UNIT_ASSERT(!env.Output->Finished);
    }

    Y_UNIT_TEST(DqConnectionHttpTlsPemRoots) {
        THttpTlsMock http;
        TBinding binding;
        binding.Endpoint = http.Url();
        binding.CaCertificate = http.Ca.Certificate;
        TQueryTransformEnv env(binding);
        env.Start();
        http.ReplyOnce("{\"id\":42,\"name\":\"Ada\",\"score\":97,\"version\":1}");
        Eventually([&] {
            return env.Output->Finished.load();
        });
        UNIT_ASSERT_VALUES_EQUAL(env.Output->Rows.size(), 1);
    }

    Y_UNIT_TEST(DqConnectionGrpcTlsPemRoots) {
        using namespace NKikimr::NCertTestUtils;
        const auto ca = GenerateCA(TProps::AsCA().WithValid(TDuration::Days(1)));
        const auto cert = GenerateSignedCert(ca, TProps::AsServer().WithValid(TDuration::Days(1)));
        grpc::SslServerCredentialsOptions options;
        grpc::SslServerCredentialsOptions::PemKeyCertPair keyCert;
        keyCert.private_key = cert.PrivateKey;
        keyCert.cert_chain = cert.Certificate;
        options.pem_key_cert_pairs.push_back(std::move(keyCert));
        TGrpcMock grpc(grpc::SslServerCredentials(options));
        THttpMock http;
        auto binding = Bindings(http, grpc)[3];
        binding.Endpoint = TStringBuilder() << "localhost:" << TStringBuf(binding.Endpoint).RNextTok(':');
        binding.CaCertificate = ca.Certificate;
        TQueryTransformEnv env(binding, 5000, 1, 0, 65536, 0, false);
        env.Start();
        grpc.State->Wait();
        grpc.State->Reply(0, GrpcProfilePayload(), 0);
        Eventually([&] {
            return env.Output->Finished.load();
        });
        UNIT_ASSERT_VALUES_EQUAL(env.Output->Rows.size(), 1);
    }
}
