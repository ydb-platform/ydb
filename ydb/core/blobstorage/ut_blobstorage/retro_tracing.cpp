#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/common.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/ut_helpers.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/vdisk_delay_emulator.h>

#include <ydb/library/actors/retro_tracing/collector/retro_collector.h>
#include <ydb/library/actors/retro_tracing/span/universal_span.h>
#include <ydb/core/retro_tracing_impl/spans/named_span.h>
#include <ydb/core/retro_tracing_impl/distributed_collector/distributed_retro_collector.h>
#include <ydb/core/base/services/blobstorage_service_id.h>

Y_UNIT_TEST_SUITE(BlobStorageRetroTracing) {
    struct TTestCtx : public TTestCtxBase {
        TTestCtx()
            : TTestCtxBase(TEnvironmentSetup::TSettings{
                .NodeCount = 9,
                .Erasure = TBlobStorageGroupType::ErasureMirror3dc,
                .StartFakeWilsonCollectors = true,
            })
        {}

        void Run() {
            NWilson::TFakeWilsonUploader* uploader = Env->FakeWilsonUploaders[Edge.NodeId()];
            UNIT_ASSERT(uploader);

            auto countSpans = [&] {
                ui32 dsproxySpans = 0;
                ui32 backpressureSpans = 0;
                ui32 vdiskSpans = 0;
                for (ui32 nodeId = 1; nodeId <= NodeCount; ++nodeId) {
                    NWilson::TFakeWilsonUploader* uploader = Env->FakeWilsonUploaders[nodeId];
                    for (const auto& span : uploader->Spans) {
                        if (span.name().find("DSProxy") != std::string::npos) {
                            ++dsproxySpans;
                        } else if (span.name().find("Backpressure") != std::string::npos) {
                            ++backpressureSpans;
                        } else if (span.name().find("VDisk") != std::string::npos) {
                            ++vdiskSpans;
                        }
                    }
                }
                return std::tuple<ui32, ui32, ui32>{dsproxySpans, backpressureSpans, vdiskSpans};
            };

            ui8 verbosity = 1;
            ui32 ttl = Max<ui32>();
            auto [dsproxySpans1, backpressureSpans1, vdiskSpans1] = countSpans();

            TString data = MakeData(1_MB);
            TLogoBlobID blobId(1, 1, 1, 1, data.size(), 123);
            Env->Runtime->WrapInActorContext(Edge, [&] {
                SendToBSProxy(Edge, GroupId, new TEvBlobStorage::TEvPut(blobId, data, TInstant::Max()),
                        0, NWilson::TTraceId::NewTraceId(verbosity, ttl, true));
            });
            auto res = Env->WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(Edge, false, TInstant::Max());
            UNIT_ASSERT(res);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);

            Env->Runtime->WrapInActorContext(Edge, [&] {
                NRetroTracing::DemandAllTraces();
            });
            Env->Sim(TDuration::Seconds(10));

            auto [dsproxySpans2, backpressureSpans2, vdiskSpans2] = countSpans();

            UNIT_ASSERT_VALUES_UNEQUAL_C(dsproxySpans1, dsproxySpans2, uploader->PrintTraces());
            UNIT_ASSERT_VALUES_UNEQUAL_C(backpressureSpans1, backpressureSpans2, uploader->PrintTraces());
            UNIT_ASSERT_VALUES_UNEQUAL_C(vdiskSpans1, vdiskSpans2, uploader->PrintTraces());
        }

        ui32 CountDSProxySpans() {
            ui32 count = 0;
            for (ui32 nodeId = 1; nodeId <= NodeCount; ++nodeId) {
                NWilson::TFakeWilsonUploader* uploader = Env->FakeWilsonUploaders[nodeId];
                if (!uploader) {
                    continue;
                }
                for (const auto& span : uploader->Spans) {
                    if (span.name().find("DSProxy") != std::string::npos) {
                        ++count;
                    }
                }
            }
            return count;
        }

        void DoPlainPut(ui64 cookie) {
            TString data = MakeData(1_MB);
            TLogoBlobID blobId(1, 1, 1, 1, data.size(), cookie);
            Env->Runtime->WrapInActorContext(Edge, [&] {
                SendToBSProxy(Edge, GroupId, new TEvBlobStorage::TEvPut(blobId, data, TInstant::Max()), 0);
            });
            auto res = Env->WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(Edge, false, TInstant::Max());
            UNIT_ASSERT(res);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
        }

        void DemandAllTracesAndCollect() {
            Env->Runtime->WrapInActorContext(Edge, [&] {
                NRetroTracing::DemandAllTraces();
            });
            Env->Sim(TDuration::Seconds(10));
        }
    };

    Y_UNIT_TEST(Basics) {
        TTestCtx ctx;
        ctx.Initialize();
        ctx.Run();
    }

    Y_UNIT_TEST(StorageGenerationControl) {
        TTestCtx ctx;
        ctx.Initialize();

        ctx.DoPlainPut(1);
        ctx.DemandAllTracesAndCollect();
        UNIT_ASSERT_VALUES_EQUAL_C(ctx.CountDSProxySpans(), 0u,
                "Expected no DSProxy retro spans while storage generation is disabled");

        ctx.Env->SetIcbControl(0, "RetroTracingControls.EnableStorageGeneration", 1);
        ctx.Env->Sim(TDuration::Seconds(20));

        ctx.DoPlainPut(2);
        ctx.DemandAllTracesAndCollect();
        UNIT_ASSERT_GT_C(ctx.CountDSProxySpans(), 0u,
                "Expected DSProxy retro spans after enabling storage generation");
    }

    ////////////////////////////////////////////////////////////////////////////
    // Distributed retro tracing test
    ////////////////////////////////////////////////////////////////////////////

    // Actor that writes retro spans with a given traceId to the thread-local span buffer.
    // Used to populate span buffers on specific nodes for the distributed collection test.
    class TRetroSpanWriterActor : public NActors::TActorBootstrapped<TRetroSpanWriterActor> {
    public:
        TRetroSpanWriterActor(const NWilson::TTraceId& traceId, const NActors::TActorId& edgeActorId)
            : TraceId(traceId)
            , EdgeActorId(edgeActorId)
        {}

        void Bootstrap() {
            const ui8 verbosity = 1;
            {
                NRetroTracing::TUniversalSpan<NKikimr::TNamedSpan> span(
                        verbosity, NWilson::TTraceId(TraceId), "TestDistributedSpan",
                        NWilson::EFlags::AUTO_END);
                span.GetRetroSpanPtr()->SetName("TestDistributedSpan");
            }
            Send(EdgeActorId, new NActors::TEvents::TEvGone);
            PassAway();
        }

    private:
        NWilson::TTraceId TraceId;
        NActors::TActorId EdgeActorId;
    };

    Y_UNIT_TEST(DistributedDemandTrace) {
        const ui32 nodeCount = 9;
        TEnvironmentSetup env(TEnvironmentSetup::TSettings{
            .NodeCount = nodeCount,
            .Erasure = TBlobStorageGroupType::ErasureMirror3dc,
            .SelfManagementConfig = true,
            .StartFakeWilsonCollectors = true,
        });

        env.Sim(TDuration::Seconds(10));

        const ui8 verbosity = 1;
        const ui32 ttl = Max<ui32>();
        NWilson::TTraceId traceId = NWilson::TTraceId::NewTraceId(verbosity, ttl, true);
        UNIT_ASSERT(traceId);
        UNIT_ASSERT(traceId.IsRetroTrace());

        for (ui32 nodeId : env.Runtime->GetNodes()) {
            const TActorId edge = env.Runtime->AllocateEdgeActor(nodeId, __FILE__, __LINE__);
            env.Runtime->Register(new TRetroSpanWriterActor(traceId, edge), nodeId);
            auto ev = env.WaitForEdgeActorEvent<NActors::TEvents::TEvGone>(edge);
            UNIT_ASSERT(ev);
        }

        auto countRetroSpans = [&]() -> ui32 {
            ui32 total = 0;
            for (auto& [nodeId, uploader] : env.FakeWilsonUploaders) {
                for (const auto& span : uploader->Spans) {
                    for (const auto& attr : span.attributes()) {
                        if (attr.key() == "type" && attr.value().string_value() == "RETRO") {
                            ++total;
                        }
                    }
                }
            }
            return total;
        };

        ui32 retroSpansBefore = countRetroSpans();

        {
            ui32 senderNodeId = *env.Runtime->GetNodes().begin();
            const TActorId edge = env.Runtime->AllocateEdgeActor(senderNodeId, __FILE__, __LINE__);
            env.Runtime->WrapInActorContext(edge, [&] {
                NRetroTracing::DemandTrace(traceId);
            });
        }

        env.Sim(TDuration::Seconds(10));

        ui32 retroSpansAfter = countRetroSpans();

        UNIT_ASSERT_C(retroSpansAfter > retroSpansBefore,
            "Expected RETRO spans to appear after DemandRetroTrace. "
            "Before: " << retroSpansBefore << ", After: " << retroSpansAfter);

        ui32 newRetroSpans = retroSpansAfter - retroSpansBefore;
        // in single-thread test environment thread_local span buffered is shared by all nodes
        // so we expect each span to be uploaded multiple times
        UNIT_ASSERT_VALUES_EQUAL(newRetroSpans, nodeCount * nodeCount);
    }

    Y_UNIT_TEST(SlowPutRequest) {
        const ui32 nodeCount = 12;
        TTestCtxBase ctx(TEnvironmentSetup::TSettings{
            .NodeCount = nodeCount,
            .Erasure = TBlobStorageGroupType::ErasureMirror3dc,
            .StartFakeWilsonCollectors = true,
        });

        ctx.CreateOneGroup();
        ctx.AllocateEdgeActor(/*findNodeWithoutVDisks=*/true);

        const std::set<ui32> storageNodes = ctx.GetNodesWithVDisks();
        const ui32 dynnodeId = ctx.Edge.NodeId();
        UNIT_ASSERT_C(!storageNodes.contains(dynnodeId),
                "Edge/DSProxy is expected to run on a VDisk-free dynamic node");

        ctx.Env->SetIcbControl(0, "RetroTracingControls.EnableStorageGeneration", 1);
        ctx.Env->SetIcbControl(0, "RetroTracingControls.EnableStorageCollectionSlowRequests", 1);
        ctx.Env->SetIcbControl(0, "DSProxyControls.LongRequestThresholdMs", 100);

        auto vdiskDelayEmulator = std::make_shared<TVDiskDelayEmulator>(ctx.Env);
        vdiskDelayEmulator->Edge = ctx.Edge;
        vdiskDelayEmulator->DefaultDelay = TDiskDelay(TDuration::Seconds(2));

        auto delayPutResult = [&](std::unique_ptr<IEventHandle>& ev) {
            if (ev->Sender.NodeId() < ctx.NodeCount) {
                vdiskDelayEmulator->DelayMsg(ev);
                return false;
            }
            return true;
        };
        vdiskDelayEmulator->AddHandler(TEvBlobStorage::TEvVPutResult::EventType, delayPutResult);
        vdiskDelayEmulator->AddHandler(TEvBlobStorage::TEvVMultiPutResult::EventType, delayPutResult);
        ctx.Env->Runtime->FilterFunction = TDelayFilterFunctor{ .VDiskDelayEmulator = vdiskDelayEmulator };

        ctx.Env->Sim(TDuration::Seconds(20));
        ctx.GetGroupStatus(ctx.GroupId);

        TString data = MakeData(1_MB);
        TLogoBlobID blobId(1, 1, 1, 1, data.size(), 1);
        ctx.Env->Runtime->WrapInActorContext(ctx.Edge, [&] {
            SendToBSProxy(ctx.Edge, ctx.GroupId,
                    new TEvBlobStorage::TEvPut(blobId, data, TInstant::Max()), 0);
        });
        auto res = ctx.Env->WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(ctx.Edge, false, TInstant::Max());
        UNIT_ASSERT(res);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);

        ctx.Env->Sim(TDuration::Seconds(30));

        auto countRetro = [&](ui32 nodeId) {
            ui32 total = 0;
            ui32 dsproxy = 0;
            ui32 vdisk = 0;
            ui32 backpressure = 0;
            NWilson::TFakeWilsonUploader* uploader = ctx.Env->FakeWilsonUploaders[nodeId];
            if (uploader) {
                for (const auto& span : uploader->Spans) {
                    bool isRetro = false;
                    for (const auto& attr : span.attributes()) {
                        if (attr.key() == "type" && attr.value().string_value() == "RETRO") {
                            isRetro = true;
                            break;
                        }
                    }
                    if (!isRetro) {
                        continue;
                    }
                    ++total;
                    if (span.name().find("DSProxy") != std::string::npos) {
                        ++dsproxy;
                    } else if (span.name().find("Backpressure") != std::string::npos) {
                        ++backpressure;
                    } else if (span.name().find("VDisk") != std::string::npos) {
                        ++vdisk;
                    }
                }
            }
            return std::tuple<ui32, ui32, ui32, ui32>{total, dsproxy, vdisk, backpressure};
        };

        {
            auto [total, dsproxy, vdisk, backpressure] = countRetro(dynnodeId);
            Y_UNUSED(vdisk, backpressure);
            UNIT_ASSERT_C(total > 0, "Dynamic node collector contains no RETRO spans");
            UNIT_ASSERT_C(dsproxy > 0, "Dynamic node collector is missing the DSProxy span");
        }

        for (ui32 nodeId : storageNodes) {
            auto [total, dsproxy, vdisk, backpressure] = countRetro(nodeId);
            Y_UNUSED(dsproxy, vdisk, backpressure);
            UNIT_ASSERT_C(total > 0, "Storage node " << nodeId << " collector contains no RETRO spans");
        }

        ui32 dsproxyAll = 0;
        ui32 vdiskAll = 0;
        ui32 backpressureAll = 0;
        for (const auto& [nodeId, uploader] : ctx.Env->FakeWilsonUploaders) {
            auto [total, dsproxy, vdisk, backpressure] = countRetro(nodeId);
            Y_UNUSED(total);
            dsproxyAll += dsproxy;
            vdiskAll += vdisk;
            backpressureAll += backpressure;
        }
        UNIT_ASSERT_C(dsproxyAll > 0, "No DSProxy retro spans were collected");
        UNIT_ASSERT_C(vdiskAll > 0, "No VDisk retro spans were collected");
        UNIT_ASSERT_C(backpressureAll > 0, "No Backpressure retro spans were collected");

        ctx.Env->Runtime->FilterFunction = {};
    }
}

