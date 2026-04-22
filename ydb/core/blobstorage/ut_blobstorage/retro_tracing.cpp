#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/common.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/ut_helpers.h>

#include <ydb/library/actors/retro_tracing/retro_collector.h>
#include <ydb/library/actors/retro_tracing/universal_span.h>
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
                for (const auto& span : uploader->Spans) {
                    if (span.name().find("DSProxy") != std::string::npos) {
                        ++dsproxySpans;
                    } else if (span.name().find("Backpressure") != std::string::npos) {
                        ++backpressureSpans;
                    }
                }
                return std::pair<ui32, ui32>{dsproxySpans, backpressureSpans};
            };

            ui8 verbosity = 1;
            ui32 ttl = Max<ui32>();
            auto [dsproxySpans1, backpressureSpans1] = countSpans();

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

            auto [dsproxySpans2, backpressureSpans2] = countSpans();

            UNIT_ASSERT_VALUES_UNEQUAL_C(dsproxySpans1, dsproxySpans2, uploader->PrintTraces());
            UNIT_ASSERT_VALUES_UNEQUAL_C(backpressureSpans1, backpressureSpans2, uploader->PrintTraces());
        }
    };

    Y_UNIT_TEST(Basics) {
        TTestCtx ctx;
        ctx.Initialize();
        ctx.Run();
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
}

