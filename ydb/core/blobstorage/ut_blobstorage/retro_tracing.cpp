#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/common.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/ut_helpers.h>

#include <ydb/library/actors/retro_tracing/retro_collector.h>

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
}
