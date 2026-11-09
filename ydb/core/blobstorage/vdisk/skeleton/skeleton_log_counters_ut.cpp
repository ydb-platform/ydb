#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_recoverylogwriter.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>

#include <library/cpp/monlib/dynamic_counters/encode.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/str.h>
#include <util/string/split.h>

namespace NKikimr {
namespace {

    class TLogWriterLauncher : public TActorBootstrapped<TLogWriterLauncher> {
        const TActorId Edge;
        const TIntrusivePtr<NMonitoring::TDynamicCounters> Counters;
        TActorId& Writer;

    public:
        TLogWriterLauncher(TActorId edge, TIntrusivePtr<NMonitoring::TDynamicCounters> counters,
                TActorId& writer)
            : Edge(edge)
            , Counters(std::move(counters))
            , Writer(writer)
        {}

        void Bootstrap(const TActorContext& ctx) {
            Writer = ctx.Register(CreateRecoveryLogWriter(Edge, Edge, NPDisk::TOwner(1), 1, 0, Counters));
            ctx.Send(Edge, new TEvents::TEvWakeup);
            PassAway();
        }
    };

} // namespace

Y_UNIT_TEST_SUITE(TVDiskRecoveryLogCounters) {
    Y_UNIT_TEST(AggregatesIncludeSingleAndBatchedRecords) {
        for (bool extended : {false, true}) {
            TTestBasicRuntime runtime;
            TAppPrepare app;
            app.FeatureFlags.SetExtendedVDiskCounters(extended);
            runtime.Initialize(app.Unwrap());
            auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
            const TActorId edge = runtime.AllocateEdgeActor();
            TActorId writer;
            runtime.Register(new TLogWriterLauncher(edge, counters, writer));
            runtime.GrabEdgeEventRethrow<TEvents::TEvWakeup>(edge);

            auto makeLog = [](TLogSignature::E signature, ui64 lsn) {
                return MakeHolder<NPDisk::TEvLog>(NPDisk::TOwner(1), 1, TLogSignature(signature),
                    TRcBuf::Copy(TString("record")), TLsnSeg(lsn, lsn), nullptr);
            };
            auto single = makeLog(TLogSignature::SignatureBlock, 1);
            ui64 expectedBytes = single->ApproximateSize();
            runtime.Send(new IEventHandle(writer, edge, single.Release()));
            runtime.GrabEdgeEventRethrow<NPDisk::TEvLog>(edge);

            auto batch = MakeHolder<NPDisk::TEvMultiLog>();
            for (ui64 lsn : {2, 3}) {
                auto log = makeLog(TLogSignature::SignatureGC, lsn);
                expectedBytes += log->ApproximateSize();
                batch->AddLog(std::move(log));
            }
            runtime.Send(new IEventHandle(writer, edge, batch.Release()));
            runtime.GrabEdgeEventRethrow<NPDisk::TEvMultiLog>(edge);

            auto logrecs = counters->FindSubgroup("subsystem", "logrecs");
            UNIT_ASSERT_VALUES_EQUAL(logrecs->FindCounter("LogRecords")->Val(), 3);
            UNIT_ASSERT_VALUES_EQUAL(logrecs->FindCounter("LogBlockMsgs")->Val(), 1);
            UNIT_ASSERT_VALUES_EQUAL(logrecs->FindCounter("LogGCMsgs")->Val(), 2);
            const ui64 detailedBytes = logrecs->FindCounter("LogBlockBytes")->Val()
                + logrecs->FindCounter("LogGCBytes")->Val();
            UNIT_ASSERT_VALUES_EQUAL(detailedBytes, expectedBytes);
            auto bytes = counters->FindSubgroup("subsystem", "lsmhull")->FindCounter("LsmLogBytesWritten");
            UNIT_ASSERT_VALUES_EQUAL(bytes->Val(), expectedBytes);
            UNIT_ASSERT(bytes->ForDerivative());

            TStringStream output;
            auto encoder = NMonitoring::CreateEncoder(&output, NMonitoring::EFormat::PROMETHEUS);
            counters->Accept({}, {}, *encoder);
            size_t samples = 0;
            for (TStringBuf line : StringSplitter(output.Str()).Split('\n').SkipEmpty()) {
                samples += !line.StartsWith('#');
            }
            UNIT_ASSERT_VALUES_EQUAL(samples, extended ? 54 : 2);
        }
    }
}

} // namespace NKikimr
