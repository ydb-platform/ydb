#include <ydb/core/kqp/compute_actor/kqp_compute_events.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/engines/reader/actor/actor.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NOlap::NReader {

Y_UNIT_TEST_SUITE(TColumnShardScanActorTests) {
    class TIdleIterator: public TScanIteratorBase {
        TConclusionStatus Start() override {
            return TConclusionStatus::Success();
        }

        bool Finished() const override {
            return false;
        }

        TConclusion<std::unique_ptr<TPartialReadResult>> GetBatch() override {
            return std::unique_ptr<TPartialReadResult>();
        }
    };

    class TIdleReadMetadata: public TReadMetadataBase {
    public:
        TIdleReadMetadata()
            : TReadMetadataBase(nullptr, ERequestSorting::ASC, TProgramContainer(), nullptr, TSnapshot(1, 1), nullptr, /*tabletId*/ 1)
        {
        }

        std::unique_ptr<TScanIteratorBase> StartScan(const std::shared_ptr<TReadContext>& /*readContext*/) const override {
            return std::make_unique<TIdleIterator>();
        }

        std::vector<TNameTypeInfo> GetKeyYqlSchema() const override {
            return {};
        }
    };

    Y_UNIT_TEST(PoisonReportsFinishedToTablet) {
        TTestBasicRuntime runtime;
        runtime.Initialize(TAppPrepare().Unwrap());
        const TActorId tablet = runtime.AllocateEdgeActor();
        const TActorId compute = runtime.AllocateEdgeActor();
        const ui64 requestCookie = 7;

        const TActorId scan = runtime.Register(new TColumnShardScan(tablet, compute, /*scanDiagnosticsActorId*/ TActorId(),
            /*storagesManager*/ nullptr, /*dataAccessorsManager*/ nullptr, /*columnDataManager*/ nullptr, TComputeShardingPolicy(), /*scanId*/ 1,
            /*txId*/ 1, /*scanGen*/ 1, requestCookie, /*tabletId*/ 1, TDuration::Zero(), std::make_shared<TIdleReadMetadata>(),
            NKikimrDataEvents::FORMAT_ARROW, NColumnShard::TScanCounters(), NConveyorComposite::TCPULimitsConfig(),
            std::make_shared<NLWTrace::TOrbit>()));
        runtime.GrabEdgeEvent<NKqp::TEvKqpCompute::TEvScanInitActor>(compute);

        runtime.Send(new IEventHandle(scan, TActorId(), new NActors::TEvents::TEvPoison()));

        const auto finished = runtime.GrabEdgeEvent<NColumnShard::TEvPrivate::TEvReadFinished>(tablet);
        UNIT_ASSERT_VALUES_EQUAL(finished->Get()->RequestCookie, requestCookie);
    }
}

}   // namespace NKikimr::NOlap::NReader
