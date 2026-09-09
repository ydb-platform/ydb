#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/schemeshard/schemeshard__stats.h>
#include <ydb/core/tx/schemeshard/schemeshard_private.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

using namespace NKikimr;
using namespace NKikimr::NSchemeShard;

Y_UNIT_TEST_SUITE(TStatsParserActor) {

    // Round-trips a raw TEvPeriodicTableStats through the aux actor: it must come back wrapped
    // as TEvPeriodicTableStatsParsed with the original datashard Sender intact, since
    // VerifySplitAndRequestStats reads it off the inner handle.
    Y_UNIT_TEST(ParsesAndBouncesBackPreservingSender) {
        TTestActorRuntime runtime;
        runtime.Initialize(TAppPrepare().Unwrap());
        const TActorId schemeShard = runtime.AllocateEdgeActor();
        const TActorId parserId = runtime.Register(CreateStatsParserActor(schemeShard));

        const TActorId datashard = runtime.AllocateEdgeActor();
        constexpr ui64 datashardId = 12345;
        constexpr ui64 tableOwnerId = 1;
        constexpr ui64 tableLocalId = 2;

        runtime.Send(parserId, datashard, new TEvDataShard::TEvPeriodicTableStats(datashardId, tableOwnerId, tableLocalId));

        auto handle = runtime.GrabEdgeEventRethrow<TEvPrivate::TEvPeriodicTableStatsParsed>(schemeShard);
        auto* parsed = handle->Get();

        UNIT_ASSERT(parsed->Ev);
        UNIT_ASSERT_VALUES_EQUAL(parsed->Ev->Sender, datashard);
        UNIT_ASSERT_VALUES_EQUAL(parsed->Ev->Get()->Record.GetDatashardId(), datashardId);
    }

    // Die() sends this actor a poison pill like every other schemeshard aux actor; a tracked
    // send after that must bounce back as undelivered, not silently vanish into a dead mailbox.
    Y_UNIT_TEST(DiesOnPoisonPill) {
        TTestActorRuntime runtime;
        runtime.Initialize(TAppPrepare().Unwrap());
        const TActorId schemeShard = runtime.AllocateEdgeActor();
        const TActorId parserId = runtime.Register(CreateStatsParserActor(schemeShard));

        const TActorId sender = runtime.AllocateEdgeActor();
        runtime.Send(parserId, sender, new TEvents::TEvPoison());

        runtime.Send(new IEventHandle(parserId, sender,
            new TEvDataShard::TEvPeriodicTableStats(12345, 1, 2),
            IEventHandle::FlagTrackDelivery));

        runtime.GrabEdgeEventRethrow<TEvents::TEvUndelivered>(sender);
    }
}
