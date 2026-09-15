#include <ydb/core/base/blobstorage.h>
#include <ydb/core/tablet/tablet_setup.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/hash_set.h>

namespace NKikimr {

using namespace NColumnShard;
using namespace NTxUT;

namespace {

constexpr ui32 ChannelsCount = 5;
constexpr ui64 TableId = 1;

// First-boot history: every channel has a single entry starting at generation 0.
TIntrusivePtr<TTabletStorageInfo> MakeOneEntryInfo(ui64 tabletId) {
    auto info = MakeIntrusive<TTabletStorageInfo>();
    info->TabletID = tabletId;
    info->TabletType = TTabletTypes::ColumnShard;
    info->Channels.resize(ChannelsCount);
    for (ui32 ch = 0; ch < ChannelsCount; ++ch) {
        info->Channels[ch].Channel = ch;
        info->Channels[ch].Type = TBlobStorageGroupType(TBlobStorageGroupType::ErasureNone);
        info->Channels[ch].History.emplace_back(0u, 0u);
    }
    return info;
}

// Reboot history: [0, secondFromGeneration) becomes a past entry and the second entry is active.
TIntrusivePtr<TTabletStorageInfo> MakeTwoEntryInfo(ui64 tabletId, ui32 secondFromGeneration) {
    auto info = MakeOneEntryInfo(tabletId);
    for (ui32 ch = 0; ch < ChannelsCount; ++ch) {
        info->Channels[ch].History.emplace_back(secondFromGeneration, 0u);
    }
    return info;
}

// Boots a ColumnShard without a bootstrapper; the launcher receives the tablet's TEvCutTabletHistory.
TActorId BootTablet(TTestBasicRuntime& runtime, TIntrusivePtr<TTabletStorageInfo> info, const TActorId& launcher) {
    auto setup = MakeIntrusive<TTabletSetupInfo>(&CreateColumnShard, TMailboxType::Simple, 0u, TMailboxType::Simple, 0u);
    const TActorId actorId = runtime.Register(CreateTablet(launcher, info.Get(), setup.Get(), 0), 0);
    TDispatchOptions opts;
    opts.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot, 1));
    runtime.DispatchEvents(opts);
    runtime.SimulateSleep(TDuration::MilliSeconds(200));
    return actorId;
}

// Poisons the tablet and reboots it so that everything written up to firstGeneration sits in a past history entry.
void RebootWithOldEntry(TTestBasicRuntime& runtime, const TActorId& tabletActorId, const ui32 firstGeneration, const TActorId& launcher) {
    runtime.Send(new IEventHandle(tabletActorId, tabletActorId, new TEvents::TEvPoison()), 0);
    runtime.SimulateSleep(TDuration::Seconds(1));
    BootTablet(runtime, MakeTwoEntryInfo(TTestTxConfig::TxTablet0, firstGeneration + 1), launcher);
}

void DriveWakeups(TTestBasicRuntime& runtime, TActorId sender, const int count, const bool& stop) {
    for (int i = 0; i < count && !stop; ++i) {
        runtime.AdvanceCurrentTime(TDuration::Seconds(2));
        Wakeup(runtime, sender, TTestTxConfig::TxTablet0);
        runtime.SimulateSleep(TDuration::MilliSeconds(200));
    }
}

class TCutHistoryTabletController: public NYDBTest::NColumnShard::TController {
public:
    bool IsCSCutHistoryEnabled() const override {
        return true;
    }
};

// The generation the tablet writes in during its first boot, and the data channels (>= 2) it puts blobs into.
struct TWrittenBlobs {
    ui32 Generation = 0;
    THashSet<ui32> DataChannels;
};

void ConfigureCutter(TTestBasicRuntime& runtime) {
    runtime.GetAppData().ColumnShardConfig.SetCutHistoryMeasureOnly(false);
    runtime.GetAppData().ColumnShardConfig.SetCutHistoryNominateCadenceSeconds(1);
}

}   // anonymous namespace

Y_UNIT_TEST_SUITE(TCutHistoryTablet) {
    // An old entry that holds live portion blobs must not be cut.
    Y_UNIT_TEST(LiveDataInOldGroupIsNotCut) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryTabletController>();
        const TActorId launcher = runtime.AllocateEdgeActor();
        ConfigureCutter(runtime);

        TWrittenBlobs written;
        TActorId tabletActorId;
        {
            auto putObserver = runtime.AddObserver<TEvBlobStorage::TEvPut>([&](TEvBlobStorage::TEvPut::TPtr& ev) {
                const TLogoBlobID& id = ev->Get()->Id;
                if (id.TabletID() == TTestTxConfig::TxTablet0) {
                    written.Generation = Max(written.Generation, id.Generation());
                    if (id.Channel() >= 2) {
                        written.DataChannels.insert(id.Channel());
                    }
                }
            });
            tabletActorId = BootTablet(runtime, MakeOneEntryInfo(TTestTxConfig::TxTablet0), launcher);
            TActorId sender = runtime.AllocateEdgeActor();
            Y_UNUSED(SetupSchema(runtime, sender, TableId));
            TestTableDescription table;
            std::vector<ui64> writeIds;
            UNIT_ASSERT(WriteData(
                runtime, sender, TTestTxConfig::TxTablet0, 1, TableId, MakeTestBlob({ 0, 1000 }, table.Schema), table.Schema, &writeIds));
            const auto planStep = ProposeCommit(runtime, sender, TTestTxConfig::TxTablet0, 1, writeIds);
            PlanCommit(runtime, sender, TTestTxConfig::TxTablet0, planStep, TSet<ui64>{ 1 });
            for (int i = 0; i < 10; ++i) {
                runtime.SimulateSleep(TDuration::Seconds(2));
            }
        }
        UNIT_ASSERT_C(!written.DataChannels.empty(), "the write must put portion blobs into data channels");

        RebootWithOldEntry(runtime, tabletActorId, written.Generation, launcher);

        THashSet<ui32> cutChannels;
        auto cutObserver = runtime.AddObserver<TEvTablet::TEvCutTabletHistory>([&](TEvTablet::TEvCutTabletHistory::TPtr& ev) {
            if (ev->Get()->Record.GetFromGeneration() == 0) {
                cutChannels.insert(ev->Get()->Record.GetChannel());
            }
        });
        const bool neverStop = false;
        DriveWakeups(runtime, runtime.AllocateEdgeActor(), 25, neverStop);
        for (const ui32 channel : written.DataChannels) {
            UNIT_ASSERT_C(!cutChannels.contains(channel), "an old entry holding live portion blobs was cut, channel " << channel);
        }
    }

    // An old entry that holds no data is cut once the first GC round of the new incarnation commits.
    Y_UNIT_TEST(EmptyOldEntryIsCutAfterFirstGcRound) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryTabletController>();
        const TActorId launcher = runtime.AllocateEdgeActor();
        ConfigureCutter(runtime);

        TWrittenBlobs written;
        TActorId tabletActorId;
        {
            auto putObserver = runtime.AddObserver<TEvBlobStorage::TEvPut>([&](TEvBlobStorage::TEvPut::TPtr& ev) {
                const TLogoBlobID& id = ev->Get()->Id;
                if (id.TabletID() == TTestTxConfig::TxTablet0) {
                    written.Generation = Max(written.Generation, id.Generation());
                }
            });
            tabletActorId = BootTablet(runtime, MakeOneEntryInfo(TTestTxConfig::TxTablet0), launcher);
            // The schema creates the index that background activities, and so the cutter, need; no data is written.
            TActorId sender = runtime.AllocateEdgeActor();
            Y_UNUSED(SetupSchema(runtime, sender, TableId));
        }
        UNIT_ASSERT_C(written.Generation > 0, "the first boot must write its log");

        RebootWithOldEntry(runtime, tabletActorId, written.Generation, launcher);

        bool sawOldEntryCut = false;
        auto cutObserver = runtime.AddObserver<TEvTablet::TEvCutTabletHistory>([&](TEvTablet::TEvCutTabletHistory::TPtr& ev) {
            if (ev->Get()->Record.GetFromGeneration() == 0 && ev->Get()->Record.GetChannel() >= 2) {
                sawOldEntryCut = true;
            }
        });
        DriveWakeups(runtime, runtime.AllocateEdgeActor(), 60, sawOldEntryCut);
        UNIT_ASSERT_C(sawOldEntryCut, "an empty old entry must be cut after the first GC round commits");
    }
}

}   // namespace NKikimr
