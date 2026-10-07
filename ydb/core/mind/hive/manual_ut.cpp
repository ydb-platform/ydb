#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <ydb/library/actors/core/executor_thread.h>

#include "ut_common.h"

using namespace NKikimr;
using namespace NHive;

Y_UNIT_TEST_SUITE(THiveManualTests) {
    Y_UNIT_TEST(ManyNodesManyTablets) {
        static constexpr size_t NUM_NODES = 20'000;
        static constexpr size_t NUM_TABLETS = 20'000;

        TMailbox mailbox;
        auto setup = MakeHolder<TActorSystemSetup>();
        TString name = "no name";
        TActorSystem as(setup, nullptr, new NLog::TSettings(TActorId(), 0, NKikimrServices::EServiceKikimr_MIN, NKikimrServices::EServiceKikimr_MAX, [&](auto&&) { return std::ref(name); }, NLog::EPriority::PRI_EMERG));
        TExecutorThread executor{0, &as, nullptr, "dummy"};
        TActorContext dummyContext(mailbox, executor, 0, TActorId());
        TlsActivationContext = &dummyContext; // all that so calls to logging do not segfault

        TIntrusivePtr<TTabletStorageInfo> hiveStorage = new TTabletStorageInfo;
        hiveStorage->TabletType = TTabletTypes::Hive;
        TTestHive hive(hiveStorage.Get(), TActorId());
        hive.UpdateConfig([](NKikimrConfig::THiveConfig& config) {
            config.SetMaxBootBatchSize(20'000);
        });

        hive.MakeNodes(NUM_NODES);
        hive.MakeTablets(NUM_TABLETS);

        NIceDb::TToughDb tough;
        NIceDb::TNiceDb db{tough}; // ignored anyway
        TSideEffects sideEffects;

        TProfileTimer timer;
        Cerr << "Start" << Endl;
        hive.ExecuteProcessBootQueue(db, sideEffects);
        Cerr << "Passed " << timer.Get().SecondsFloat() << Endl;
    }
}
