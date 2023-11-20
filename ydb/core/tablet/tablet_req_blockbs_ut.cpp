#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tablet/tablet_impl.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TBlockBlobStorageTest) {

    Y_UNIT_TEST(DelayedErrorsNotIgnored) {
        TTestBasicRuntime runtime;
        SetupTabletServices(runtime);

        THashSet<ui32> groups;
        TIntrusivePtr<TTabletStorageInfo> info = CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::Dummy);
        for (size_t channel = 0; channel < info->Channels.size(); ++channel) {
            // use non-existant groups for all channels >= 2
            if (channel >= 2) {
                info->Channels.at(channel).History.at(0).GroupID = ui32(channel);
            }
            groups.insert(info->Channels.at(channel).History.at(0).GroupID);
        }

        TVector<THolder<IEventHandle>> blocked;
        size_t passed = 0;
        auto blockErrors = [&](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvBlobStorage::TEvBlockResult::EventType: {
                    auto* msg = ev->Get<TEvBlobStorage::TEvBlockResult>();
                    auto target = ev->GetRecipientRewrite();
                    if (msg->Status != NKikimrProto::OK) {
                        Cerr << "... blocking block result " << msg->Status << " for " << target << Endl;
                        blocked.emplace_back(ev.Release());
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    Cerr << "... passing block result " << msg->Status << " for " << target << Endl;
                    ++passed;
                    break;
                }
                default: {
                    break;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        auto prevObserverFunc = runtime.SetObserverFunc(blockErrors);

        auto waitFor = [&](const auto& condition, const TString& description) {
            if (!condition()) {
                Cerr << "... waiting for " << description << Endl;
                TDispatchOptions options;
                options.CustomFinalCondition = [&]() {
                    return condition();
                };
                runtime.DispatchEvents(options);
                UNIT_ASSERT_C(condition(), "... failed to wait for " << description);
            }
        };

        TActorId owner = runtime.AllocateEdgeActor();
        runtime.Register(CreateTabletReqBlockBlobStorage(owner, info.Get(), 1, false));

        waitFor([&]{ return blocked.size() + passed >= groups.size(); }, "all block results");

        runtime.SetObserverFunc(prevObserverFunc);
        for (auto& ev : blocked) {
            runtime.Send(ev.Release(), 0, true);
        }

        auto ev = runtime.GrabEdgeEventRethrow<TEvTabletBase::TEvBlockBlobStorageResult>(owner);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrProto::NO_GROUP);
    }

} // Y_UNIT_TEST_SUITE(TBlockBlobStorageTest)

} // namespace NKikimr
