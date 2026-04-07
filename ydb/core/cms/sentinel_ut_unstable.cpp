#include "sentinel_ut_helpers.h"

namespace NKikimr::NCmsTest {

Y_UNIT_TEST_SUITE(TSentinelUnstableTests) {
    Y_UNIT_TEST(BSControllerCantChangeStatus) {
        TTestEnv env(8, 4);

        const TPDiskID id1 = env.RandomPDiskID();
        const TPDiskID id2 = env.RandomPDiskID();
        const TPDiskID id3 = env.RandomPDiskID();

        for (size_t i = 0; i < sizeof(ErrorStates) / sizeof(ErrorStates[0]); ++i) {
            env.AddBSCFailures(id1, {true, false, false, true, false, false});
            // will fail for all requests assuming there is only 5 retries
            env.AddBSCFailures(id2, {false, false, false, false, false, false});
            env.AddBSCFailures(id3, {false, true, false, false, true, false});
        }

        for (const EPDiskState state : ErrorStates) {
            env.SetPDiskState({id1, id2, id3}, state, EPDiskStatus::FAULTY);
            env.SetPDiskState({id1, id2, id3}, NKikimrBlobStorage::TPDiskState::Normal, EPDiskStatus::ACTIVE);
        }
    }

    Y_UNIT_TEST(InsertOrAssignUpdatesRequestsDuringRetry) {
        NKikimrCms::TCmsConfig config;
        auto* sentinelConfig = config.MutableSentinelConfig();
        sentinelConfig->SetRetryChangeStatus(TDuration::MilliSeconds(100).GetValue());
        sentinelConfig->SetChangeStatusRetries(5);
        auto* stateLimit = sentinelConfig->AddStateLimits();
        stateLimit->SetState(static_cast<ui32>(ErrorStates[0]));
        stateLimit->SetLimit(1);

        TTestEnv env(8, 4, config);
        const TPDiskID id1 = env.RandomPDiskID();

        std::vector<EPDiskStatus> requestStatuses;
        bool pdiskStateChangedToNormal = false;

        TTestActorRuntimeBase::TEventObserver prevObserver = env.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::TEvControllerConfigRequest::EventType) {
                auto* msg = ev->Get<TEvBlobStorage::TEvControllerConfigRequest>();

                for (const auto& cmd : msg->Record.GetRequest().GetCommand()) {
                    if (cmd.HasUpdateDriveStatus()) {
                        TPDiskID id(cmd.GetUpdateDriveStatus().GetHostKey().GetNodeId(),
                                    cmd.GetUpdateDriveStatus().GetPDiskId());

                        if (id == id1) {
                            auto status = static_cast<EPDiskStatus>(cmd.GetUpdateDriveStatus().GetStatus());
                            requestStatuses.push_back(status);

                            if (!pdiskStateChangedToNormal) {
                                pdiskStateChangedToNormal = true;
                                env.SetPDiskStateImpl({id1}, NKikimrBlobStorage::TPDiskState::Normal);
                            }
                        }
                    }
                }
            }
            return prevObserver(ev);
        });

        env.AddBSCFailures(id1, {false});

        env.SetPDiskState({id1}, ErrorStates[0]);

        TDispatchOptions options;
        options.FinalEvents.emplace_back([&](IEventHandle&) {
            return requestStatuses.size() >= 2;
        });
        UNIT_ASSERT(env.DispatchEvents(options));

        UNIT_ASSERT_VALUES_EQUAL(requestStatuses.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(requestStatuses[0], EPDiskStatus::FAULTY);
        UNIT_ASSERT_VALUES_EQUAL(requestStatuses[1], EPDiskStatus::INACTIVE);
    }

}

}
