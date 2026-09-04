#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/load_test/service_actor.h>

#include <library/cpp/protobuf/util/pb_io.h>

#include <algorithm>
#include <utility>

struct TEvDelayedMessageWrapper : public TEventLocal<TEvDelayedMessageWrapper, TEvBlobStorage::EvDelayedMessageWrapper> {
    std::unique_ptr<IEventHandle> Event;

    explicit TEvDelayedMessageWrapper(std::unique_ptr<IEventHandle>& ev)
        : Event(ev.release())
    {}
};

struct TLoadScenarioSettings {
    bool SharedRequestDispatching = false;
    bool EnableReads = false;
    bool DelayPutResults = false;
    bool WaitForFinish = true;
    ui64 MinPutRequests = 0;
    ui64 MinGetRequests = 0;
    ui32 DurationSeconds = 4;
    ui32 MaxInFlightWriteRequests = 1;
    ui32 MaxInFlightReadRequests = 4;
    TDuration PutResultDelay = TDuration::Seconds(1);
    bool RequirePutsFromEveryGroup = false;
    bool RequireGetsFromEveryGroup = false;
};

struct TLoadRunMetrics {
    ui32 MaxInFlightTotal = 0;
    ui64 LoadPuts = 0;
    ui64 LoadGets = 0;
    TVector<ui64> LoadPutsByGroup;
    TVector<ui64> LoadGetsByGroup;
    TVector<ui32> MaxInFlightByGroup;
    bool InvalidAccounting = false;
};

bool HasRequestsForEachGroup(const TVector<ui64>& values) {
    for (ui64 value : values) {
        if (value == 0) {
            return false;
        }
    }
    return true;
}

TLoadRunMetrics RunLoadScenario(const TLoadScenarioSettings& settings) {
    TEnvironmentSetup env({
        .NodeCount = 8,
        .VDiskReplPausedAtStart = false,
        .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
    });

    env.CreateBoxAndPool(1, 2);
    env.Sim(TDuration::Minutes(1));

    auto groups = env.GetGroups();
    UNIT_ASSERT_VALUES_EQUAL(groups.size(), 2);

    auto groupInfo1 = env.GetGroupInfo(groups[0]);
    auto groupInfo2 = env.GetGroupInfo(groups[1]);
    auto vDiskActorId = groupInfo1->GetActorId(0);

    THashMap<ui64, ui32> tabletIdToGroupIdx;
    tabletIdToGroupIdx.emplace(1001, 0);
    tabletIdToGroupIdx.emplace(1002, 1);

    const TActorId edge = env.Runtime->AllocateEdgeActor(vDiskActorId.NodeId(), __FILE__, __LINE__);

    TLoadRunMetrics metrics;
    metrics.MaxInFlightByGroup.resize(groups.size());
    metrics.LoadPutsByGroup.resize(groups.size());
    metrics.LoadGetsByGroup.resize(groups.size());
    TVector<ui32> inFlightByGroup(groups.size(), 0);
    ui32 inFlightTotal = 0;

    auto previousFilter = env.Runtime->FilterFunction;
    env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
        bool unwrappedDelayed = false;
        if (ev->GetTypeRewrite() == TEvDelayedMessageWrapper::EventType) {
            std::unique_ptr<IEventHandle> delayed(std::move(ev));
            ev.reset(delayed->Get<TEvDelayedMessageWrapper>()->Event.release());
            unwrappedDelayed = true;
        }

        switch (ev->GetTypeRewrite()) {
            case TEvBlobStorage::EvPut: {
                const auto* put = ev->Get<TEvBlobStorage::TEvPut>();
                const auto it = tabletIdToGroupIdx.find(put->Id.TabletID());
                if (it != tabletIdToGroupIdx.end() && put->Id.Step() != Max<ui32>()) {
                    const ui32 groupIdx = it->second;
                    ++metrics.LoadPuts;
                    ++metrics.LoadPutsByGroup[groupIdx];
                    ++inFlightByGroup[groupIdx];
                    ++inFlightTotal;
                    metrics.MaxInFlightByGroup[groupIdx] = std::max(metrics.MaxInFlightByGroup[groupIdx], inFlightByGroup[groupIdx]);
                    metrics.MaxInFlightTotal = std::max(metrics.MaxInFlightTotal, inFlightTotal);
                }
                break;
            }

            case TEvBlobStorage::EvPutResult: {
                const auto* res = ev->Get<TEvBlobStorage::TEvPutResult>();
                const auto it = tabletIdToGroupIdx.find(res->Id.TabletID());
                if (it != tabletIdToGroupIdx.end() && res->Id.Step() != Max<ui32>()) {
                    if (settings.DelayPutResults && !unwrappedDelayed) {
                        env.Runtime->WrapInActorContext(edge, [&] {
                            TActivationContext::Schedule(settings.PutResultDelay, new IEventHandle(
                                ev->Sender,
                                ev->Recipient,
                                new TEvDelayedMessageWrapper(ev)
                            ));
                        });
                        return false;
                    }

                    const ui32 groupIdx = it->second;
                    if (inFlightByGroup[groupIdx] == 0 || inFlightTotal == 0) {
                        metrics.InvalidAccounting = true;
                    } else {
                        --inFlightByGroup[groupIdx];
                        --inFlightTotal;
                    }
                }
                break;
            }

            case TEvBlobStorage::EvGet: {
                const auto* get = ev->Get<TEvBlobStorage::TEvGet>();
                if (get->QuerySize) {
                    const auto& id = get->Queries[0].Id;
                    const auto it = tabletIdToGroupIdx.find(id.TabletID());
                    if (it != tabletIdToGroupIdx.end() && id.Step() != Max<ui32>()) {
                        ++metrics.LoadGets;
                        ++metrics.LoadGetsByGroup[it->second];
                    }
                }
                break;
            }

            default:
                break;
        }

        return previousFilter ? previousFilter(nodeId, ev) : true;
    };

    TIntrusivePtr<::NMonitoring::TDynamicCounters> counters = new ::NMonitoring::TDynamicCounters();
    const TActorId storageLoadActorId = env.Runtime->Register(NKikimr::CreateLoadTestActor(counters), TActorId(), 0, std::nullopt, 1);

    auto req = std::make_unique<TEvLoad::TEvLoadTestRequest>();
    TStringBuilder conf;
    conf << "StorageLoad: {\n";
    conf << "DurationSeconds: " << settings.DurationSeconds << "\n";
    conf << "Tablets: {\n";
    conf << "Tablets: { TabletId: 1001 Channel: 0 GroupId: " << ToString(groupInfo1->GroupID) << " Generation: 1 }\n";
    conf << "Tablets: { TabletId: 1002 Channel: 0 GroupId: " << ToString(groupInfo2->GroupID) << " Generation: 1 }\n";
    conf << "WriteSizes: { Weight: 1.0 Min: 4096 Max: 4096 }\n";
    conf << "WriteIntervals: { Weight: 1.0 Uniform: { MinUs: 1 MaxUs: 1 } }\n";
    conf << "MaxInFlightWriteRequests: " << settings.MaxInFlightWriteRequests << "\n";
    if (settings.EnableReads) {
        conf << "ReadIntervals: { Weight: 1.0 Uniform: { MinUs: 1000 MaxUs: 1000 } }\n";
        conf << "ReadSizes: { Weight: 1.0 Min: 1024 Max: 2048 }\n";
        conf << "MaxInFlightReadRequests: " << settings.MaxInFlightReadRequests << "\n";
        conf << "GetHandleClass: FastRead\n";
    }
    conf << "FlushIntervals: { Weight: 1.0 Uniform: { MinUs: 10000000 MaxUs: 10000000 } }\n";
    conf << "PutHandleClass: TabletLog\n";
    conf << "SharedRequestDispatching: " << (settings.SharedRequestDispatching ? "true" : "false") << "\n";
    conf << "}\n";
    conf << "}\n";

    auto confStream = TStringInput(conf);
    req->Record = ParseFromTextFormat<NKikimr::TEvLoadTestRequest>(confStream);

    env.Runtime->WrapInActorContext(edge, [&] {
        env.Runtime->Send(new IEventHandle(storageLoadActorId, edge, req.release()));
    });

    {
        auto res = env.WaitForEdgeActorEvent<TEvLoad::TEvLoadTestResponse>(edge, false, env.Now() + TDuration::Seconds(60));
        UNIT_ASSERT_C(res, "No TEvLoadTestResponse");
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Record.GetStatus(), 1);
    }

    if (!settings.WaitForFinish) {
        constexpr ui32 maxIterations = 100000;
        auto enoughRequestsObserved = [&] {
            return metrics.LoadPuts >= settings.MinPutRequests &&
                metrics.LoadGets >= settings.MinGetRequests &&
                (!settings.RequirePutsFromEveryGroup || HasRequestsForEachGroup(metrics.LoadPutsByGroup)) &&
                (!settings.RequireGetsFromEveryGroup || HasRequestsForEachGroup(metrics.LoadGetsByGroup));
        };
        for (ui32 i = 0; i < maxIterations && !enoughRequestsObserved(); ++i) {
            bool iteration = true;
            env.Runtime->Sim([&] {
                return std::exchange(iteration, false);
            });
        }

        env.Runtime->FilterFunction = previousFilter;
        env.Runtime->DestroyActor(storageLoadActorId);
        return metrics;
    }

    {
        auto res = env.WaitForEdgeActorEvent<TEvLoad::TEvNodeFinishResponse>(edge, false, env.Now() + TDuration::Seconds(180));
        UNIT_ASSERT_C(res, "No TEvNodeFinishResponse");
        UNIT_ASSERT_C(res->Get()->Record.GetSuccess(), "Load actor must finish successfully");
    }

    env.Sim(TDuration::Seconds(5));
    env.Runtime->FilterFunction = previousFilter;

    UNIT_ASSERT_C(!metrics.InvalidAccounting, "Invalid in-flight accounting observed");
    UNIT_ASSERT_VALUES_EQUAL(inFlightTotal, 0);
    for (ui32 value : inFlightByGroup) {
        UNIT_ASSERT_VALUES_EQUAL(value, 0);
    }

    return metrics;
}

void AssertEachGroupHasRequests(const TVector<ui64>& values, const TStringBuf requestType) {
    UNIT_ASSERT_C(HasRequestsForEachGroup(values),
        TStringBuilder() << "Expected " << requestType << " for every group");
    for (ui32 groupIdx = 0; groupIdx < values.size(); ++groupIdx) {
        UNIT_ASSERT_C(values[groupIdx] > 0,
            TStringBuilder() << "Expected " << requestType << " for group# " << groupIdx);
    }
}

Y_UNIT_TEST_SUITE(LoadActorSharedDispatching) {
    Y_UNIT_TEST(NonSharedWriteOnlyRespectsPerGroupInflightLimit) {
        TLoadScenarioSettings settings{
            .SharedRequestDispatching = false,
            .EnableReads = false,
            .DelayPutResults = true,
            .DurationSeconds = 4,
            .MaxInFlightWriteRequests = 1,
            .MaxInFlightReadRequests = 0,
            .PutResultDelay = TDuration::Seconds(1),
        };

        auto result = RunLoadScenario(settings);
        UNIT_ASSERT_VALUES_EQUAL(result.MaxInFlightByGroup.size(), 2);
        UNIT_ASSERT_C(result.LoadPuts > 0, "Expected puts in non-shared write-only mode");
        AssertEachGroupHasRequests(result.LoadPutsByGroup, "puts");
        UNIT_ASSERT_C(result.MaxInFlightByGroup[0] <= 1 && result.MaxInFlightByGroup[1] <= 1,
            "Per-group in-flight limit must be respected in non-shared mode");
        UNIT_ASSERT_C(result.MaxInFlightTotal >= 2,
            TStringBuilder() << "Expected total in-flight >= 2 in non-shared mode, got " << result.MaxInFlightTotal);
    }

    Y_UNIT_TEST(NonSharedMixedReadWriteModeWorks) {
        TLoadScenarioSettings settings{
            .SharedRequestDispatching = false,
            .EnableReads = true,
            .DelayPutResults = false,
            .WaitForFinish = false,
            .MinPutRequests = 10,
            .MinGetRequests = 2,
            .DurationSeconds = 5,
            .MaxInFlightWriteRequests = 4,
            .MaxInFlightReadRequests = 4,
            .PutResultDelay = TDuration::Zero(),
            .RequirePutsFromEveryGroup = true,
            .RequireGetsFromEveryGroup = true,
        };

        auto result = RunLoadScenario(settings);
        UNIT_ASSERT_C(result.LoadPuts >= settings.MinPutRequests,
            TStringBuilder() << "Expected at least " << settings.MinPutRequests
                             << " puts in non-shared mixed mode, got " << result.LoadPuts);
        UNIT_ASSERT_C(result.LoadGets >= settings.MinGetRequests,
            TStringBuilder() << "Expected at least " << settings.MinGetRequests
                             << " gets in non-shared mixed mode, got " << result.LoadGets);
        AssertEachGroupHasRequests(result.LoadPutsByGroup, "puts");
        AssertEachGroupHasRequests(result.LoadGetsByGroup, "gets");
    }

    Y_UNIT_TEST(SharedRequestDispatchingRespectsGlobalInflightLimit) {
        TLoadScenarioSettings perGroupSettings{
            .SharedRequestDispatching = false,
            .EnableReads = false,
            .DelayPutResults = true,
            .DurationSeconds = 4,
            .MaxInFlightWriteRequests = 1,
            .MaxInFlightReadRequests = 0,
            .PutResultDelay = TDuration::Seconds(1),
        };

        auto perGroup = RunLoadScenario(perGroupSettings);
        UNIT_ASSERT_VALUES_EQUAL(perGroup.MaxInFlightByGroup.size(), 2);
        UNIT_ASSERT_C(perGroup.LoadPuts > 0, "Expected puts in per-group mode");
        UNIT_ASSERT_C(perGroup.MaxInFlightByGroup[0] <= 1 && perGroup.MaxInFlightByGroup[1] <= 1,
            "Per-group in-flight limit must be respected per group");
        UNIT_ASSERT_C(perGroup.MaxInFlightTotal >= 2,
            TStringBuilder() << "Expected total in-flight >= 2 in per-group mode, got " << perGroup.MaxInFlightTotal);

        TLoadScenarioSettings sharedSettings = perGroupSettings;
        sharedSettings.SharedRequestDispatching = true;

        auto shared = RunLoadScenario(sharedSettings);
        UNIT_ASSERT_C(shared.LoadPuts > 0, "Expected puts in shared mode");
        AssertEachGroupHasRequests(shared.LoadPutsByGroup, "puts");
        UNIT_ASSERT_VALUES_EQUAL(shared.MaxInFlightTotal, 1);
        UNIT_ASSERT_C(shared.MaxInFlightByGroup[0] <= 1 && shared.MaxInFlightByGroup[1] <= 1,
            "Per-group counters must never exceed configured per-request limit");
    }

    Y_UNIT_TEST(SharedRequestDispatchingSupportsMixedReadWriteMode) {
        TLoadScenarioSettings perGroupSettings{
            .SharedRequestDispatching = false,
            .EnableReads = true,
            .DelayPutResults = false,
            .WaitForFinish = false,
            .MinPutRequests = 10,
            .MinGetRequests = 2,
            .DurationSeconds = 5,
            .MaxInFlightWriteRequests = 4,
            .MaxInFlightReadRequests = 4,
            .PutResultDelay = TDuration::Zero(),
            .RequirePutsFromEveryGroup = true,
            .RequireGetsFromEveryGroup = true,
        };

        auto perGroup = RunLoadScenario(perGroupSettings);
        UNIT_ASSERT_C(perGroup.LoadPuts >= perGroupSettings.MinPutRequests,
            TStringBuilder() << "Expected at least " << perGroupSettings.MinPutRequests
                             << " puts in mixed per-group mode, got " << perGroup.LoadPuts);
        UNIT_ASSERT_C(perGroup.LoadGets >= perGroupSettings.MinGetRequests,
            TStringBuilder() << "Expected at least " << perGroupSettings.MinGetRequests
                             << " gets in mixed per-group mode, got " << perGroup.LoadGets);
        AssertEachGroupHasRequests(perGroup.LoadPutsByGroup, "puts");
        AssertEachGroupHasRequests(perGroup.LoadGetsByGroup, "gets");

        TLoadScenarioSettings sharedSettings = perGroupSettings;
        sharedSettings.SharedRequestDispatching = true;

        auto shared = RunLoadScenario(sharedSettings);
        UNIT_ASSERT_C(shared.LoadPuts >= sharedSettings.MinPutRequests,
            TStringBuilder() << "Expected at least " << sharedSettings.MinPutRequests
                             << " puts in mixed shared mode, got " << shared.LoadPuts);
        UNIT_ASSERT_C(shared.LoadGets >= sharedSettings.MinGetRequests,
            TStringBuilder() << "Expected at least " << sharedSettings.MinGetRequests
                             << " gets in mixed shared mode, got " << shared.LoadGets);
        AssertEachGroupHasRequests(shared.LoadPutsByGroup, "puts");
        AssertEachGroupHasRequests(shared.LoadGetsByGroup, "gets");
    }
}
