#include <ydb/core/blobstorage/dsproxy/dsproxy_strategy_put_mirror3dc.h>
#include <ydb/core/blobstorage/dsproxy/dsproxy_strategy_put_m3dc.h>
#include <ydb/core/blobstorage/dsproxy/dsproxy_strategy_accelerate_put_m3dc.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace {

using R = NKikimrBlobStorage::TGroupInfo::TFailRealm;
using S = TBlobState::ESituation;
using P = TEvBlobStorage::TEvPut;
using TCounts = std::array<ui32, 3>;
using TStates = std::array<R::EState, 3>;

TIntrusivePtr<TBlobStorageGroupInfo> MakeInfo(const TStates& states) {
    TBlobStorageGroupInfo::TDynamicInfo dynamic(TGroupId::Zero(), 1);
    dynamic.FailRealmStates.assign(states.begin(), states.end());
    return MakeIntrusive<TBlobStorageGroupInfo>(TBlobStorageGroupInfo::TTopology(
        TBlobStorageGroupType::ErasureMirror3dc, 3, 4, 1), std::move(dynamic), "");
}

struct TTest {
    TIntrusivePtr<TBlobStorageGroupInfo> Info;
    TIntrusivePtr<TGroupQueues> Queues;
    TBlackboard Blackboard;
    TLogoBlobID Id;
    TLogContext LogCtx{NKikimrServices::BS_PROXY_PUT, false};

    explicit TTest(const TStates& states, ui32 cookie = 0)
        : Info(MakeInfo(states))
        , Queues(MakeIntrusive<TGroupQueues>(Info->GetTopology()))
        , Blackboard(Info, Queues, NKikimrBlobStorage::UserData, NKikimrBlobStorage::FastRead)
        , Id(1, 1, 1, 0, 8, cookie)
    {
        LogCtx.LogAcc.IsLogEnabled = false;
        Blackboard.AccelerationMode = TBlackboard::AccelerationModeSkipMarked;
        Blackboard.RegisterBlobForPut(Id, 0);
        TStackVec<TRope, TypicalPartsInBlob> parts(3);
        ErasureSplit(TErasureType::CrcModeNone, Info->Type, TRope(TString("testdata")), parts,
            nullptr, GetDefaultRcBufAllocator());
        for (ui32 part = 0; part < 3; ++part) {
            Blackboard.AddPartToPut(Id, part, std::move(parts[part]));
        }
    }

    ui32 Realm(ui32 disk) {
        return Info->GetVDiskId(Blackboard[Id].Disks[disk].OrderNumber).FailRealm;
    }

    void Set(const TCounts& counts, S situation) {
        for (ui32 disk = 0; disk < 9; ++disk) {
            if (disk / 3 < counts[Realm(disk)]) {
                Blackboard[Id].Disks[disk].DiskParts[disk % 3].Situation = situation;
            }
        }
    }

    EStrategyOutcome Run(P::ETactic tactic, bool accelerate = false, bool mod3x3 = false) {
        Blackboard.GroupDiskRequests.PutsPending.clear();
        return TMirror3dcPutStrategy(tactic, mod3x3, accelerate).Process(
            LogCtx, Blackboard[Id], *Info, Blackboard, Blackboard.GroupDiskRequests, {});
    }

    EStrategyOutcome RunLegacy(P::ETactic tactic, bool accelerate = false, bool mod3x3 = false) {
        Blackboard.GroupDiskRequests.PutsPending.clear();
        if (accelerate) {
            return TAcceleratePut3dcStrategy(tactic, mod3x3).Process(
                LogCtx, Blackboard[Id], *Info, Blackboard, Blackboard.GroupDiskRequests, {});
        }
        return TPut3dcStrategy(tactic, mod3x3).Process(
            LogCtx, Blackboard[Id], *Info, Blackboard, Blackboard.GroupDiskRequests, {});
    }

    ui32 RequestMask() const {
        ui32 mask = 0;
        for (const auto& request : Blackboard.GroupDiskRequests.PutsPending) {
            const ui32 disk = Info->GetIdxInSubgroup(Info->GetVDiskId(request.OrderNumber), Id.Hash());
            UNIT_ASSERT_VALUES_EQUAL(request.Id.PartId(), disk % 3 + 1);
            UNIT_ASSERT(!(mask & (1u << disk)));
            mask |= 1u << disk;
        }
        return mask;
    }

    TCounts Requests() const {
        TCounts counts{};
        for (const auto& request : Blackboard.GroupDiskRequests.PutsPending) {
            ++counts[Info->GetVDiskId(request.OrderNumber).FailRealm];
            const ui32 disk = Info->GetIdxInSubgroup(Info->GetVDiskId(request.OrderNumber), Id.Hash());
            UNIT_ASSERT_VALUES_EQUAL(request.Id.PartId(), disk % 3 + 1);
        }
        return counts;
    }

    void Expect(const TCounts& counts) const {
        const auto actual = Requests();
        for (ui32 realm = 0; realm < 3; ++realm) {
            UNIT_ASSERT_VALUES_EQUAL_C(actual[realm], counts[realm], "realm " << realm);
        }
    }
};

} // namespace

Y_UNIT_TEST_SUITE(TMirror3dcPutStrategyTest) {
    Y_UNIT_TEST(WrittenQuorumUsesCurrentLayoutAndExcludesExpiredDisks) {
        for (ui32 cookie = 0; cookie < 3; ++cookie) {
            TTest t({R::N1, R::F, R::F}, cookie);
            t.Set({2, 3, 3}, S::Present);
            TBlobStorageGroupInfo::TGroupVDisks expired(&t.Info->GetTopology());
            const auto& state = t.Blackboard[t.Id];
            UNIT_ASSERT(state.HasWrittenQuorum(*t.Info, expired));
            for (ui32 disk = 0; disk < 3; ++disk) {
                if (t.Realm(disk) == 0) {
                    expired += TBlobStorageGroupInfo::TGroupVDisks(&t.Info->GetTopology(),
                        t.Info->GetVDiskId(state.Disks[disk].OrderNumber));
                }
            }
            UNIT_ASSERT(!state.HasWrittenQuorum(*t.Info, expired));
        }
    }

    Y_UNIT_TEST(NormalInitialAndPartialRepliesMatchLegacy) {
        for (auto tactic : {P::TacticDefault, P::TacticMaxThroughput, P::TacticMinLatency}) {
            for (bool mod3x3 : {false, true}) {
                for (ui32 cookie = 0; cookie < 3; ++cookie) {
                    TTest initial({R::N0, R::N0, R::N0}, cookie);
                    TTest legacyInitial({R::N0, R::N0, R::N0}, cookie);
                    UNIT_ASSERT_VALUES_EQUAL(ui32(initial.Run(tactic, false, mod3x3).Value),
                        ui32(legacyInitial.RunLegacy(tactic, false, mod3x3).Value));
                    const ui32 initialMask = initial.RequestMask();
                    UNIT_ASSERT_VALUES_EQUAL(initialMask, legacyInitial.RequestMask());
                    // Every subset of initial requests has replied OK; the others remain Sent.
                    for (ui32 replied = initialMask; ; replied = (replied - 1) & initialMask) {
                        TTest current({R::N0, R::N0, R::N0}, cookie);
                        TTest legacy({R::N0, R::N0, R::N0}, cookie);
                        for (ui32 disk = 0; disk < 9; ++disk) {
                            if (initialMask & (1u << disk)) {
                                const auto situation = replied & (1u << disk) ? S::Present : S::Sent;
                                current.Blackboard[current.Id].Disks[disk].DiskParts[disk % 3].Situation = situation;
                                legacy.Blackboard[legacy.Id].Disks[disk].DiskParts[disk % 3].Situation = situation;
                            }
                        }
                        const auto context = TStringBuilder() << "tactic " << ui32(tactic)
                            << " 3x3 " << mod3x3 << " cookie " << cookie << " replied " << replied;
                        UNIT_ASSERT_VALUES_EQUAL_C(ui32(current.Run(tactic, false, mod3x3).Value),
                            ui32(legacy.RunLegacy(tactic, false, mod3x3).Value), context);
                        UNIT_ASSERT_VALUES_EQUAL_C(current.RequestMask(), legacy.RequestMask(), context);
                        if (!replied) {
                            break;
                        }
                    }
                }
            }
        }
    }

    Y_UNIT_TEST(NormalFailureMasksMatchLegacy) {
        for (auto tactic : {P::TacticDefault, P::TacticMaxThroughput, P::TacticMinLatency}) {
            for (bool mod3x3 : {false, true}) {
                for (ui32 errors = 0; errors < 512; ++errors) {
                    TTest current({R::N0, R::N0, R::N0});
                    TTest legacy({R::N0, R::N0, R::N0});
                    bool degraded = false;
                    for (ui32 realm = 0; realm < 3; ++realm) {
                        const ui32 realmMask = 0b001001001u << realm;
                        degraded |= (errors & realmMask) == realmMask;
                    }
                    for (ui32 disk = 0; disk < 9; ++disk) {
                        if (errors & (1u << disk)) {
                            current.Blackboard[current.Id].Disks[disk].DiskParts[disk % 3].Situation = S::Error;
                            legacy.Blackboard[legacy.Id].Disks[disk].DiskParts[disk % 3].Situation = S::Error;
                        }
                    }
                    const auto context = TStringBuilder() << "tactic " << ui32(tactic)
                        << " 3x3 " << mod3x3 << " errors " << errors;
                    const auto outcome = current.Run(tactic, false, mod3x3);
                    UNIT_ASSERT_VALUES_EQUAL_C(ui32(outcome.Value),
                        ui32(legacy.RunLegacy(tactic, false, mod3x3).Value), context);
                    if (outcome == EStrategyOutcome::ERROR) {
                        UNIT_ASSERT_VALUES_EQUAL(current.RequestMask(), 0);
                        UNIT_ASSERT_VALUES_EQUAL(legacy.RequestMask(), 0);
                        continue;
                    }
                    // Skip request equality for degraded MinLatency: legacy targets 2+2, new targets 3+3.
                    // The legacy change belongs in a separate PR; outcomes are still compared here.
                    if (tactic != P::TacticMinLatency || mod3x3 || !degraded) {
                        UNIT_ASSERT_VALUES_EQUAL_C(current.RequestMask(), legacy.RequestMask(), context);
                    }
                    // Resolve each strategy's own requests successfully and compare completion.
                    for (auto* test : {&current, &legacy}) {
                        for (const auto& request : test->Blackboard.GroupDiskRequests.PutsPending) {
                            test->Blackboard.AddPutOkResponse(request.Id, request.OrderNumber);
                        }
                    }
                    UNIT_ASSERT(current.Run(tactic, false, mod3x3) == EStrategyOutcome::DONE);
                    UNIT_ASSERT(legacy.RunLegacy(tactic, false, mod3x3) == EStrategyOutcome::DONE);
                }
            }
        }
    }

    Y_UNIT_TEST(NormalInitialAccelerationMatchesLegacy) {
        for (auto tactic : {P::TacticDefault, P::TacticMaxThroughput, P::TacticMinLatency}) {
            for (bool mod3x3 : {false, true}) {
                TTest current({R::N0, R::N0, R::N0});
                TTest legacy({R::N0, R::N0, R::N0});
                UNIT_ASSERT(current.Run(tactic, false, mod3x3) == EStrategyOutcome::IN_PROGRESS);
                UNIT_ASSERT(legacy.RunLegacy(tactic, false, mod3x3) == EStrategyOutcome::IN_PROGRESS);
                UNIT_ASSERT_VALUES_EQUAL(ui32(current.Run(tactic, true, mod3x3).Value),
                    ui32(legacy.RunLegacy(tactic, true, mod3x3).Value));
                UNIT_ASSERT_VALUES_EQUAL(current.RequestMask(), legacy.RequestMask());
            }
        }
    }

    Y_UNIT_TEST(DegradedMinLatencyAccelerationUsesSpareCopies) {
        for (ui32 cookie = 0; cookie < 3; ++cookie) {
            TTest current({R::N0, R::N0, R::N0}, cookie);
            current.Set({0, 0, 3}, S::Error);
            current.Set({1, 0, 0}, S::Sent);
            // Target 3+3: two fresh disks supplement the pending write, all three in the other realm.
            // Do not compare with legacy here: its degraded MinLatency still targets 2+2.
            UNIT_ASSERT(current.Run(P::TacticMinLatency, true) == EStrategyOutcome::IN_PROGRESS);
            current.Expect({2, 3, 0});
        }
    }

    Y_UNIT_TEST(NormalFailureModelIsCheckedBeforeWrittenQuorum) {
        for (auto tactic : {P::TacticMaxThroughput, P::TacticMinLatency}) {
            TTest current({R::N0, R::N0, R::N0});
            TTest legacy({R::N0, R::N0, R::N0});
            for (auto* test : {&current, &legacy}) {
                test->Set({3, 3, 3}, S::Present);
                test->Set({2, 2, 0}, S::Error); // Remaining copies 1+1+3 satisfy W, errors violate legacy's E.
            }
            UNIT_ASSERT(current.Run(tactic) == EStrategyOutcome::ERROR);
            UNIT_ASSERT(legacy.RunLegacy(tactic) == EStrategyOutcome::ERROR);
            UNIT_ASSERT_VALUES_EQUAL(current.RequestMask(), 0);
            UNIT_ASSERT_VALUES_EQUAL(legacy.RequestMask(), 0);
        }
    }

    Y_UNIT_TEST(FailureModelUsesCurrentEnsuredLayoutAndIgnoresRecoveryAndFailedRealms) {
        for (ui32 cookie = 0; cookie < 3; ++cookie) {
            for (auto tactic : {P::TacticMaxThroughput, P::TacticMinLatency}) {
                for (bool accelerate : {false, true}) {
                    TTest inv1({R::N1, R::R, R::F}, cookie);
                    inv1.Set({1, 0, 0}, S::Error); // E=1 in N; W=2+2 is still attainable.
                    UNIT_ASSERT(inv1.Run(tactic, accelerate) == EStrategyOutcome::ERROR);
                    inv1.Expect({0, 0, 0});

                    TTest inv2({R::N2, R::R, R::F}, cookie);
                    inv2.Set({1, 0, 3}, S::Error); // E=2 permits one failed disk in N.
                    UNIT_ASSERT(inv2.Run(tactic, accelerate) == EStrategyOutcome::IN_PROGRESS);

                    TTest recovering({R::N1, R::N1, R::R}, cookie);
                    recovering.Set({1, 0, 3}, S::Error); // E=1+1 uses only N realms.
                    UNIT_ASSERT(recovering.Run(tactic, accelerate) == EStrategyOutcome::IN_PROGRESS);
                    recovering.Set({1, 1, 3}, S::Error);
                    UNIT_ASSERT(recovering.Run(tactic, accelerate) == EStrategyOutcome::ERROR);
                    recovering.Expect({0, 0, 0});
                }
            }
        }
    }

    Y_UNIT_TEST(InitialLayoutsFollowRealmRoles) {
        struct TCase {
            TStates States;
            TCounts Throughput;
            TCounts Latency;
        };
        const TCase cases[] = {
            {{R::N0, R::N0, R::N0}, {1, 1, 1}, {2, 2, 2}}, // W = 1+1+1 OR 2+2.
            {{R::N1, R::N2, R::R}, {1, 1, 1}, {2, 2, 1}}, // Union of 1+1+1 and 2+2 in N/N.
            {{R::N0, R::N0, R::F}, {2, 2, 0}, {3, 3, 0}}, // W = 2+2, expand to 3+3.
            {{R::N2, R::N2, R::F}, {2, 2, 0}, {3, 3, 0}},
            {{R::N1, R::F, R::F}, {2, 0, 0}, {3, 0, 0}},
            {{R::N2, R::F, R::F}, {2, 0, 0}, {3, 0, 0}},
            {{R::N1, R::R, R::F}, {2, 2, 0}, {3, 3, 0}}, // W = 2+2 in N/R.
        };
        for (const auto& test : cases) {
            std::array<ui32, 3> permutation = {0, 1, 2};
            do {
                for (ui32 cookie = 0; cookie < 3; ++cookie) {
                    for (auto tactic : {P::TacticDefault, P::TacticMaxThroughput, P::TacticMinLatency}) {
                        TTest t({test.States[permutation[0]], test.States[permutation[1]], test.States[permutation[2]]}, cookie);
                        UNIT_ASSERT(t.Run(tactic) == EStrategyOutcome::IN_PROGRESS);
                        const auto& expected = tactic == P::TacticMinLatency ? test.Latency : test.Throughput;
                        t.Expect({expected[permutation[0]], expected[permutation[1]], expected[permutation[2]]});
                        // Re-running while all requests are pending does not send duplicates.
                        UNIT_ASSERT(t.Run(tactic) == EStrategyOutcome::IN_PROGRESS);
                        t.Expect({0, 0, 0});
                    }
                }
            } while (std::next_permutation(permutation.begin(), permutation.end()));
        }
    }

    Y_UNIT_TEST(PresentAndSentReduceNewRequests) {
        TTest throughput({R::N0, R::N0, R::N0});
        throughput.Set({2, 0, 0}, S::Present);
        UNIT_ASSERT(throughput.Run(P::TacticMaxThroughput) == EStrategyOutcome::IN_PROGRESS);
        throughput.Expect({0, 1, 1}); // Prefer three realms even with two copies in the first.

        TTest latency({R::N0, R::N0, R::N0});
        latency.Set({1, 0, 0}, S::Present);
        latency.Set({0, 1, 0}, S::Sent);
        UNIT_ASSERT(latency.Run(P::TacticMinLatency) == EStrategyOutcome::IN_PROGRESS);
        latency.Expect({1, 1, 2}); // Complete the 3x2 rectangle without repeating writes.
    }

    Y_UNIT_TEST(PartialRectangleAndErrorFallback) {
        for (auto tactic : {P::TacticMaxThroughput, P::TacticMinLatency}) {
            TTest t({R::N0, R::N0, R::N0});
            t.Set({0, 1, 3}, S::Error); // Available: 3+2+0; only W = 2+2 remains.
            UNIT_ASSERT(t.Run(tactic) == EStrategyOutcome::IN_PROGRESS);
            t.Expect(tactic == P::TacticMinLatency ? TCounts{3, 2, 0} : TCounts{2, 2, 0});
        }
        TTest t({R::N0, R::N0, R::N0});
        t.Set({0, 2, 2}, S::Error); // W is attainable, but the errors exceed E.
        UNIT_ASSERT(t.Run(P::TacticMinLatency) == EStrategyOutcome::ERROR);
        t.Expect({0, 0, 0});
    }

    Y_UNIT_TEST(RecoveryCannotReplaceAnAvailableNormalRealm) {
        TTest t({R::N0, R::N0, R::R});
        t.Set({2, 0, 2}, S::Present);
        UNIT_ASSERT(t.Run(P::TacticMaxThroughput) == EStrategyOutcome::IN_PROGRESS);
        t.Expect({0, 1, 0}); // Complete 1+1+1, not 2+2 in N/R.
        t.Set({0, 3, 0}, S::Error);
        UNIT_ASSERT(t.Run(P::TacticMaxThroughput) == EStrategyOutcome::ERROR);
        t.Expect({0, 0, 0});
    }

    Y_UNIT_TEST(DoneUsesWriteWithoutWaitingForDurability) {
        TTest t({R::N2, R::N2, R::R});
        t.Set({2, 2, 0}, S::Present); // W = 2+2 is complete, D = 1+1+1 is not.
        UNIT_ASSERT(t.Run(P::TacticMinLatency) == EStrategyOutcome::DONE);
        t.Expect({0, 0, 0});

        TTest solo({R::N1, R::F, R::F});
        solo.Set({3, 3, 3}, S::Present);
        solo.Set({1, 0, 0}, S::Error); // Violates E = 1 but W = 2 remains satisfied.
        UNIT_ASSERT(solo.Run(P::TacticMaxThroughput) == EStrategyOutcome::ERROR);
        solo.Expect({0, 0, 0});
    }

    Y_UNIT_TEST(AccelerationUsesFreshDisks) {
        for (auto tactic : {P::TacticMaxThroughput, P::TacticMinLatency}) {
            TTest t({R::N0, R::N0, R::N0});
            UNIT_ASSERT(t.Run(tactic) == EStrategyOutcome::IN_PROGRESS);
            UNIT_ASSERT(t.Run(tactic, true) == EStrategyOutcome::IN_PROGRESS);
            t.Expect({1, 1, 1});
            UNIT_ASSERT(t.Run(tactic) == EStrategyOutcome::IN_PROGRESS);
            t.Expect({0, 0, 0});
        }
        TTest t({R::N1, R::R, R::F});
        t.Set({1, 1, 0}, S::Present);
        UNIT_ASSERT(t.Run(P::TacticMaxThroughput) == EStrategyOutcome::IN_PROGRESS);
        t.Expect({1, 1, 0});
        UNIT_ASSERT(t.Run(P::TacticMaxThroughput, true) == EStrategyOutcome::IN_PROGRESS);
        t.Expect({1, 1, 0});
        UNIT_ASSERT(t.Run(P::TacticMaxThroughput, true) == EStrategyOutcome::IN_PROGRESS);
        t.Expect({0, 0, 0}); // All remaining disks already have a pending write.
    }

    Y_UNIT_TEST(SlowDisksAreAvoidedOnlyWhileQuorumRemainsPossible) {
        TTest t({R::N0, R::N0, R::N0});
        for (ui32 disk = 0; disk < 9; ++disk) {
            t.Blackboard[t.Id].Disks[disk].IsSlow = t.Realm(disk) == 2;
        }
        UNIT_ASSERT(t.Run(P::TacticMaxThroughput) == EStrategyOutcome::IN_PROGRESS);
        t.Expect({2, 2, 0});

        TTest solo({R::N1, R::F, R::F});
        for (auto& disk : solo.Blackboard[solo.Id].Disks) {
            disk.IsSlow = true;
        }
        UNIT_ASSERT(solo.Run(P::TacticMaxThroughput) == EStrategyOutcome::IN_PROGRESS);
        solo.Expect({2, 0, 0});
    }

    Y_UNIT_TEST(Explicit3x3ModeStillSkipsFailedRealms) {
        TTest normal({R::N0, R::N0, R::N0});
        UNIT_ASSERT(normal.Run(P::TacticMinLatency, false, true) == EStrategyOutcome::IN_PROGRESS);
        normal.Expect({3, 3, 3});
        TTest failed({R::N0, R::N0, R::F});
        UNIT_ASSERT(failed.Run(P::TacticMinLatency, false, true) == EStrategyOutcome::IN_PROGRESS);
        failed.Expect({3, 3, 0});
        // Explicit 3x3 mode must also reject errors that exceed E.
        TTest partial({R::N0, R::N0, R::N0});
        partial.Set({1, 1, 2}, S::Error);
        UNIT_ASSERT(partial.Run(P::TacticMinLatency, false, true) == EStrategyOutcome::ERROR);
        partial.Expect({0, 0, 0});
    }

    Y_UNIT_TEST(InvalidStateDoesNotSendWrites) {
        TTest t({R::N0, R::R, R::R});
        UNIT_ASSERT(t.Run(P::TacticMaxThroughput) == EStrategyOutcome::ERROR);
        t.Expect({0, 0, 0});
    }

    Y_UNIT_TEST(EveryFailureMaskTerminatesAccordingToWriteQuorumAndFailureModel) {
        // 512 binary failure masks, not a Cartesian product of all reply statuses.
        for (const auto states : {TStates{R::N0, R::N0, R::N0}, TStates{R::N1, R::N1, R::R},
                TStates{R::N0, R::N0, R::F}, TStates{R::N1, R::R, R::F}, TStates{R::N1, R::F, R::F}}) {
            for (auto tactic : {P::TacticMaxThroughput, P::TacticMinLatency}) {
                for (ui32 errors = 0; errors < 512; ++errors) {
                    TTest t(states);
                    const auto checker = TMirror3dcWriteQuorumChecker::TryCreate(*t.Info, t.Id.Hash());
                    const auto possible = TBlobStorageGroupInfo::TSubgroupVDisks::CreateFromMask(
                        &t.Info->GetTopology(), 511 ^ errors);
                    TBlobStorageGroupInfo::TSubgroupVDisks observedFailures(&t.Info->GetTopology());
                    ui32 sent = 0;
                    for (ui32 step = 0; ; ++step) {
                        UNIT_ASSERT(step < 10);
                        const auto outcome = t.Run(tactic);
                        if (outcome != EStrategyOutcome::IN_PROGRESS) {
                            const bool failureModel = checker->GetBlobState({}, observedFailures)
                                != TBlobStorageGroupInfo::EBS_DISINTEGRATED;
                            const bool expected = failureModel && checker->CheckQuorumForSubgroup(possible);
                            UNIT_ASSERT_VALUES_EQUAL_C(outcome == EStrategyOutcome::DONE, expected,
                                "errors " << errors << " tactic " << static_cast<ui32>(tactic));
                            break;
                        }
                        UNIT_ASSERT(!t.Blackboard.GroupDiskRequests.PutsPending.empty());
                        for (const auto& request : t.Blackboard.GroupDiskRequests.PutsPending) {
                            const ui32 disk = t.Info->GetIdxInSubgroup(t.Info->GetVDiskId(request.OrderNumber), t.Id.Hash());
                            UNIT_ASSERT(!(sent & (1u << disk)));
                            UNIT_ASSERT(states[t.Realm(disk)] != R::F);
                            sent |= 1u << disk;
                            if (errors & (1u << disk)) {
                                observedFailures += TBlobStorageGroupInfo::TSubgroupVDisks(&t.Info->GetTopology(), disk);
                                t.Blackboard.AddErrorResponse(request.Id, request.OrderNumber, "test error");
                            } else {
                                t.Blackboard.AddPutOkResponse(request.Id, request.OrderNumber);
                            }
                        }
                    }
                }
            }
        }
    }

}

} // namespace NKikimr
