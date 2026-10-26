#include <ydb/core/blobstorage/dsproxy/dsproxy_strategy_get_mirror3dc.h>
#include <ydb/core/blobstorage/dsproxy/dsproxy_strategy_get_m3dc_basic.h>
#include <ydb/core/blobstorage/dsproxy/dsproxy_strategy_get_m3dc_restore.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace {

using R = NKikimrBlobStorage::TGroupInfo::TFailRealm;
using S = TBlobState::ESituation;
using G = TBlobStorageGroupInfo;
using TStates = std::array<R::EState, 3>;

TIntrusivePtr<G> MakeInfo(const TStates& states) {
    auto topology = std::make_shared<G::TTopology>(TBlobStorageGroupType::ErasureMirror3dc, 3, 4, 1);
    topology->FinalizeConstruction();
    G::TDynamicInfo dynamic(TGroupId::Zero(), 1);
    dynamic.FailRealmStates.assign(states.begin(), states.end());
    for (ui32 order = 0; order < topology->GetTotalVDisksNum(); ++order) {
        dynamic.PushBackActorId(TActorId(order + 1, 0, 1, 0));
    }
    return MakeIntrusive<G>(std::move(topology), std::move(dynamic), "",
        TMaybe<TKikimrScopeId>{}, NPDisk::DEVICE_TYPE_UNKNOWN);
}

struct TTest {
    TIntrusivePtr<G> Info;
    TIntrusivePtr<TGroupQueues> Queues;
    TBlackboard Blackboard;
    TLogoBlobID Id;
    TLogContext LogCtx{NKikimrServices::BS_PROXY_GET, false};
    TNodeLayoutInfoPtr NodeLayout;

    explicit TTest(const TStates& states, ui32 cookie = 0, ui32 shift = 0, ui32 size = 8)
        : Info(MakeInfo(states))
        , Queues(MakeIntrusive<TGroupQueues>(Info->GetTopology()))
        , Blackboard(Info, Queues, NKikimrBlobStorage::UserData, NKikimrBlobStorage::FastRead)
        , Id(1, 1, 1, 0, 8, cookie)
    {
        LogCtx.LogAcc.IsLogEnabled = false;
        Blackboard.AccelerationMode = TBlackboard::AccelerationModeSkipMarked;
        Blackboard.AddNeeded(Id, shift, size);
        Blackboard[Id].BlobIdx = 0;
    }

    ui32 Realm(ui32 disk) {
        return Info->GetVDiskId(Blackboard[Id].Disks[disk].OrderNumber).FailRealm;
    }

    EStrategyOutcome Run(bool phantomCheck = false, bool legacy = false) {
        Blackboard.GroupDiskRequests.GetsPending.clear();
        if (legacy) {
            return TMirror3dcBasicGetStrategy(NodeLayout, phantomCheck).Process(
                LogCtx, Blackboard[Id], *Info, Blackboard, Blackboard.GroupDiskRequests, {});
        }
        return TMirror3dcGetStrategy(NodeLayout, phantomCheck).Process(
            LogCtx, Blackboard[Id], *Info, Blackboard, Blackboard.GroupDiskRequests, {});
    }

    void Reply(ui32 disk, S situation) {
        const ui32 order = Blackboard[Id].Disks[disk].OrderNumber;
        const TLogoBlobID partId(Id, disk % 3 + 1);
        switch (situation) {
            case S::Absent: Blackboard.AddNoDataResponse(partId, order); break;
            case S::Error: Blackboard.AddErrorResponse(partId, order, "test error"); break;
            case S::Lost: Blackboard.AddNotYetResponse(partId, order); break;
            default: UNIT_FAIL("Expected a negative reply");
        }
    }

    EStrategyOutcome RunRestore(bool accelerate = false, bool legacy = false) {
        Blackboard.GroupDiskRequests.GetsPending.clear();
        Blackboard.GroupDiskRequests.PutsPending.clear();
        if (legacy) {
            return TMirror3dcGetWithRestoreStrategy().Process(
                LogCtx, Blackboard[Id], *Info, Blackboard, Blackboard.GroupDiskRequests, {});
        }
        return TMirror3dcGetStrategy(NodeLayout, false, true, accelerate).Process(
            LogCtx, Blackboard[Id], *Info, Blackboard, Blackboard.GroupDiskRequests, {});
    }

    void ReadFoundData() {
        UNIT_ASSERT(RunRestore() == EStrategyOutcome::IN_PROGRESS);
        const auto requests = Blackboard.GroupDiskRequests.GetsPending;
        UNIT_ASSERT(!requests.empty());
        for (const auto& request : requests) {
            UNIT_ASSERT_VALUES_EQUAL(request.Shift, 0);
            UNIT_ASSERT_VALUES_EQUAL(request.Size, Id.BlobSize());
            const ui32 disk = Info->GetIdxInSubgroup(Info->GetVDiskId(request.OrderNumber), Id.Hash());
            UNIT_ASSERT(Info->GetFailRealmState(Realm(disk)) != R::F);
            if (request.OrderNumber == requests.front().OrderNumber) {
                Blackboard.AddResponseData(request.Id, request.OrderNumber, 0, TRope(TString("testdata")));
            } else {
                Reply(disk, S::Absent);
            }
        }
    }

    void ConfirmPuts() {
        for (const auto& request : Blackboard.GroupDiskRequests.PutsPending) {
            const auto realm = Info->GetVDiskId(request.OrderNumber).FailRealm;
            UNIT_ASSERT(Info->GetFailRealmState(realm) != R::F);
            UNIT_ASSERT_VALUES_EQUAL(request.Buffer.ConvertToString(), "testdata");
            Blackboard.AddPutOkResponse(request.Id, request.OrderNumber);
        }
    }

    void Data(ui32 disk) {
        Blackboard.AddResponseData(TLogoBlobID(Id, disk % 3 + 1),
            Blackboard[Id].Disks[disk].OrderNumber, 0, TRope(TString("testdata")));
    }

    ui32 PutMask() const {
        ui32 mask = 0;
        for (const auto& request : Blackboard.GroupDiskRequests.PutsPending) {
            const ui32 disk = Info->GetIdxInSubgroup(Info->GetVDiskId(request.OrderNumber), Id.Hash());
            UNIT_ASSERT_VALUES_EQUAL(request.Id.PartId(), disk % 3 + 1);
            mask |= 1u << disk;
        }
        return mask;
    }

    EStrategyOutcome::TValue Result(const EStrategyOutcome& outcome) {
        return Blackboard[Id].WholeSituation == S::Error ? EStrategyOutcome::ERROR : outcome.Value;
    }

    ui32 RequestedDisk() {
        const auto& requests = Blackboard.GroupDiskRequests.GetsPending;
        UNIT_ASSERT_VALUES_EQUAL(requests.size(), 1);
        const auto& request = requests.front();
        const ui32 disk = Info->GetIdxInSubgroup(Info->GetVDiskId(request.OrderNumber), Id.Hash());
        UNIT_ASSERT_VALUES_EQUAL(request.Id.PartId(), disk % 3 + 1);
        return disk;
    }
};

} // namespace

Y_UNIT_TEST_SUITE(TMirror3dcGetStrategyTest) {
    Y_UNIT_TEST(RestoreReadsWholeBlobAndWaitsForWriteQuorum) {
        for (const auto states : {TStates{R::N0, R::N0, R::N0}, TStates{R::N2, R::N2, R::R},
                TStates{R::N1, R::N1, R::F}, TStates{R::N1, R::F, R::F}, TStates{R::N1, R::R, R::F}}) {
            for (ui32 cookie = 0; cookie < 3; ++cookie) {
                TTest t(states, cookie, 2, 3);
                t.ReadFoundData();
                UNIT_ASSERT(t.RunRestore() == EStrategyOutcome::IN_PROGRESS);
                UNIT_ASSERT(!t.Blackboard.GroupDiskRequests.PutsPending.empty());
                UNIT_ASSERT(t.Blackboard.GroupDiskRequests.GetsPending.empty());
                UNIT_ASSERT(t.Blackboard[t.Id].WholeSituation == S::Unknown);
                const auto pending = t.Blackboard.GroupDiskRequests.PutsPending;
                UNIT_ASSERT(t.RunRestore() == EStrategyOutcome::IN_PROGRESS);
                UNIT_ASSERT(t.Blackboard.GroupDiskRequests.PutsPending.empty());
                t.Blackboard.GroupDiskRequests.PutsPending = pending;
                t.ConfirmPuts();
                UNIT_ASSERT(t.RunRestore() == EStrategyOutcome::DONE);
                UNIT_ASSERT(t.Blackboard[t.Id].WholeSituation == S::Present);
                UNIT_ASSERT_VALUES_EQUAL(t.Blackboard[t.Id].Whole.Data.Read(2, 3).ConvertToString(), "std");
                UNIT_ASSERT(t.Blackboard[t.Id].Whole.Needed == TIntervalSet<i32>(2, 5));
            }
        }
    }

    Y_UNIT_TEST(RestoreUsesDataFromRecoveringRealm) {
        TTest t({R::N1, R::R, R::F}); // W = 2+2 in N/R.
        for (ui32 disk = 0; disk < 9; ++disk) {
            if (t.Realm(disk) == 0) {
                t.Reply(disk, S::Absent);
            }
        }
        t.ReadFoundData();
        UNIT_ASSERT(t.RunRestore() == EStrategyOutcome::IN_PROGRESS);
        std::array<ui32, 3> writes{};
        for (const auto& request : t.Blackboard.GroupDiskRequests.PutsPending) {
            ++writes[t.Info->GetVDiskId(request.OrderNumber).FailRealm];
        }
        UNIT_ASSERT_VALUES_EQUAL(writes[0], 2);
        UNIT_ASSERT_VALUES_EQUAL(writes[1], 1);
        UNIT_ASSERT_VALUES_EQUAL(writes[2], 0);
        t.ConfirmPuts();
        UNIT_ASSERT(t.RunRestore() == EStrategyOutcome::DONE);
    }

    Y_UNIT_TEST(RestoreRetriesFailedWriteAndAcceleratesPendingWrite) {
        for (bool accelerate : {false, true}) {
            TTest t({R::N2, R::F, R::F}); // One failed write stays within E = 2.
            t.ReadFoundData();
            UNIT_ASSERT(t.RunRestore() == EStrategyOutcome::IN_PROGRESS);
            UNIT_ASSERT_VALUES_EQUAL(t.Blackboard.GroupDiskRequests.PutsPending.size(), 1);
            const auto first = t.Blackboard.GroupDiskRequests.PutsPending.front();
            if (!accelerate) {
                t.Blackboard.AddErrorResponse(first.Id, first.OrderNumber, "write failed");
            }
            UNIT_ASSERT(t.RunRestore(accelerate) == EStrategyOutcome::IN_PROGRESS);
            UNIT_ASSERT_VALUES_EQUAL(t.Blackboard.GroupDiskRequests.PutsPending.size(), 1);
            UNIT_ASSERT(t.Blackboard.GroupDiskRequests.PutsPending.front().OrderNumber != first.OrderNumber);
            t.ConfirmPuts();
            UNIT_ASSERT(t.RunRestore() == EStrategyOutcome::DONE);
        }
    }

    Y_UNIT_TEST(RestoreDoesNotWriteOutsideEnsuredFailureModel) {
        TTest t({R::N1, R::F, R::F}); // E = 1: one read error is excessive.
        t.ReadFoundData();
        const ui32 found = t.Info->GetIdxInSubgroup(t.Info->GetVDiskId(
            t.Blackboard.GroupDiskRequests.GetsPending.front().OrderNumber), t.Id.Hash());
        t.Reply((found + 3) % 9, S::Error);
        UNIT_ASSERT(t.RunRestore() == EStrategyOutcome::ERROR);
        UNIT_ASSERT(t.Blackboard.GroupDiskRequests.PutsPending.empty());
    }

    Y_UNIT_TEST(RestoreWaitsOnlyWhileUnknownRepliesExceedFailureModel) {
        for (bool legacy : {false, true}) {
            TTest t({R::N0, R::N0, R::N0});
            UNIT_ASSERT(t.RunRestore(false, legacy) == EStrategyOutcome::IN_PROGRESS);
            UNIT_ASSERT_VALUES_EQUAL(t.Blackboard.GroupDiskRequests.GetsPending.size(), 9);
            t.Blackboard.AddResponseData(TLogoBlobID(t.Id, 1),
                t.Blackboard[t.Id].Disks[0].OrderNumber, 0, TRope(TString("testdata")));
            UNIT_ASSERT(t.RunRestore(false, legacy) == EStrategyOutcome::IN_PROGRESS);
            UNIT_ASSERT(t.Blackboard.GroupDiskRequests.PutsPending.empty());
            UNIT_ASSERT(t.Blackboard.GroupDiskRequests.GetsPending.empty());
            // Only realm 2 remains unknown: it cannot hide E = 1+1+1 OR 2+2.
            for (ui32 disk : {1u, 3u, 4u, 6u, 7u}) {
                t.Reply(disk, S::Absent);
            }
            UNIT_ASSERT(t.RunRestore(false, legacy) == EStrategyOutcome::IN_PROGRESS);
            UNIT_ASSERT(!t.Blackboard.GroupDiskRequests.PutsPending.empty());
            t.ConfirmPuts();
            UNIT_ASSERT(t.RunRestore(false, legacy) == EStrategyOutcome::DONE);
        }
    }

    Y_UNIT_TEST(RestoreExistingQuorumDoesNotWriteOrWaitForOtherReads) {
        for (bool legacy : {false, true}) {
            for (ui32 present : {0b000000111u, 0b000011011u}) {
                TTest t({R::N0, R::N0, R::N0});
                UNIT_ASSERT(t.RunRestore(false, legacy) == EStrategyOutcome::IN_PROGRESS);
                for (ui32 disk = 0; disk < 9; ++disk) {
                    if (present & (1u << disk)) {
                        t.Blackboard.AddResponseData(TLogoBlobID(t.Id, disk % 3 + 1),
                            t.Blackboard[t.Id].Disks[disk].OrderNumber, 0, TRope(TString("testdata")));
                    }
                }
                UNIT_ASSERT(t.RunRestore(false, legacy) == EStrategyOutcome::DONE);
                UNIT_ASSERT(t.Blackboard.GroupDiskRequests.PutsPending.empty());
                UNIT_ASSERT(t.Blackboard[t.Id].WholeSituation == S::Present);
            }
        }
    }

    Y_UNIT_TEST(RestoreParallelReadsExcludeFailedRealms) {
        for (const auto states : {TStates{R::N0, R::N0, R::N0}, TStates{R::N1, R::N1, R::F},
                TStates{R::N1, R::F, R::F}, TStates{R::N1, R::R, R::F}}) {
            for (ui32 cookie = 0; cookie < 3; ++cookie) {
                TTest t(states, cookie, 2, 3);
                UNIT_ASSERT(t.RunRestore() == EStrategyOutcome::IN_PROGRESS);
                ui32 mask = 0;
                for (const auto& request : t.Blackboard.GroupDiskRequests.GetsPending) {
                    const ui32 disk = t.Info->GetIdxInSubgroup(t.Info->GetVDiskId(request.OrderNumber), t.Id.Hash());
                    UNIT_ASSERT(!(mask & (1u << disk)));
                    mask |= 1u << disk;
                    UNIT_ASSERT_VALUES_EQUAL(request.Shift, 0);
                    UNIT_ASSERT_VALUES_EQUAL(request.Size, 8);
                }
                for (ui32 disk = 0; disk < 9; ++disk) {
                    UNIT_ASSERT_VALUES_EQUAL(bool(mask & (1u << disk)), states[t.Realm(disk)] != R::F);
                }
                UNIT_ASSERT(t.RunRestore() == EStrategyOutcome::IN_PROGRESS);
                UNIT_ASSERT(t.Blackboard.GroupDiskRequests.GetsPending.empty());
            }
        }
    }

    Y_UNIT_TEST(RestoreWaitThresholdDependsOnEnsuredLayout) {
        for (const auto normal : {R::N1, R::N2}) {
            TTest t({normal, R::F, R::F});
            UNIT_ASSERT(t.RunRestore() == EStrategyOutcome::IN_PROGRESS);
            const auto requests = t.Blackboard.GroupDiskRequests.GetsPending;
            UNIT_ASSERT_VALUES_EQUAL(requests.size(), 3);
            const auto& found = requests[0];
            t.Blackboard.AddResponseData(found.Id, found.OrderNumber, 0, TRope(TString("testdata")));
            UNIT_ASSERT(t.RunRestore() == EStrategyOutcome::IN_PROGRESS);
            UNIT_ASSERT(t.Blackboard.GroupDiskRequests.PutsPending.empty());
            t.Blackboard.AddNoDataResponse(requests[1].Id, requests[1].OrderNumber);
            UNIT_ASSERT(t.RunRestore() == EStrategyOutcome::IN_PROGRESS);
            // One pending reply can hide E = 1, but cannot hide E = 2.
            UNIT_ASSERT_VALUES_EQUAL(!t.Blackboard.GroupDiskRequests.PutsPending.empty(), normal == R::N2);
            if (normal == R::N1) {
                t.Blackboard.AddNoDataResponse(requests[2].Id, requests[2].OrderNumber);
                UNIT_ASSERT(t.RunRestore() == EStrategyOutcome::IN_PROGRESS);
                UNIT_ASSERT(!t.Blackboard.GroupDiskRequests.PutsPending.empty());
            }
        }
    }

    Y_UNIT_TEST(RestoreAbsenceUsesEnsuredWithoutWriting) {
        for (const auto normal : {R::N1, R::N2}) {
            TTest t({normal, R::F, R::F});
            for (ui32 disk = 0; disk < 9; ++disk) {
                t.Reply(disk, S::Absent);
            }
            UNIT_ASSERT(t.RunRestore() == EStrategyOutcome::DONE);
            UNIT_ASSERT(t.Blackboard[t.Id].WholeSituation == S::Absent);
            UNIT_ASSERT(t.Blackboard.GroupDiskRequests.PutsPending.empty());
        }
    }

    Y_UNIT_TEST(PendingReadIsNotAnErrorOrDuplicate) {
        TTest t({R::N1, R::F, R::F});
        UNIT_ASSERT(t.Run() == EStrategyOutcome::IN_PROGRESS);
        const ui32 disk = t.RequestedDisk();
        UNIT_ASSERT_VALUES_EQUAL(t.Realm(disk), 0);
        UNIT_ASSERT(t.Run() == EStrategyOutcome::IN_PROGRESS);
        UNIT_ASSERT(t.Blackboard.GroupDiskRequests.GetsPending.empty());
        t.Blackboard[t.Id].Disks[disk].IsSlow = true;
        UNIT_ASSERT(t.Run() == EStrategyOutcome::IN_PROGRESS);
        UNIT_ASSERT(t.RequestedDisk() != disk);
        UNIT_ASSERT_VALUES_EQUAL(t.Realm(t.RequestedDisk()), 0);
    }

    Y_UNIT_TEST(ReadsRequestedRangeFromNormalOrRecoveringRealm) {
        for (const auto states : {TStates{R::N1, R::F, R::F}, TStates{R::N1, R::R, R::F}}) {
            for (ui32 cookie = 0; cookie < 3; ++cookie) {
                TTest t(states, cookie, 2, 3);
                if (states[1] == R::R) {
                    // Force fallback to R; its positive reply can supply the requested data.
                    for (ui32 disk = 0; disk < 9; ++disk) {
                        if (t.Realm(disk) == 0) {
                            t.Reply(disk, S::Absent);
                        }
                    }
                }
                UNIT_ASSERT(t.Run() == EStrategyOutcome::IN_PROGRESS);
                const auto request = t.Blackboard.GroupDiskRequests.GetsPending.front();
                UNIT_ASSERT_VALUES_EQUAL(request.Shift, 2);
                UNIT_ASSERT_VALUES_EQUAL(request.Size, 3);
                t.Blackboard.AddResponseData(request.Id, request.OrderNumber, 2, TRope(TString("std")));
                UNIT_ASSERT(t.Run() == EStrategyOutcome::DONE);
                UNIT_ASSERT(t.Blackboard[t.Id].WholeSituation == S::Present);
                UNIT_ASSERT_VALUES_EQUAL(t.Blackboard[t.Id].Whole.Data.Read(2, 3).ConvertToString(), "std");
                UNIT_ASSERT(t.Blackboard.GroupDiskRequests.PutsPending.empty());
            }
        }
    }

    Y_UNIT_TEST(FailedRealmDataIsIgnored) {
        TTest t({R::N1, R::F, R::F});
        for (ui32 disk = 0; disk < 9; ++disk) {
            if (t.Realm(disk) == 0) {
                t.Reply(disk, S::Absent);
            } else {
                t.Blackboard.AddResponseData(TLogoBlobID(t.Id, disk % 3 + 1),
                    t.Blackboard[t.Id].Disks[disk].OrderNumber, 0, TRope(TString("testdata")));
            }
        }
        UNIT_ASSERT(t.Run() == EStrategyOutcome::DONE);
        UNIT_ASSERT(t.Blackboard[t.Id].WholeSituation == S::Absent);
        UNIT_ASSERT(t.Blackboard[t.Id].Whole.Here().IsEmpty());
    }

    Y_UNIT_TEST(EnsuredLayoutControlsAbsence) {
        struct TCase {
            TStates States;
            bool NotYet;
            S Expected;
        };
        const TCase cases[] = {
            {{R::N1, R::F, R::F}, false, S::Absent}, // E = 1 is excluded by NODATA in N.
            {{R::N1, R::F, R::F}, true, S::Absent}, // Without ERROR, NOT_YET confirms missing data.
            {{R::N2, R::F, R::F}, true, S::Absent}, // NOT_YET alone does not make absence uncertain.
            {{R::N2, R::N2, R::R}, true, S::Absent}, // NOT_YET without ERROR confirms missing data; R NODATA is trusted.
        };
        for (const auto& test : cases) {
            TTest t(test.States);
            bool lost = false;
            for (ui32 disk = 0; disk < 9; ++disk) {
                const auto realmState = test.States[t.Realm(disk)];
                const bool useLost = test.NotYet && !lost && (realmState == R::N1 || realmState == R::N2);
                t.Reply(disk, useLost ? S::Lost : S::Absent);
                lost |= useLost;
            }
            UNIT_ASSERT(t.Run() == EStrategyOutcome::DONE);
            UNIT_ASSERT(t.Blackboard[t.Id].WholeSituation == test.Expected);
        }
    }

    Y_UNIT_TEST(RecoveryNegativeRepliesFollowEnsuredAlternatives) {
        for (bool restore : {false, true}) {
            for (const auto recoveryReply : {S::Absent, S::Error, S::Lost}) {
                for (ui32 errorsPerNormalRealm = 0; errorsPerNormalRealm <= 2; ++errorsPerNormalRealm) {
                    for (ui32 cookie = 0; cookie < 3; ++cookie) {
                        TTest t({R::N2, R::N2, R::R}, cookie); // E = 1+1+1 OR 2+2.
                        for (ui32 disk = 0; disk < 9; ++disk) {
                            t.Reply(disk, t.Realm(disk) == 2 ? recoveryReply
                                : disk / 3 < errorsPerNormalRealm ? S::Error : S::Absent);
                        }
                        const auto outcome = restore ? t.RunRestore() : t.Run();
                        const bool hidden = errorsPerNormalRealm == 2
                            || (errorsPerNormalRealm == 1 && recoveryReply != S::Absent);
                        const bool failedModel = errorsPerNormalRealm == 2
                            || (errorsPerNormalRealm == 1 && recoveryReply == S::Error);
                        UNIT_ASSERT_VALUES_EQUAL(ui32(outcome.Value), ui32(failedModel
                            ? EStrategyOutcome::ERROR : EStrategyOutcome::DONE));
                        if (!failedModel) {
                            UNIT_ASSERT(t.Blackboard[t.Id].WholeSituation == (hidden ? S::Error : S::Absent));
                        }
                        UNIT_ASSERT(t.Blackboard.GroupDiskRequests.PutsPending.empty());
                    }
                }
            }
        }
    }

    Y_UNIT_TEST(RecoveryErrorsFailConservativePhantomCheck) {
        for (const auto recoveryReply : {S::Error, S::Lost}) {
            TTest t({R::N2, R::N2, R::R});
            for (ui32 disk = 0; disk < 9; ++disk) {
                t.Reply(disk, t.Realm(disk) == 2 ? recoveryReply : S::Absent);
            }
            UNIT_ASSERT(t.Run(true) == EStrategyOutcome::DONE);
            UNIT_ASSERT(t.Blackboard[t.Id].WholeSituation == (recoveryReply == S::Error ? S::Error : S::Absent));
        }
    }

    Y_UNIT_TEST(RecoveryErrorsCanViolateFailureModelDespiteExistingWriteQuorum) {
        TTest t({R::N2, R::N2, R::R});
        for (ui32 disk = 0; disk < 9; ++disk) {
            if (t.Realm(disk) != 2 && disk / 3 < 2) {
                t.Data(disk);
            } else {
                t.Reply(disk, S::Error);
            }
        }
        UNIT_ASSERT(t.RunRestore() == EStrategyOutcome::ERROR);
        UNIT_ASSERT(t.Blackboard[t.Id].WholeSituation != S::Present);
        UNIT_ASSERT(t.Blackboard.GroupDiskRequests.PutsPending.empty());
    }

    Y_UNIT_TEST(RestoreWaitsWhileUnknownRecoveryReplyCanHideEnsuredLayout) {
        for (const auto recoveryReply : {S::Absent, S::Unknown, S::Error, S::Lost}) {
            for (ui32 cookie = 0; cookie < 3; ++cookie) {
                TTest t({R::N2, R::N2, R::R}, cookie);
                const ui32 found = t.Realm(0) != 2 ? 3 : 4;
                for (ui32 disk = 0; disk < 9; ++disk) {
                    if (disk == found) {
                        t.Data(disk);
                    } else if (disk >= 3) {
                        t.Reply(disk, S::Absent);
                    } else if (t.Realm(disk) == 2 && recoveryReply != S::Unknown) {
                        t.Reply(disk, recoveryReply);
                    }
                }
                UNIT_ASSERT(t.RunRestore() == EStrategyOutcome::IN_PROGRESS);
                if (recoveryReply == S::Unknown) {
                    UNIT_ASSERT_VALUES_EQUAL(t.PutMask(), 0);
                    for (ui32 disk = 0; disk < 3; ++disk) {
                        if (t.Realm(disk) == 2) {
                            t.Reply(disk, S::Absent);
                        }
                    }
                    UNIT_ASSERT(t.RunRestore() == EStrategyOutcome::IN_PROGRESS);
                }
                UNIT_ASSERT(t.PutMask());
                t.ConfirmPuts();
                UNIT_ASSERT(t.RunRestore() == EStrategyOutcome::DONE);
            }
        }
    }

    Y_UNIT_TEST(BasicReadReturnsKnownDataDespiteEnsuredFailureModel) {
        TTest t({R::N1, R::F, R::F}); // E = 1; one error violates the model even with a found copy.
        UNIT_ASSERT(t.Run() == EStrategyOutcome::IN_PROGRESS);
        const auto request = t.Blackboard.GroupDiskRequests.GetsPending.front();
        const ui32 found = t.RequestedDisk();
        t.Blackboard.AddResponseData(request.Id, request.OrderNumber, 0, TRope(TString("testdata")));
        t.Reply((found + 3) % 9, S::Error);
        UNIT_ASSERT(t.Run() == EStrategyOutcome::DONE);
        UNIT_ASSERT(t.Blackboard[t.Id].WholeSituation == S::Present);
    }

    Y_UNIT_TEST(NormalKnownDataMatchesLegacyWithEveryOtherDiskFailureMask) {
        for (ui32 mask = 0; mask < 256; ++mask) {
            for (bool phantom : {false, true}) {
                TTest current({R::N0, R::N0, R::N0});
                TTest legacy({R::N0, R::N0, R::N0});
                for (auto* test : {&current, &legacy}) {
                    test->Blackboard.AddResponseData(TLogoBlobID(test->Id, 1),
                        test->Blackboard[test->Id].Disks[0].OrderNumber, 0, TRope(TString("testdata")));
                    for (ui32 disk = 1; disk < 9; ++disk) {
                        test->Reply(disk, mask & (1u << (disk - 1)) ? S::Error : S::Absent);
                    }
                }
                UNIT_ASSERT(current.Run(phantom) == EStrategyOutcome::DONE);
                UNIT_ASSERT(legacy.Run(phantom, true) == EStrategyOutcome::DONE);
                UNIT_ASSERT(current.Blackboard[current.Id].WholeSituation == S::Present);
                UNIT_ASSERT(legacy.Blackboard[legacy.Id].WholeSituation == S::Present);
                UNIT_ASSERT_VALUES_EQUAL(current.Blackboard[current.Id].Whole.Data.Read(0, 8).ConvertToString(),
                    legacy.Blackboard[legacy.Id].Whole.Data.Read(0, 8).ConvertToString());
                UNIT_ASSERT(current.Blackboard.GroupDiskRequests.GetsPending.empty());
            }
        }
    }

    Y_UNIT_TEST(PhantomCheckRequiresNoErrors) {
        for (bool phantom : {false, true}) {
            TTest t({R::N0, R::N0, R::N0});
            for (ui32 disk = 0; disk < 9; ++disk) {
                t.Reply(disk, disk == 0 ? S::Error : S::Absent);
            }
            UNIT_ASSERT(t.Run(phantom) == EStrategyOutcome::DONE);
            UNIT_ASSERT(t.Blackboard[t.Id].WholeSituation == (phantom ? S::Error : S::Absent));
        }
    }

    Y_UNIT_TEST(NormalRequestOrderAndSuccessfulReadMatchLegacy) {
        for (ui32 found = 0; found < 9; ++found) {
            TTest current({R::N0, R::N0, R::N0});
            TTest legacy({R::N0, R::N0, R::N0});
            for (ui32 step = 0; ; ++step) {
                UNIT_ASSERT(step <= 9);
                const auto outcome = current.Run();
                UNIT_ASSERT_VALUES_EQUAL(ui32(outcome.Value), ui32(legacy.Run(false, true).Value));
                if (outcome == EStrategyOutcome::DONE) {
                    UNIT_ASSERT(current.Blackboard[current.Id].WholeSituation == S::Present);
                    UNIT_ASSERT(legacy.Blackboard[legacy.Id].WholeSituation == S::Present);
                    UNIT_ASSERT_VALUES_EQUAL(current.Blackboard[current.Id].Whole.Data.Read(0, 8).ConvertToString(),
                        legacy.Blackboard[legacy.Id].Whole.Data.Read(0, 8).ConvertToString());
                    break;
                }
                const ui32 disk = current.RequestedDisk();
                UNIT_ASSERT_VALUES_EQUAL(disk, legacy.RequestedDisk());
                for (auto* test : {&current, &legacy}) {
                    if (disk == found) {
                        const auto request = test->Blackboard.GroupDiskRequests.GetsPending.front();
                        test->Blackboard.AddResponseData(request.Id, request.OrderNumber, 0, TRope(TString("testdata")));
                    } else {
                        test->Reply(disk, S::Absent);
                    }
                }
            }
        }
    }

    Y_UNIT_TEST(NormalReadsMatchLegacy) {
        for (bool phantom : {false, true}) {
            for (ui32 errors = 0; errors < 512; ++errors) {
                TTest current({R::N0, R::N0, R::N0});
                TTest legacy({R::N0, R::N0, R::N0});
                for (ui32 disk = 0; disk < 9; ++disk) {
                    const auto reply = errors & (1u << disk) ? S::Error : S::Absent;
                    current.Reply(disk, reply);
                    legacy.Reply(disk, reply);
                }
                const auto currentOutcome = current.Run(phantom);
                const auto legacyOutcome = legacy.Run(phantom, true);
                UNIT_ASSERT_VALUES_EQUAL_C(ui32(currentOutcome.Value), ui32(legacyOutcome.Value),
                    "errors " << errors << " phantom " << phantom);
                UNIT_ASSERT_VALUES_EQUAL_C(ui32(current.Blackboard[current.Id].WholeSituation),
                    ui32(legacy.Blackboard[legacy.Id].WholeSituation), "errors " << errors << " phantom " << phantom);
            }
        }
    }

    Y_UNIT_TEST(AllFailureMasksUseEnsuredAndNeverReadFailedRealms) {
        for (const auto states : {TStates{R::N0, R::N0, R::N0}, TStates{R::N2, R::N2, R::R},
                TStates{R::N1, R::N1, R::F}, TStates{R::N1, R::F, R::F}, TStates{R::N1, R::R, R::F}}) {
            for (ui32 mask = 0; mask < 512; ++mask) {
                TTest t(states);
                const auto checker = TMirror3dcWriteQuorumChecker::TryCreate(*t.Info, t.Id.Hash());
                UNIT_ASSERT(checker);
                G::TSubgroupVDisks failed(&t.Info->GetTopology());
                for (ui32 disk = 0; disk < 9; ++disk) {
                    if (mask & (1u << disk)) {
                        failed += G::TSubgroupVDisks(&t.Info->GetTopology(), disk);
                    }
                }
                const auto expected = checker->GetBlobState({}, failed);
                ui32 requested = 0;
                for (ui32 step = 0; ; ++step) {
                    UNIT_ASSERT(step <= 9);
                    const auto outcome = t.Run();
                    if (outcome != EStrategyOutcome::IN_PROGRESS) {
                        UNIT_ASSERT_VALUES_EQUAL(outcome == EStrategyOutcome::ERROR, expected == G::EBS_DISINTEGRATED);
                        if (outcome == EStrategyOutcome::DONE) {
                            UNIT_ASSERT(t.Blackboard[t.Id].WholeSituation == S::Absent);
                        }
                        break;
                    }
                    const ui32 disk = t.RequestedDisk();
                    UNIT_ASSERT(states[t.Realm(disk)] != R::F);
                    UNIT_ASSERT(!(requested & (1u << disk)));
                    requested |= 1u << disk;
                    t.Reply(disk, mask & (1u << disk) ? S::Error : S::Absent);
                }
            }
        }
    }

    Y_UNIT_TEST(NormalRestoreMasksMatchOutcomesButAvoidRedundantWrites) {
        bool sawRedundantLegacyWrite = false;
        for (ui32 mask = 0; mask < 512; ++mask) {
            TTest current({R::N0, R::N0, R::N0});
            TTest legacy({R::N0, R::N0, R::N0});
            for (auto* test : {&current, &legacy}) {
                for (ui32 disk = 0; disk < 9; ++disk) {
                    if (mask & (1u << disk)) {
                        test->Data(disk);
                    } else {
                        test->Reply(disk, S::Absent);
                    }
                }
            }
            const auto actual = current.RunRestore();
            const auto expected = legacy.RunRestore(false, true);
            UNIT_ASSERT_VALUES_EQUAL_C(ui32(current.Result(actual)), ui32(legacy.Result(expected)), "mask " << mask);
            if (actual == EStrategyOutcome::IN_PROGRESS) {
                ui32 missingRealms = 0;
                for (ui32 realm = 0; realm < 3; ++realm) {
                    if (!(mask & (0b001001001u << realm))) {
                        missingRealms |= 1u << realm;
                    }
                }
                // New Put reuses any known replica; legacy can rewrite the primary before visiting a handoff.
                UNIT_ASSERT_VALUES_EQUAL_C(current.PutMask(), missingRealms, "mask " << mask);
                UNIT_ASSERT_VALUES_EQUAL_C(legacy.PutMask(), (~mask & 0b111u), "mask " << mask);
                sawRedundantLegacyWrite |= current.PutMask() != legacy.PutMask();
                current.ConfirmPuts();
                legacy.ConfirmPuts();
                UNIT_ASSERT(current.RunRestore() == EStrategyOutcome::DONE);
                UNIT_ASSERT(legacy.RunRestore(false, true) == EStrategyOutcome::DONE);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(current.PutMask(), 0);
                UNIT_ASSERT_VALUES_EQUAL(legacy.PutMask(), 0);
            }
            UNIT_ASSERT(current.Blackboard[current.Id].WholeSituation == legacy.Blackboard[legacy.Id].WholeSituation);
        }
        UNIT_ASSERT(sawRedundantLegacyWrite);
    }

    Y_UNIT_TEST(NormalRestorePendingRepliesWithoutErrorsMatchLegacy) {
        for (ui32 mask = 0; mask < 256; ++mask) {
            TTest current({R::N0, R::N0, R::N0});
            TTest legacy({R::N0, R::N0, R::N0});
            for (auto* test : {&current, &legacy}) {
                test->Data(0);
                for (ui32 disk = 1; disk < 9; ++disk) {
                    if (!(mask & (1u << (disk - 1)))) {
                        test->Reply(disk, S::Absent);
                    }
                }
            }
            const auto actual = current.RunRestore();
            const auto expected = legacy.RunRestore(false, true);
            UNIT_ASSERT_VALUES_EQUAL_C(ui32(current.Result(actual)), ui32(legacy.Result(expected)), "mask " << mask);
            UNIT_ASSERT_VALUES_EQUAL_C(current.PutMask(), legacy.PutMask(), "mask " << mask);
            UNIT_ASSERT_VALUES_EQUAL(current.Blackboard.GroupDiskRequests.GetsPending.size(),
                legacy.Blackboard.GroupDiskRequests.GetsPending.size());
        }
    }

    Y_UNIT_TEST(NormalRestoreKnownCopyAndCompletedErrorRepliesMatchLegacy) {
        for (ui32 mask = 0; mask < 256; ++mask) {
            TTest current({R::N0, R::N0, R::N0});
            TTest legacy({R::N0, R::N0, R::N0});
            for (auto* test : {&current, &legacy}) {
                test->Data(0);
                for (ui32 disk = 1; disk < 9; ++disk) {
                    test->Reply(disk, mask & (1u << (disk - 1)) ? S::Error : S::Absent);
                }
            }
            const auto actual = current.RunRestore();
            const auto expected = legacy.RunRestore(false, true);
            UNIT_ASSERT_VALUES_EQUAL_C(ui32(current.Result(actual)), ui32(legacy.Result(expected)), "mask " << mask);
            UNIT_ASSERT_VALUES_EQUAL_C(current.PutMask(), legacy.PutMask(), "mask " << mask);
            if (actual == EStrategyOutcome::IN_PROGRESS) {
                current.ConfirmPuts();
                legacy.ConfirmPuts();
                UNIT_ASSERT(current.RunRestore() == EStrategyOutcome::DONE);
                UNIT_ASSERT(legacy.RunRestore(false, true) == EStrategyOutcome::DONE);
            }
        }
    }

    Y_UNIT_TEST(NormalBasicSlowDiskMasksMatchLegacy) {
        for (ui32 mask = 0; mask < 512; ++mask) {
            TTest current({R::N0, R::N0, R::N0});
            TTest legacy({R::N0, R::N0, R::N0});
            for (auto* test : {&current, &legacy}) {
                for (ui32 disk = 0; disk < 9; ++disk) {
                    test->Blackboard[test->Id].Disks[disk].IsSlow = mask & (1u << disk);
                }
            }
            UNIT_ASSERT(current.Run() == EStrategyOutcome::IN_PROGRESS);
            UNIT_ASSERT(legacy.Run(false, true) == EStrategyOutcome::IN_PROGRESS);
            UNIT_ASSERT_VALUES_EQUAL_C(current.RequestedDisk(), legacy.RequestedDisk(), "mask " << mask);
        }
    }

    Y_UNIT_TEST(NormalLocationAndSlowDiskOrderMatchesLegacy) {
        const NActors::TNodeLocation self("dc1", "module1", "rack1", "unit1");
        const std::array<NActors::TNodeLocation, 9> locations = {
            NActors::TNodeLocation("dc2"), NActors::TNodeLocation("dc1", "module2"), self,
            NActors::TNodeLocation(), NActors::TNodeLocation("dc1", "module1", "rack2"), self,
            NActors::TNodeLocation("dc1", "module1", "rack1", "unit2"), self, NActors::TNodeLocation("dc3"),
        };
        for (ui32 mask = 0; mask < 512; ++mask) {
            TTest current({R::N0, R::N0, R::N0});
            TTest legacy({R::N0, R::N0, R::N0});
            for (auto* test : {&current, &legacy}) {
                THashMap<ui32, NActors::TNodeLocation> nodeLocations;
                test->NodeLayout = MakeIntrusive<TNodeLayoutInfo>(self, test->Info, nodeLocations);
                for (ui32 disk = 0; disk < 9; ++disk) {
                    auto& state = test->Blackboard[test->Id].Disks[disk];
                    state.IsSlow = mask & (1u << disk);
                    test->NodeLayout->LocationPerOrderNumber[state.OrderNumber] = locations[disk];
                }
            }
            UNIT_ASSERT(current.Run() == EStrategyOutcome::IN_PROGRESS);
            UNIT_ASSERT(legacy.Run(false, true) == EStrategyOutcome::IN_PROGRESS);
            UNIT_ASSERT_VALUES_EQUAL_C(current.RequestedDisk(), legacy.RequestedDisk(), "mask " << mask);
        }
    }

    Y_UNIT_TEST(NormalMergedRangesMatchLegacy) {
        TTest current({R::N0, R::N0, R::N0}, 0, 0, 2);
        TTest legacy({R::N0, R::N0, R::N0}, 0, 0, 2);
        for (auto* test : {&current, &legacy}) {
            test->Blackboard.AddNeeded(test->Id, 1, 2);
            test->Blackboard.AddNeeded(test->Id, 5, 2);
        }
        UNIT_ASSERT(current.Run() == EStrategyOutcome::IN_PROGRESS);
        UNIT_ASSERT(legacy.Run(false, true) == EStrategyOutcome::IN_PROGRESS);
        const auto& actual = current.Blackboard.GroupDiskRequests.GetsPending;
        const auto& expected = legacy.Blackboard.GroupDiskRequests.GetsPending;
        UNIT_ASSERT_VALUES_EQUAL(actual.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(actual.size(), expected.size());
        for (ui32 i = 0; i < actual.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(actual[i].OrderNumber, expected[i].OrderNumber);
            UNIT_ASSERT_VALUES_EQUAL(actual[i].Shift, expected[i].Shift);
            UNIT_ASSERT_VALUES_EQUAL(actual[i].Size, expected[i].Size);
        }
        for (auto* test : {&current, &legacy}) {
            for (const auto& request : test->Blackboard.GroupDiskRequests.GetsPending) {
                test->Blackboard.AddResponseData(request.Id, request.OrderNumber, request.Shift,
                    TRope(TString("testdata").substr(request.Shift, request.Size)));
            }
        }
        UNIT_ASSERT(current.Run() == EStrategyOutcome::DONE);
        UNIT_ASSERT(legacy.Run(false, true) == EStrategyOutcome::DONE);
    }

    Y_UNIT_TEST(NormalExcessiveErrorsWithPendingReadsMatchLegacy) {
        for (bool restore : {false, true}) {
            for (ui32 errors = 0; errors < 512; ++errors) {
                TTest current({R::N0, R::N0, R::N0});
                TTest legacy({R::N0, R::N0, R::N0});
                for (ui32 disk = 0; disk < 9; ++disk) {
                    if (errors & (1u << disk)) {
                        current.Reply(disk, S::Error);
                        legacy.Reply(disk, S::Error);
                    }
                }
                const auto actual = restore ? current.RunRestore() : current.Run();
                const auto expected = restore ? legacy.RunRestore(false, true) : legacy.Run(false, true);
                UNIT_ASSERT_VALUES_EQUAL_C(ui32(current.Result(actual)), ui32(legacy.Result(expected)),
                    "errors " << errors << " restore " << restore);
                if (actual == EStrategyOutcome::ERROR) {
                    UNIT_ASSERT(current.Blackboard.GroupDiskRequests.GetsPending.empty());
                    UNIT_ASSERT(current.Blackboard.GroupDiskRequests.PutsPending.empty());
                }
            }
        }
    }

    Y_UNIT_TEST(NormalRestoreErrorsAndPendingRepliesMatchLegacy) {
        TTest current({R::N0, R::N0, R::N0});
        TTest legacy({R::N0, R::N0, R::N0});
        for (auto* test : {&current, &legacy}) {
            test->Data(0);
            test->Reply(3, S::Error);
            for (ui32 disk : {4u, 5u, 6u, 7u, 8u}) {
                test->Reply(disk, S::Absent);
            }
        }
        // Error at 3 and pending reads at 1,2 jointly hide E=1+1+1; separately they do not.
        UNIT_ASSERT(current.RunRestore() == EStrategyOutcome::IN_PROGRESS);
        UNIT_ASSERT(legacy.RunRestore(false, true) == EStrategyOutcome::IN_PROGRESS);
        UNIT_ASSERT_VALUES_EQUAL(current.PutMask(), legacy.PutMask());
        UNIT_ASSERT(current.PutMask());
        current.ConfirmPuts();
        legacy.ConfirmPuts();
        UNIT_ASSERT(current.RunRestore() == EStrategyOutcome::DONE);
        UNIT_ASSERT(legacy.RunRestore(false, true) == EStrategyOutcome::DONE);
    }

    Y_UNIT_TEST(NormalBasicNotYetRepliesMatchLegacy) {
        for (bool phantom : {false, true}) {
            for (bool error : {false, true}) {
                for (ui32 mask = 0; mask < 512; ++mask) {
                    TTest current({R::N0, R::N0, R::N0});
                    TTest legacy({R::N0, R::N0, R::N0});
                    for (ui32 disk = 0; disk < 9; ++disk) {
                        const auto reply = error && disk == 0 ? S::Error
                            : mask & (1u << disk) ? S::Lost : S::Absent;
                        current.Reply(disk, reply);
                        legacy.Reply(disk, reply);
                    }
                    const auto actual = current.Run(phantom);
                    const auto expected = legacy.Run(phantom, true);
                    UNIT_ASSERT_VALUES_EQUAL_C(ui32(current.Result(actual)), ui32(legacy.Result(expected)),
                        "mask " << mask << " error " << error << " phantom " << phantom);
                    if (!error) {
                        UNIT_ASSERT(actual == EStrategyOutcome::DONE);
                        UNIT_ASSERT(current.Blackboard[current.Id].WholeSituation == S::Absent);
                    }
                }
            }
        }
    }

    Y_UNIT_TEST(NormalRestoreNotYetWithKnownDataMatchesLegacy) {
        for (bool error : {false, true}) {
            for (ui32 mask = 0; mask < 256; ++mask) {
                TTest current({R::N0, R::N0, R::N0});
                TTest legacy({R::N0, R::N0, R::N0});
                for (auto* test : {&current, &legacy}) {
                    test->Data(0);
                    for (ui32 disk = 1; disk < 9; ++disk) {
                        test->Reply(disk, error && disk == 3 ? S::Error
                            : mask & (1u << (disk - 1)) ? S::Lost : S::Absent);
                    }
                }
                UNIT_ASSERT(current.RunRestore() == EStrategyOutcome::IN_PROGRESS);
                UNIT_ASSERT(legacy.RunRestore(false, true) == EStrategyOutcome::IN_PROGRESS);
                UNIT_ASSERT_VALUES_EQUAL_C(current.PutMask(), legacy.PutMask(), "mask " << mask << " error " << error);
                UNIT_ASSERT(current.PutMask());
                current.ConfirmPuts();
                legacy.ConfirmPuts();
                UNIT_ASSERT(current.RunRestore() == EStrategyOutcome::DONE);
                UNIT_ASSERT(legacy.RunRestore(false, true) == EStrategyOutcome::DONE);
            }
        }
    }

    Y_UNIT_TEST(NormalRestoreNotYetWithoutDataMatchesLegacy) {
        for (ui32 mask = 0; mask < 512; ++mask) {
            TTest current({R::N0, R::N0, R::N0});
            TTest legacy({R::N0, R::N0, R::N0});
            for (ui32 disk = 0; disk < 9; ++disk) {
                current.Reply(disk, mask & (1u << disk) ? S::Lost : S::Absent);
                legacy.Reply(disk, mask & (1u << disk) ? S::Lost : S::Absent);
            }
            const auto actual = current.RunRestore();
            const auto expected = legacy.RunRestore(false, true);
            UNIT_ASSERT_VALUES_EQUAL_C(ui32(current.Result(actual)), ui32(legacy.Result(expected)), "mask " << mask);
            UNIT_ASSERT_VALUES_EQUAL(current.PutMask(), 0);
        }
    }

    Y_UNIT_TEST(RestoreKnownCopyDespiteNotYetInActiveRealms) {
        for (const auto states : {TStates{R::N1, R::F, R::F}, TStates{R::N2, R::N2, R::R},
                TStates{R::N1, R::N1, R::F}, TStates{R::N1, R::R, R::F}}) {
            for (ui32 cookie = 0; cookie < 3; ++cookie) {
                TTest t(states, cookie);
                bool found = false;
                for (ui32 disk = 0; disk < 9; ++disk) {
                    if (!found && states[t.Realm(disk)] != R::F) {
                        t.Data(disk);
                        found = true;
                    } else {
                        t.Reply(disk, S::Lost);
                    }
                }
                UNIT_ASSERT(t.RunRestore() == EStrategyOutcome::IN_PROGRESS);
                UNIT_ASSERT(t.PutMask());
                t.ConfirmPuts();
                UNIT_ASSERT(t.RunRestore() == EStrategyOutcome::DONE);
                UNIT_ASSERT(t.Blackboard[t.Id].WholeSituation == S::Present);
            }
        }
    }

    Y_UNIT_TEST(InvalidStateDoesNotSendReads) {
        TTest t({R::N0, R::R, R::R});
        UNIT_ASSERT(t.Run() == EStrategyOutcome::ERROR);
        UNIT_ASSERT(t.Blackboard.GroupDiskRequests.GetsPending.empty());
    }
}

} // namespace NKikimr
