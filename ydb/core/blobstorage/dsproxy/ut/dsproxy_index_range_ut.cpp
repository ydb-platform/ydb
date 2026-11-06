#include "dsproxy_env_mock_ut.h"
#include "dsproxy_test_state_ut.h"

#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_mirror3dc_state.h>

using TDataCenterState = NKikimrBlobStorage::TDataCenterState;

namespace NKikimr {
namespace {

using TCounts = std::array<ui32, 3>;

enum class ERead { Range, Index, Get, Restore };

void CheckRead(ERead operation, const TMirror3dcGroupState& realms, const TCounts& copies,
        const TCounts& failures, NKikimrProto::EReplyStatus expected, bool phantomCheck = false,
        bool expectRestore = false, bool expectBlobError = false) {
    const bool range = operation == ERead::Range;
    ui32 writes = 0;
    TTestBasicRuntime runtime;
    runtime.SetDispatchTimeout(TDuration::Seconds(10));
    SetupRuntime(runtime);
    TDSProxyEnv env;
    env.Configure(runtime, TBlobStorageGroupType(TErasureType::ErasureMirror3dc),
        0, 0, TBlobStorageGroupInfo::EEM_NONE, DormantTimeoutDefaultControl, false, TVector<EMirror3dcFailRealmState>(realms.FailRealmStates.begin(), realms.FailRealmStates.end()), realms.State);
    TTestState state(runtime, env.Info);
    auto& group = state.GetGroupMock();
    const TString data = "testdata";
    const TLogoBlobID id(72075186224047637, 1, 1, 0, data.size(), 1);
    for (ui32 disk = 0; disk < 9; ++disk) {
        const auto vdisk = env.Info->GetVDiskInSubgroup(disk, id.Hash());
        const ui32 realm = vdisk.FailRealm;
        UNIT_ASSERT(copies[realm] + failures[realm] <= 3);
        if (disk / 3 < copies[realm]) {
            group.PutPartAtSlot(id, disk, disk % 3 + 1, data);
        }
        if (disk / 3 >= 3 - failures[realm]) {
            group.SetError(vdisk, NKikimrProto::ERROR);
        }
    }
    if (range) {
        auto query = std::make_unique<TEvBlobStorage::TEvRange>(id.TabletID(), id, id,
            false, TInstant::Max(), true);
        runtime.Send(new IEventHandle(env.RealProxyActorId, state.EdgeActor, query.release()), 0, true);
    } else {
        auto query = std::make_unique<TEvBlobStorage::TEvGet>(id, 0, 0, TInstant::Max(),
            NKikimrBlobStorage::FastRead, operation != ERead::Get, operation == ERead::Index);
        query->PhantomCheck = phantomCheck;
        runtime.Send(new IEventHandle(env.RealProxyActorId, state.EdgeActor, query.release()), 0, true);
    }
    for (;;) {
        TAutoPtr<IEventHandle> handle;
        auto [get, put, rangeResult, indexResult] = runtime.GrabEdgeEventsRethrow<TEvBlobStorage::TEvVGet, TEvBlobStorage::TEvVPut,
            TEvBlobStorage::TEvRangeResult, TEvBlobStorage::TEvGetResult>(handle);
        if (get) {
            const auto vdisk = VDiskIDFromVDiskID(get->Record.GetVDiskID());
            UNIT_ASSERT(realms.FailRealmStates[vdisk.FailRealm] != TDataCenterState::EXCLUDED);
            const ui32 disk = env.Info->GetIdxInSubgroup(vdisk, id.Hash());
            TEvBlobStorage::TEvVGet::TPtr request(
                static_cast<TEventHandle<TEvBlobStorage::TEvVGet>*>(handle.Release()));
            auto reply = state.CreateEventResultPtr(request, NKikimrProto::UNKNOWN);
            if (range || !get->Record.GetIndexOnly() || disk / 3 >= 3 - failures[vdisk.FailRealm]) {
                group.OnVGet(*request->Get(), *reply->Get());
            } else {
                // Index VGet replies identify the full blob and describe local parts in ingress.
                reply->Get()->Record.SetStatus(NKikimrProto::OK);
                reply->Get()->Record.SetCookie(get->Record.GetCookie());
                const bool present = disk / 3 < copies[vdisk.FailRealm];
                TIngress ingress;
                if (present) {
                    ingress = *TIngress::CreateIngressWithLocal(&env.Info->GetTopology(),
                        TVDiskIdShort(vdisk), TLogoBlobID(id, disk % 3 + 1));
                }
                ui64 raw = ingress.Raw();
                ui64 cookie = get->Record.GetExtremeQueries(0).GetCookie();
                const auto local = ingress.LocalParts(env.Info->Type);
                reply->Get()->AddResult(present ? NKikimrProto::OK : NKikimrProto::NODATA,
                    id, &cookie, &raw, &local, false, false);
            }
            runtime.Send(reply.Release());
        } else if (put) {
            const auto vdisk = VDiskIDFromVDiskID(put->Record.GetVDiskID());
            UNIT_ASSERT(realms.FailRealmStates[vdisk.FailRealm] != TDataCenterState::EXCLUDED);
            TEvBlobStorage::TEvVPut::TPtr request(
                static_cast<TEventHandle<TEvBlobStorage::TEvVPut>*>(handle.Release()));
            auto reply = state.CreateEventResultPtr(request, group.OnVPut(*request->Get()), vdisk);
            runtime.Send(reply.Release());
            ++writes;
        } else if (rangeResult) {
            UNIT_ASSERT(range);
            UNIT_ASSERT_VALUES_EQUAL(rangeResult->Status,
                expected == NKikimrProto::NODATA ? NKikimrProto::OK : expected);
            if (rangeResult->Status == NKikimrProto::OK) {
                UNIT_ASSERT_VALUES_EQUAL(rangeResult->Responses.size(), expected == NKikimrProto::OK ? 1 : 0);
            }
            break;
        } else {
            UNIT_ASSERT(!range && indexResult);
            UNIT_ASSERT_VALUES_EQUAL(indexResult->Status,
                expected == NKikimrProto::NODATA || expectBlobError ? NKikimrProto::OK : expected);
            if (indexResult->Status == NKikimrProto::OK) {
                UNIT_ASSERT_VALUES_EQUAL(indexResult->ResponseSz, 1);
                UNIT_ASSERT_VALUES_EQUAL(indexResult->Responses[0].Status, expected);
                if (expected == NKikimrProto::OK && operation != ERead::Index && !phantomCheck) {
                    UNIT_ASSERT_VALUES_EQUAL(indexResult->Responses[0].Buffer.ConvertToString(), data);
                }
                if (phantomCheck && expected == NKikimrProto::NODATA) {
                    UNIT_ASSERT(indexResult->Responses[0].LooksLikePhantom.value_or(false));
                }
            }
            break;
        }
    }
    if (expectRestore) {
        UNIT_ASSERT(writes > 0);
    }
}

void CheckPut(const TMirror3dcGroupState& realms, const TCounts& failures, NKikimrProto::EReplyStatus expected) {
    TTestBasicRuntime runtime;
    runtime.SetDispatchTimeout(TDuration::Seconds(10));
    SetupRuntime(runtime);
    TDSProxyEnv env;
    env.Configure(runtime, TBlobStorageGroupType(TErasureType::ErasureMirror3dc),
        0, 0, TBlobStorageGroupInfo::EEM_NONE, DormantTimeoutDefaultControl, false, TVector<EMirror3dcFailRealmState>(realms.FailRealmStates.begin(), realms.FailRealmStates.end()), realms.State);
    TTestState state(runtime, env.Info);
    auto& group = state.GetGroupMock();
    const TString data = "testdata";
    const TLogoBlobID id(72075186224047637, 1, 1, 0, data.size(), 1);
    for (ui32 disk = 0; disk < 9; ++disk) {
        const auto vdisk = env.Info->GetVDiskInSubgroup(disk, id.Hash());
        if (disk / 3 < failures[vdisk.FailRealm]) {
            group.SetError(vdisk, NKikimrProto::ERROR);
        }
    }
    const TBlobTestSet::TBlob blob(id, data);
    auto query = state.CreatePutRequest(blob, TEvBlobStorage::TEvPut::TacticMaxThroughput,
        NKikimrBlobStorage::TabletLog);
    runtime.Register(env.CreatePutRequestActor(query).release());
    for (;;) {
        TAutoPtr<IEventHandle> handle;
        auto [put, result] = runtime.GrabEdgeEventsRethrow<TEvBlobStorage::TEvVPut,
            TEvBlobStorage::TEvPutResult>(handle);
        if (result) {
            UNIT_ASSERT_VALUES_EQUAL(result->Status, expected);
            break;
        }
        UNIT_ASSERT(put);
        const auto vdisk = VDiskIDFromVDiskID(put->Record.GetVDiskID());
        UNIT_ASSERT(realms.FailRealmStates[vdisk.FailRealm] != TDataCenterState::EXCLUDED);
        TEvBlobStorage::TEvVPut::TPtr request(
            static_cast<TEventHandle<TEvBlobStorage::TEvVPut>*>(handle.Release()));
        auto reply = state.CreateEventResultPtr(request, group.OnVPut(*request->Get()), vdisk);
        runtime.Send(reply.Release());
    }
}

} // namespace

Y_UNIT_TEST_SUITE(TDSProxyMirror3dcIndexRange) {
    Y_UNIT_TEST(NormalAndSoloWriteQuorumsReturnOk) {
        for (bool range : {false, true}) {
            CheckRead(range ? ERead::Range : ERead::Index, {NKikimrBlobStorage::TGroupState::NORMAL, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::INCLUDED}}, {1, 1, 1}, {}, NKikimrProto::OK);
            CheckRead(range ? ERead::Range : ERead::Index, {NKikimrBlobStorage::TGroupState::DC_SOLO_INV1, {TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED, TDataCenterState::EXCLUDED}}, {2, 0, 0}, {}, NKikimrProto::OK);
            CheckRead(range ? ERead::Range : ERead::Index, {NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV1, {TDataCenterState::INCLUDED, TDataCenterState::RECOVERING, TDataCenterState::EXCLUDED}}, {2, 2, 0}, {}, NKikimrProto::OK);
        }
    }

    Y_UNIT_TEST(EnsuredLayoutControlsGroupFailures) {
        for (bool range : {false, true}) {
            CheckRead(range ? ERead::Range : ERead::Index, {NKikimrBlobStorage::TGroupState::DC_SOLO_INV1, {TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED, TDataCenterState::EXCLUDED}}, {}, {}, NKikimrProto::NODATA);
            CheckRead(range ? ERead::Range : ERead::Index, {NKikimrBlobStorage::TGroupState::DC_SOLO_INV1, {TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED, TDataCenterState::EXCLUDED}}, {}, {1, 0, 0}, NKikimrProto::ERROR);
            CheckRead(range ? ERead::Range : ERead::Index, {NKikimrBlobStorage::TGroupState::DC_SOLO_INV2, {TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED, TDataCenterState::EXCLUDED}}, {}, {1, 0, 0}, NKikimrProto::NODATA);
            CheckRead(range ? ERead::Range : ERead::Index, {NKikimrBlobStorage::TGroupState::DC_RECOVERY_INV1, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::RECOVERING}}, {}, {1, 1, 3}, NKikimrProto::ERROR);
            CheckRead(range ? ERead::Range : ERead::Index, {NKikimrBlobStorage::TGroupState::DC_RECOVERY_INV1, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::RECOVERING}}, {}, {2, 2, 3}, NKikimrProto::ERROR);
        }
    }

    Y_UNIT_TEST(AbsenceDependsOnNormalRealmsInEnsuredPair) {
        for (const ERead operation : {ERead::Range, ERead::Index, ERead::Get, ERead::Restore}) {
            CheckRead(operation, {NKikimrBlobStorage::TGroupState::DC_RECOVERY_INV1, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::RECOVERING}}, {}, {1, 1, 1}, NKikimrProto::ERROR);
            CheckRead(operation, {NKikimrBlobStorage::TGroupState::DC_RECOVERY_INV1, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::RECOVERING}}, {}, {2, 0, 2}, NKikimrProto::NODATA);
            CheckRead(operation, {NKikimrBlobStorage::TGroupState::DC_RECOVERY_INV1, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::RECOVERING}}, {}, {1, 1, 0}, NKikimrProto::ERROR);
            CheckRead(operation, {NKikimrBlobStorage::TGroupState::DC_RECOVERY_INV1, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::RECOVERING}}, {}, {0, 0, 3}, NKikimrProto::NODATA);
        }
    }

    Y_UNIT_TEST(OrdinaryGetReadsNormalAndRecoveringRealms) {
        CheckRead(ERead::Get, {NKikimrBlobStorage::TGroupState::DC_SOLO_INV1, {TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED, TDataCenterState::EXCLUDED}}, {1, 0, 0}, {}, NKikimrProto::OK);
        CheckRead(ERead::Get, {NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV1, {TDataCenterState::INCLUDED, TDataCenterState::RECOVERING, TDataCenterState::EXCLUDED}}, {0, 1, 0}, {}, NKikimrProto::OK);
    }

    Y_UNIT_TEST(PhantomCheckSkipsFailedRealmsAndRejectsRecoveryErrors) {
        CheckRead(ERead::Get, {NKikimrBlobStorage::TGroupState::DC_SOLO_INV1, {TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED, TDataCenterState::EXCLUDED}}, {}, {}, NKikimrProto::NODATA, true);
        CheckRead(ERead::Get, {NKikimrBlobStorage::TGroupState::DC_RECOVERY_INV1, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::RECOVERING}}, {}, {0, 0, 3}, NKikimrProto::ERROR, true, false, true);
    }

    Y_UNIT_TEST(PartialCopiesAreRestoredThroughNewPutStrategy) {
        for (const auto operation : {ERead::Index, ERead::Restore}) {
            CheckRead(operation, {NKikimrBlobStorage::TGroupState::DC_SOLO_INV1, {TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED, TDataCenterState::EXCLUDED}}, {1, 0, 0}, {}, NKikimrProto::OK, false, true);
            CheckRead(operation, {NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV1, {TDataCenterState::INCLUDED, TDataCenterState::RECOVERING, TDataCenterState::EXCLUDED}}, {1, 0, 0}, {}, NKikimrProto::OK, false, true);
        }
    }

    Y_UNIT_TEST(OrdinaryPutChecksFailureModelAndWriteQuorumAndSkipsFailedRealms) {
        CheckPut({NKikimrBlobStorage::TGroupState::NORMAL, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::INCLUDED}}, {1, 1, 1}, NKikimrProto::ERROR);
        CheckPut({NKikimrBlobStorage::TGroupState::DC_SOLO_INV1, {TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED, TDataCenterState::EXCLUDED}}, {1, 0, 0}, NKikimrProto::ERROR);
        CheckPut({NKikimrBlobStorage::TGroupState::DC_SOLO_INV2, {TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED, TDataCenterState::EXCLUDED}}, {1, 0, 0}, NKikimrProto::OK);
        CheckPut({NKikimrBlobStorage::TGroupState::DC_SOLO_INV1, {TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED, TDataCenterState::EXCLUDED}}, {2, 0, 0}, NKikimrProto::ERROR);
        CheckPut({NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV1, {TDataCenterState::INCLUDED, TDataCenterState::RECOVERING, TDataCenterState::EXCLUDED}}, {1, 1, 0}, NKikimrProto::ERROR);
        CheckPut({NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV1, {TDataCenterState::INCLUDED, TDataCenterState::RECOVERING, TDataCenterState::EXCLUDED}}, {0, 1, 0}, NKikimrProto::OK);
        CheckPut({NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV1, {TDataCenterState::INCLUDED, TDataCenterState::RECOVERING, TDataCenterState::EXCLUDED}}, {0, 2, 0}, NKikimrProto::ERROR);
    }

}

} // namespace NKikimr
