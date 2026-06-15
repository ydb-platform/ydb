#include "defs.h"
#include "dsproxy_env_mock_ut.h"
#include "dsproxy_vdisk_mock_ut.h"
#include "dsproxy_test_state_ut.h"

#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>

#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/actor_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NDSProxyCheckIntegrityTest {

namespace {

using TEvCheckIntegrityResult = TEvBlobStorage::TEvCheckIntegrityResult;
using EPlacementStatus = TEvCheckIntegrityResult::EPlacementStatus;
using EDataStatus = TEvCheckIntegrityResult::EDataStatus;

enum class EDiskReply : ui32 {
    ND   = 0, // NODATA
    OK1  = 1,  OK2  = 2,  OK3  = 3,  OK4  = 4,  OK5  = 5,  OK6  = 6,  // The numbers encode PartId
    RND1 = 11, RND2 = 12, RND3 = 13, RND4 = 14, RND5 = 15, RND6 = 16, // OK, but random data (corrupted)
    NY1  = 21, NY2  = 22, NY3  = 23, NY4  = 24, NY5  = 25, NY6  = 26, // NOT_YET
    ERR  = 99, // including timeout
};
using enum EDiskReply;

struct TFixture {
    TTestBasicRuntime Runtime;
    TDSProxyEnv Env;
    std::unique_ptr<TTestState> TestState;
    TBlobStorageGroupType GroupType;

    explicit TFixture(TBlobStorageGroupType::EErasureSpecies erasure)
        : Runtime(1, false)
        , GroupType(erasure)
    {
        Runtime.SetDispatchTimeout(TDuration::Seconds(5));
        SetupRuntime(Runtime);
        Env.Configure(Runtime, GroupType, 0, 0, TBlobStorageGroupInfo::EEM_NONE);
        TestState.reset(new TTestState(Runtime, Env.Info));
    }

    ui32 SubgroupSize() const { return GroupType.BlobSubgroupSize(); }
    ui32 TotalParts() const { return GroupType.TotalPartCount(); }

    TLogoBlobID MakeBlobId(ui64 tabletId) const {
        return TLogoBlobID(tabletId, 1, 1, 0, TestState->BlobSize, 0);
    }

    void SendCheckIntegrity(TLogoBlobID id, TInstant deadline = TInstant::Max()) {
        auto ev = std::make_unique<TEvBlobStorage::TEvCheckIntegrity>(
            id, deadline, NKikimrBlobStorage::FastRead, false);
        Runtime.Send(new IEventHandle(Env.RealProxyActorId, TestState->EdgeActor,
            ev.release()), 0, true);
    }

    TEvCheckIntegrityResult* GrabResult(TAutoPtr<IEventHandle>& handle) {
        auto* result = Runtime.GrabEdgeEventRethrow<TEvCheckIntegrityResult>(handle);
        Cerr << (result ? result->ToString() : "<null>") << Endl;
        return result;
    }

    void SetupAndRun(TLogoBlobID blobId, std::initializer_list<EDiskReply> pattern) {
        UNIT_ASSERT_VALUES_EQUAL(pattern.size(), SubgroupSize());

        auto& gm = TestState->GetGroupMock();
        ui32 slot = 0;
        for (auto reply : pattern) {
            ui32 partId = 0;
            switch (reply) {
                case ND: break;
                case ERR: break;
                case OK1: case RND1: case NY1: partId = 1; break;
                case OK2: case RND2: case NY2: partId = 2; break;
                case OK3: case RND3: case NY3: partId = 3; break;
                case OK4: case RND4: case NY4: partId = 4; break;
                case OK5: case RND5: case NY5: partId = 5; break;
                case OK6: case RND6: case NY6: partId = 6; break;
            }

            switch (reply) {
                case ND:
                    break;
                case ERR: {
                    TVDiskID vDiskId = Env.Info->GetVDiskInSubgroup(slot, blobId.Hash());
                    gm.SetError(vDiskId, NKikimrProto::ERROR);
                    break;
                }
                case OK1: case OK2: case OK3: case OK4: case OK5: case OK6:
                    gm.PutPartAtSlot(blobId, slot, partId, TestState->BlobData);
                    break;
                case RND1: case RND2: case RND3: case RND4: case RND5: case RND6:
                    gm.PutPartAtSlot(blobId, slot, partId, TestState->BlobData);
                    gm.CorruptPart(blobId, slot, partId);
                    break;
                case NY1: case NY2: case NY3: case NY4: case NY5: case NY6:
                    gm.PutPartAtSlot(blobId, slot, partId, TestState->BlobData);
                    gm.MarkPartNotYet(blobId, slot, partId);
                    break;
            }
            ++slot;
        }

        SendCheckIntegrity(blobId);
        TestState->HandleVGetsWithMock(SubgroupSize());
    }
};

////////////////////////////////////////////////////////////////////////////////
// Assertion helpers
////////////////////////////////////////////////////////////////////////////////

void AssertResultOk(TEvCheckIntegrityResult* r, TLogoBlobID expectedId,
        EPlacementStatus ps, EDataStatus ds)
{
    UNIT_ASSERT(r);
    UNIT_ASSERT_VALUES_EQUAL(r->Status, NKikimrProto::OK);
    UNIT_ASSERT_VALUES_EQUAL(r->Id, expectedId);
    UNIT_ASSERT_VALUES_EQUAL_C(static_cast<int>(r->PlacementStatus), static_cast<int>(ps),
        "expected " << TEvCheckIntegrityResult::PlacementStatusToString(ps)
        << " but got " << TEvCheckIntegrityResult::PlacementStatusToString(r->PlacementStatus));
    UNIT_ASSERT_VALUES_EQUAL_C(static_cast<int>(r->DataStatus), static_cast<int>(ds),
        "expected " << TEvCheckIntegrityResult::DataStatusToString(ds)
        << " but got " << TEvCheckIntegrityResult::DataStatusToString(r->DataStatus));
}

void AssertActorError(TEvCheckIntegrityResult* r) {
    UNIT_ASSERT(r);
    UNIT_ASSERT_VALUES_EQUAL(r->Status, NKikimrProto::ERROR);
    UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(r->PlacementStatus),
        static_cast<int>(TEvCheckIntegrityResult::PS_UNKNOWN));
    UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(r->DataStatus),
        static_cast<int>(TEvCheckIntegrityResult::DS_UNKNOWN));
    UNIT_ASSERT(!r->ErrorReason.empty());
}

} // anonymous namespace

////////////////////////////////////////////////////////////////////////////////
// ErasureNone
////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDSProxyCheckIntegrityErasureNone) {

Y_UNIT_TEST(PlacementOk) {
    TFixture fx(TBlobStorageGroupType::ErasureNone);
    auto id = fx.MakeBlobId(100001);
    fx.SetupAndRun(id, {OK1});

    TAutoPtr<IEventHandle> h;
    AssertResultOk(fx.GrabResult(h), id,
        TEvCheckIntegrityResult::PS_OK,
        TEvCheckIntegrityResult::DS_OK);
}

Y_UNIT_TEST(PlacementBlobIsLost) {
    // The only replica is missing and no disk has errored -> blob is lost.
    TFixture fx(TBlobStorageGroupType::ErasureNone);
    auto id = fx.MakeBlobId(100002);
    fx.SetupAndRun(id, {ND});

    TAutoPtr<IEventHandle> h;
    auto* r = fx.GrabResult(h);
    AssertResultOk(r, id,
        TEvCheckIntegrityResult::PS_BLOB_IS_LOST,
        TEvCheckIntegrityResult::DS_OK);
}

Y_UNIT_TEST(PlacementUnknownOnError) {
    // The single disk replied with ERROR. QuorumTracker says ERROR for the
    // whole group (only disk is faulty) -> actor replies Status=ERROR.
    TFixture fx(TBlobStorageGroupType::ErasureNone);
    auto id = fx.MakeBlobId(100003);
    fx.SetupAndRun(id, {ERR});

    TAutoPtr<IEventHandle> h;
    auto* r = fx.GrabResult(h);
    AssertActorError(r);
    UNIT_ASSERT_VALUES_EQUAL(r->Id, id);
}

} // ErasureNone

////////////////////////////////////////////////////////////////////////////////
// Block 4+2
//
// Subgroup size = 8 (6 parts + 2 handoff). Quorum:
//   EBS_FULL                      effectiveReplicas == 6
//   EBS_RECOVERABLE_FRAGMENTARY   distinctParts >= 4
//   EBS_UNRECOVERABLE_FRAGMENTARY distinctParts < 4
////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDSProxyCheckIntegrityBlock42) {

Y_UNIT_TEST(PlacementOkAllPartsPresent) {
    // All 6 parts present at their canonical primary slots, both handoff
    // slots empty -> EBS_FULL.
    TFixture fx(TBlobStorageGroupType::Erasure4Plus2Block);
    auto id = fx.MakeBlobId(200001);
    fx.SetupAndRun(id, {OK1, OK2, OK3, OK4, OK5, OK6, ND, ND});

    TAutoPtr<IEventHandle> h;
    AssertResultOk(fx.GrabResult(h), id,
        TEvCheckIntegrityResult::PS_OK,
        TEvCheckIntegrityResult::DS_OK);
}

Y_UNIT_TEST(PlacementOkHandoffsUsed) {
    // Two primary slots (0, 1) carry no data; their parts (1, 2) live on the
    // two handoff slots (6, 7) instead.  All 6 distinct partIds are still
    // present on 6 distinct disks -> EBS_FULL.
    TFixture fx(TBlobStorageGroupType::Erasure4Plus2Block);
    auto id = fx.MakeBlobId(200101);
    fx.SetupAndRun(id, {ND, ND, OK3, OK4, OK5, OK6, OK1, OK2});

    TAutoPtr<IEventHandle> h;
    AssertResultOk(fx.GrabResult(h), id,
        TEvCheckIntegrityResult::PS_OK,
        TEvCheckIntegrityResult::DS_OK);
}

Y_UNIT_TEST(PlacementOkOneErrorOnHandoff) {
    // All 6 parts present on primary slots; one handoff disk is errored.
    // Failure model tolerates 1 errored disk -> still EBS_FULL.
    TFixture fx(TBlobStorageGroupType::Erasure4Plus2Block);
    auto id = fx.MakeBlobId(200002);
    fx.SetupAndRun(id, {OK1, OK2, OK3, OK4, OK5, OK6, ND, ERR});

    TAutoPtr<IEventHandle> h;
    AssertResultOk(fx.GrabResult(h), id,
        TEvCheckIntegrityResult::PS_OK,
        TEvCheckIntegrityResult::DS_OK);
}

Y_UNIT_TEST(PlacementRecoverable5of6) {
    // 5 parts present, 1 missing, no errors -> recoverable.
    TFixture fx(TBlobStorageGroupType::Erasure4Plus2Block);
    auto id = fx.MakeBlobId(200003);
    fx.SetupAndRun(id, {OK1, OK2, OK3, OK4, OK5, ND, ND, ND});

    TAutoPtr<IEventHandle> h;
    AssertResultOk(fx.GrabResult(h), id,
        TEvCheckIntegrityResult::PS_BLOB_IS_RECOVERABLE,
        TEvCheckIntegrityResult::DS_OK);
}

Y_UNIT_TEST(PlacementRecoverable4of6) {
    // Exactly 4 distinct parts -> at the boundary of recoverability.
    TFixture fx(TBlobStorageGroupType::Erasure4Plus2Block);
    auto id = fx.MakeBlobId(200004);
    fx.SetupAndRun(id, {OK1, OK2, OK3, OK4, ND, ND, ND, ND});

    TAutoPtr<IEventHandle> h;
    AssertResultOk(fx.GrabResult(h), id,
        TEvCheckIntegrityResult::PS_BLOB_IS_RECOVERABLE,
        TEvCheckIntegrityResult::DS_OK);
}

Y_UNIT_TEST(PlacementRecoverableAllPartsAtTheSameSlot) {
    TFixture fx(TBlobStorageGroupType::Erasure4Plus2Block);
    auto& gm = fx.TestState->GetGroupMock();

    auto blobId = fx.MakeBlobId(200001);
    for (int i = 1; i <= 6; i++) {
        gm.PutPartAtSlot(blobId, 0, i, fx.TestState->BlobData);
    }
    fx.SendCheckIntegrity(blobId);
    fx.TestState->HandleVGetsWithMock(fx.SubgroupSize());

    TAutoPtr<IEventHandle> h;
    AssertResultOk(fx.GrabResult(h), blobId,
        TEvCheckIntegrityResult::PS_BLOB_IS_RECOVERABLE,
        TEvCheckIntegrityResult::DS_OK);
}

Y_UNIT_TEST(PlacementBlobIsLost) {
    // Part 4 is missing, part 3 is duplicated
    TFixture fx(TBlobStorageGroupType::Erasure4Plus2Block);
    auto id = fx.MakeBlobId(200005);
    fx.SetupAndRun(id, {OK1, OK2, OK3, OK3, ND, ND, ND, ND});

    TAutoPtr<IEventHandle> h;
    AssertResultOk(fx.GrabResult(h), id,
        TEvCheckIntegrityResult::PS_BLOB_IS_LOST,
        TEvCheckIntegrityResult::DS_OK);
}

Y_UNIT_TEST(PlacementBlobIsLostNoParts) {
    TFixture fx(TBlobStorageGroupType::Erasure4Plus2Block);
    auto id = fx.MakeBlobId(200006);
    fx.SetupAndRun(id, {ND, ND, ND, ND, ND, ND, ND, ND});

    TAutoPtr<IEventHandle> h;
    AssertResultOk(fx.GrabResult(h), id,
        TEvCheckIntegrityResult::PS_BLOB_IS_LOST,
        TEvCheckIntegrityResult::DS_OK);
}

Y_UNIT_TEST(PlacementUnknownUnrecoverableWithErrors) {
    // 3 OK parts (unrecoverable on its own) + 1 ERROR disk:
    // the missing parts may sit on the errored disk -> PS_UNKNOWN, not LOST.
    TFixture fx(TBlobStorageGroupType::Erasure4Plus2Block);
    auto id = fx.MakeBlobId(200007);
    fx.SetupAndRun(id, {OK1, OK2, OK3, ND, ND, ND, ND, ERR});

    TAutoPtr<IEventHandle> h;
    AssertResultOk(fx.GrabResult(h), id,
        TEvCheckIntegrityResult::PS_UNKNOWN,
        TEvCheckIntegrityResult::DS_UNKNOWN);
}

Y_UNIT_TEST(PlacementRecoverableWithErrors) {
    // 4 OK parts + ERROR disk: recoverable wins over PS_UNKNOWN because
    // PS_UNKNOWN is only returned for unrecoverable layouts.
    TFixture fx(TBlobStorageGroupType::Erasure4Plus2Block);
    auto id = fx.MakeBlobId(200008);
    fx.SetupAndRun(id, {OK1, OK2, OK3, OK4, ND, ND, ND, ERR});

    TAutoPtr<IEventHandle> h;
    AssertResultOk(fx.GrabResult(h), id,
        TEvCheckIntegrityResult::PS_BLOB_IS_RECOVERABLE,
        TEvCheckIntegrityResult::DS_OK);
}

Y_UNIT_TEST(ReplicationInProgress) {
    // 5 parts OK + 1 NOT_YET. PartLayout has 5 parts (recoverable);
    // PartLayoutWithNotYet has 6 (full) -> PS_REPLICATION_IN_PROGRESS.
    TFixture fx(TBlobStorageGroupType::Erasure4Plus2Block);
    auto id = fx.MakeBlobId(200009);
    fx.SetupAndRun(id, {OK1, OK2, OK3, OK4, OK5, NY6, ND, ND});

    TAutoPtr<IEventHandle> h;
    AssertResultOk(fx.GrabResult(h), id,
        TEvCheckIntegrityResult::PS_REPLICATION_IN_PROGRESS,
        TEvCheckIntegrityResult::DS_OK);
}

Y_UNIT_TEST(DataCorruptionDetected) {
    // 6 parts placed, one of them carries random bytes -> PS_OK, DS_ERROR.
    TFixture fx(TBlobStorageGroupType::Erasure4Plus2Block);
    auto id = fx.MakeBlobId(200010);
    fx.SetupAndRun(id, {OK1, OK2, OK3, OK4, OK5, RND6, ND, ND});

    TAutoPtr<IEventHandle> h;
    AssertResultOk(fx.GrabResult(h), id,
        TEvCheckIntegrityResult::PS_OK,
        TEvCheckIntegrityResult::DS_ERROR);
}

Y_UNIT_TEST(DataCorruptionTwoParts) {
    TFixture fx(TBlobStorageGroupType::Erasure4Plus2Block);
    auto id = fx.MakeBlobId(200011);
    fx.SetupAndRun(id, {OK1, OK2, OK3, OK4, RND5, RND6, ND, ND});

    TAutoPtr<IEventHandle> h;
    AssertResultOk(fx.GrabResult(h), id,
        TEvCheckIntegrityResult::PS_OK,
        TEvCheckIntegrityResult::DS_ERROR);
}

Y_UNIT_TEST(DataCorruptionInconsistentParts) {
    TFixture fx(TBlobStorageGroupType::Erasure4Plus2Block);
    auto& gm = fx.TestState->GetGroupMock();

    auto blobId = fx.MakeBlobId(200001);
    for (ui32 slot = 0; slot < fx.SubgroupSize(); slot++) {
        for (int part = 1; part <= 6; part++) {
            gm.PutPartAtSlot(blobId, slot, part, fx.TestState->BlobData);
            if (slot >= 3) {
                gm.CorruptPart(blobId, slot, part);
            }
        }
    }
    fx.SendCheckIntegrity(blobId);
    fx.TestState->HandleVGetsWithMock(fx.SubgroupSize());

    TAutoPtr<IEventHandle> h;
    AssertResultOk(fx.GrabResult(h), blobId,
        TEvCheckIntegrityResult::PS_OK,
        TEvCheckIntegrityResult::DS_ERROR);
}

} // Block42

////////////////////////////////////////////////////////////////////////////////
// Mirror 3 DC
//
// Subgroup is a 3x3 matrix: rows are fail domains, columns are realms (DCs).
// Slots 0,1,2 are the main holders (one per realm); 3..8 are handoff slots
// distributed across realms in the same column-wise pattern.
//
// Quorum:
//   EBS_FULL                      CheckQuorumForSubgroup: at least 1 disk per
//                                 realm (num1==3) or 2 disks in 2 realms.
//   EBS_RECOVERABLE_FRAGMENTARY   any disk has the part, but not full quorum.
//   EBS_UNRECOVERABLE_FRAGMENTARY no disk has the part.
//
// Mirror-3-dc has TotalParts==3, so partId in (1, 2, 3).  All "parts" are
// full copies of the data, and the placement check looks only at which
// slots host *any* replica - the partId does not influence the verdict.
////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDSProxyCheckIntegrityMirror3dc) {

Y_UNIT_TEST(PlacementOkOnePerRealm) {
    // One replica in each of the three realms -> num1 == 3 -> EBS_FULL.
    TFixture fx(TBlobStorageGroupType::ErasureMirror3dc);
    auto id = fx.MakeBlobId(300001);
    fx.SetupAndRun(id, {OK1, OK2, OK3,   ND, ND, ND,   ND, ND, ND});

    TAutoPtr<IEventHandle> h;
    AssertResultOk(fx.GrabResult(h), id,
        TEvCheckIntegrityResult::PS_OK,
        TEvCheckIntegrityResult::DS_OK);
}

Y_UNIT_TEST(PlacementRecoverableOnlyOneRealm) {
    // 3 replicas all in the same realm (column 0) -> only 1 realm has the
    // blob, num1==1 -> not full but disksWithReplica != 0 -> recoverable.
    TFixture fx(TBlobStorageGroupType::ErasureMirror3dc);
    auto id = fx.MakeBlobId(300002);
    fx.SetupAndRun(id, {OK1, ND, ND,   OK2, ND, ND,   OK3, ND, ND});

    TAutoPtr<IEventHandle> h;
    AssertResultOk(fx.GrabResult(h), id,
        TEvCheckIntegrityResult::PS_BLOB_IS_RECOVERABLE,
        TEvCheckIntegrityResult::DS_OK);
}

Y_UNIT_TEST(PlacementOk2Plus2DuringDcOutage) {
    // Simulates a write that completed during a DC2 outage: two replicas
    // landed in realm 0 (slots 0, 3) and two in realm 1 (slots 1, 4); all
    // disks of realm 2 are errored. num2 == 2 (two realms have >=2 disks
    // with the part) -> EBS_FULL -> PS_OK. Fail model is satisfied since
    // failures are confined to a single realm.
    TFixture fx(TBlobStorageGroupType::ErasureMirror3dc);
    auto id = fx.MakeBlobId(300011);
    fx.SetupAndRun(id, {OK1, OK1, ERR,   OK1, OK1, ERR,   ND, ND, ERR});

    TAutoPtr<IEventHandle> h;
    AssertResultOk(fx.GrabResult(h), id,
        TEvCheckIntegrityResult::PS_OK,
        TEvCheckIntegrityResult::DS_OK);
}

Y_UNIT_TEST(PlacementRecoverable2Plus0OneRealmAlive) {
    // Only realm 0 has replicas (slots 0 and 3); realm 1 has none; realm 2
    // is fully errored. disksWithReplica != 0 -> recoverable, but neither
    // num1 nor num2 reaches the EBS_FULL threshold.
    TFixture fx(TBlobStorageGroupType::ErasureMirror3dc);
    auto id = fx.MakeBlobId(300012);
    fx.SetupAndRun(id, {OK1, ND, ERR,   OK1, ND, ERR,   ND, ND, ERR});

    TAutoPtr<IEventHandle> h;
    AssertResultOk(fx.GrabResult(h), id,
        TEvCheckIntegrityResult::PS_BLOB_IS_RECOVERABLE,
        TEvCheckIntegrityResult::DS_OK);
}

Y_UNIT_TEST(PlacementRecoverablePhantomBlob) {
    // Single residual replica from a deleted blob -> still recoverable
    // (mirror-3-dc only flags as unrecoverable when *zero* disks have it).
    TFixture fx(TBlobStorageGroupType::ErasureMirror3dc);
    auto id = fx.MakeBlobId(300003);
    fx.SetupAndRun(id, {OK1, ND, ND,   ND, ND, ND,   ND, ND, ND});

    TAutoPtr<IEventHandle> h;
    AssertResultOk(fx.GrabResult(h), id,
        TEvCheckIntegrityResult::PS_BLOB_IS_RECOVERABLE,
        TEvCheckIntegrityResult::DS_OK);
}

Y_UNIT_TEST(PlacementBlobIsLost) {
    TFixture fx(TBlobStorageGroupType::ErasureMirror3dc);
    auto id = fx.MakeBlobId(300004);
    fx.SetupAndRun(id, {ND, ND, ND,   ND, ND, ND,   ND, ND, ND});

    TAutoPtr<IEventHandle> h;
    AssertResultOk(fx.GrabResult(h), id,
        TEvCheckIntegrityResult::PS_BLOB_IS_LOST,
        TEvCheckIntegrityResult::DS_OK);
}

Y_UNIT_TEST(PlacementUnknownNoBlobButErrors) {
    // No replicas, but some disks errored -> the blob may live on those
    // disks -> PS_UNKNOWN.
    TFixture fx(TBlobStorageGroupType::ErasureMirror3dc);
    auto id = fx.MakeBlobId(300005);
    fx.SetupAndRun(id, {ND, ND, ND,   ND, ND, ND,   ERR, ND, ND});

    TAutoPtr<IEventHandle> h;
    AssertResultOk(fx.GrabResult(h), id,
        TEvCheckIntegrityResult::PS_UNKNOWN,
        TEvCheckIntegrityResult::DS_UNKNOWN);
}

Y_UNIT_TEST(DataCorruption) {
    TFixture fx(TBlobStorageGroupType::ErasureMirror3dc);
    auto id = fx.MakeBlobId(300006);
    fx.SetupAndRun(id, {OK1, OK2, RND3,   ND, ND, ND,   ND, ND, ND});

    TAutoPtr<IEventHandle> h;
    AssertResultOk(fx.GrabResult(h), id,
        TEvCheckIntegrityResult::PS_OK,
        TEvCheckIntegrityResult::DS_ERROR);
}

Y_UNIT_TEST(DataCorruptionAndPlacementRecoverable) {
    TFixture fx(TBlobStorageGroupType::ErasureMirror3dc);
    auto id = fx.MakeBlobId(300007);
    fx.SetupAndRun(id, {OK1, RND2, ND,   ND, ND, ND,   ND, ND, ND});

    TAutoPtr<IEventHandle> h;
    AssertResultOk(fx.GrabResult(h), id,
        TEvCheckIntegrityResult::PS_BLOB_IS_RECOVERABLE,
        TEvCheckIntegrityResult::DS_ERROR);
}

Y_UNIT_TEST(ReplicationInProgress) {
    TFixture fx(TBlobStorageGroupType::ErasureMirror3dc);
    auto id = fx.MakeBlobId(300008);
    fx.SetupAndRun(id, {OK1, OK2, NY3,   ND, ND, ND,   ND, ND, ND});

    TAutoPtr<IEventHandle> h;
    AssertResultOk(fx.GrabResult(h), id,
        TEvCheckIntegrityResult::PS_REPLICATION_IN_PROGRESS,
        TEvCheckIntegrityResult::DS_OK);
}

} // Mirror3dc

} // namespace NDSProxyCheckIntegrityTest
} // namespace NKikimr
