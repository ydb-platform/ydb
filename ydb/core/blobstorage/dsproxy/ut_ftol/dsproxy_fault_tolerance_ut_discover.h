#pragma once

#include "defs.h"
#include "dsproxy_fault_tolerance_ut_base.h"

namespace NKikimr {

class TDiscoverFaultToleranceTest : public TFaultToleranceTestBase<TDiscoverFaultToleranceTest> {
public:
    using TFaultToleranceTestBase::TFaultToleranceTestBase;

    void RunTestAction() {
        const ui64 tabletId = 100500;

        // put some data
        TLogoBlobID lastBlobId;
        const ui32 numGenerations = 10;
        const ui32 numStepsPerGeneration = 3;
        for (ui32 i = 0; i < numGenerations; ++i) {
            for (ui32 j = 0; j < numStepsPerGeneration; ++j) {
                TString data = Sprintf("%" PRIu32 "/%" PRIu32, i + 1, j + 1);
                TLogoBlobID id(tabletId, i + 1, j + 1, 0, data.size(), 0);
                Put(id, data);
                lastBlobId = id;
            }
        }

        // find the location of last blob
        THashSet<TVDiskID> vdisksWithLastBlob;
        for (const auto& pair : Runtime.VDisks) {
            auto ev = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(pair.first, TInstant::Max(), NKikimrBlobStorage::FastRead,
                    TEvBlobStorage::TEvVGet::EFlags::None, {}, {lastBlobId});
            GetActorContext().Send(pair.second, ev.release());
            auto resp = WaitForSpecificEvent<TEvBlobStorage::TEvVGetResult>(&TDiscoverFaultToleranceTest::ProcessUnexpectedEvent);
            const auto& record = resp->Get()->Record;
            UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrProto::OK);
            UNIT_ASSERT(record.ResultSize() >= 1);
            const auto& item = record.GetResult(0);
            if (item.GetStatus() == NKikimrProto::OK) {
                vdisksWithLastBlob.emplace(pair.first);
            }
        }

        // put blocks
        SendToBSProxy(GetActorContext(), Info->GroupID, new TEvBlobStorage::TEvBlock(1, numGenerations - 1, TInstant::Max()));
        auto response = WaitForSpecificEvent<TEvBlobStorage::TEvBlockResult>(&TDiscoverFaultToleranceTest::ProcessUnexpectedEvent);
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->Status, NKikimrProto::OK);

        TBlobStorageGroupInfo::TVDiskIds lastBlobSubgroup;
        Info->PickSubgroup(lastBlobId.Hash(), &lastBlobSubgroup, nullptr);

        for (const auto& failedDisks : FaultsExceedingFailModel) {
            ui32 num = 0;
            for (const TVDiskID& vdisk : vdisksWithLastBlob) {
                if (~failedDisks & TBlobStorageGroupInfo::TGroupVDisks(&Info->GetTopology(), vdisk)) {
                    ++num;
                }
            }
            const bool recoverable = num >= Info->Type.MinimalRestorablePartCount();

            TBlobStorageGroupInfo::TSubgroupVDisks failedSubgroupDisks(&Info->GetTopology());
            for (const TVDiskID& vdisk : lastBlobSubgroup) {
                if (failedDisks & TBlobStorageGroupInfo::TGroupVDisks(&Info->GetTopology(), vdisk)) {
                    ui32 nodeId = Info->GetIdxInSubgroup(vdisk, lastBlobId.Hash());
                    failedSubgroupDisks += TBlobStorageGroupInfo::TSubgroupVDisks(&Info->GetTopology(), nodeId);
                }
            }
            const bool restorable = Info->GetQuorumChecker().CheckFailModelForSubgroup(failedSubgroupDisks);

            SetFailedDisks(failedDisks);
            SendToBSProxy(GetActorContext(), Info->GroupID, new TEvBlobStorage::TEvDiscover(tabletId, 0, false, false,
                TInstant::Max(), 0, true));
            auto resp = WaitForSpecificEvent<TEvBlobStorage::TEvDiscoverResult>(&TDiscoverFaultToleranceTest::ProcessUnexpectedEvent);

            const NKikimrProto::EReplyStatus status = resp->Get()->Status;

            CTEST << "recoverable# " << (recoverable ? "true" : "false")
                << " restorable# " << (restorable ? "true" : "false")
                << " status# " << NKikimrProto::EReplyStatus_Name(status)
                << " vdisksWithLastBlob# [";
            for (auto it = vdisksWithLastBlob.begin(); it != vdisksWithLastBlob.end(); ++it) {
                CTEST << (it != vdisksWithLastBlob.begin() ? " " : "")
                    << it->ToString();
            }
            CTEST << "] failedDisks# [";
            bool first = true;
            for (const auto& vdisk : Info->GetVDisks()) {
                auto vd = Info->GetVDiskId(vdisk.OrderNumber);
                if (failedDisks & TBlobStorageGroupInfo::TGroupVDisks(&Info->GetTopology(), vd)) {
                    CTEST << (first ? "" : " ") << vd;
                    first = false;
                }
            }
            CTEST << "] num# " << num << Endl;

            NKikimrProto::EReplyStatus expected = recoverable && restorable
                ? NKikimrProto::OK
                : NKikimrProto::ERROR;

            UNIT_ASSERT_C(status == expected || status == NKikimrProto::ERROR,
                "status# " << NKikimrProto::EReplyStatus_Name(status)
                << " expected# " << NKikimrProto::EReplyStatus_Name(expected));
        }
        SetFailedDisks(TBlobStorageGroupInfo::TGroupVDisks(&Info->GetTopology()));

        // permute any combinations of faulty disks and check for blob recovery
        for (const auto& disks : FaultsFittingFailModel) {
            Delete(TLogoBlobID(0, 0, 0, 0, 0, 0), TLogoBlobID(Max<ui64>(), Max<ui32>(), Max<ui32>(),
                    TLogoBlobID::MaxChannel, TLogoBlobID::MaxBlobSize, TLogoBlobID::MaxCookie),
                    disks, false);

            SendToBSProxy(GetActorContext(), Info->GroupID, new TEvBlobStorage::TEvDiscover(tabletId, 0, true, true, TInstant::Max(), 0, true));
            auto response = WaitForSpecificEvent<TEvBlobStorage::TEvDiscoverResult>(&TDiscoverFaultToleranceTest::ProcessUnexpectedEvent);

            UNIT_ASSERT_VALUES_EQUAL(response->Get()->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(response->Get()->Id.TabletID(), tabletId);
            UNIT_ASSERT_VALUES_EQUAL(response->Get()->Id.Generation(), numGenerations);
            UNIT_ASSERT_VALUES_EQUAL(response->Get()->Id.Step(), numStepsPerGeneration);
            UNIT_ASSERT_VALUES_EQUAL(response->Get()->Id.Channel(), 0);
//            UNIT_ASSERT_VALUES_EQUAL(response->Get()->BlockedGeneration, numGenerations - 1);
            UNIT_ASSERT_VALUES_EQUAL(response->Get()->Buffer, Sprintf("%" PRIu32 "/%" PRIu32, numGenerations,
                        numStepsPerGeneration));
        }
    }
};

} // NKikimr
