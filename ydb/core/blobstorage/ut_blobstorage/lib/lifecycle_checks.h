#pragma once

#include "env.h"

#include <bit>

namespace NBlobStorageLifecycle {

inline void CheckGroupBlob(TEnvironmentSetup& env, const TIntrusivePtr<TBlobStorageGroupInfo>& info,
        const TLogoBlobID& id, const TString& data, bool mustRestoreFirst = false) {
    const TActorId edge = env.Runtime->AllocateEdgeActor(env.Settings.ControllerNodeId);
    const TInstant deadline = env.Now() + TDuration::Seconds(30);
    env.Runtime->WrapInActorContext(edge, [&] {
        SendToBSProxy(edge, info->GroupID, new TEvBlobStorage::TEvGet(id.FullID(), 0, 0, deadline,
            NKikimrBlobStorage::FastRead, mustRestoreFirst));
    });
    auto result = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge, true, deadline);
    UNIT_ASSERT(result);
    UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::OK);
    UNIT_ASSERT_VALUES_EQUAL(result->Get()->ResponseSz, 1);
    UNIT_ASSERT_VALUES_EQUAL(result->Get()->Responses[0].Status, NKikimrProto::OK);
    UNIT_ASSERT_VALUES_EQUAL(result->Get()->Responses[0].Buffer.ConvertToString(), data);
}

inline TVDiskID SubgroupDisk(const TIntrusivePtr<TBlobStorageGroupInfo>& info,
        const TLogoBlobID& id, ui32 subgroupIdx) {
    return info->CreateVDiskID(info->GetTopology().GetVDiskInSubgroup(subgroupIdx, id.FullID().Hash()));
}

inline void CheckPart(TEnvironmentSetup& env, const TIntrusivePtr<TBlobStorageGroupInfo>& info,
        ui32 subgroupIdx, const TLogoBlobID& id, ui32 partIdx, const TRope& expected) {
    const TVDiskID vdiskId = SubgroupDisk(info, id, subgroupIdx);
    env.WithQueueId(vdiskId, NKikimrBlobStorage::GetFastRead, [&](TActorId queueId) {
        const TActorId edge = env.Runtime->AllocateEdgeActor(queueId.NodeId());
        const TInstant deadline = env.Now() + TDuration::Seconds(30);
        auto query = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(vdiskId, deadline,
            NKikimrBlobStorage::FastRead, {}, {}, {{TLogoBlobID(id, partIdx + 1)}});
        env.Runtime->Send(new IEventHandle(queueId, edge, query.release()), queueId.NodeId());
        auto result = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVGetResult>(edge, true, deadline);
        UNIT_ASSERT(result);
        const auto& record = result->Get()->Record;
        UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(record.ResultSize(), 1);
        const auto& item = record.GetResult(0);
        UNIT_ASSERT_VALUES_EQUAL(item.GetStatus(), NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(LogoBlobIDFromLogoBlobID(item.GetBlobID()), TLogoBlobID(id, partIdx + 1));
        UNIT_ASSERT_EQUAL(result->Get()->GetBlobData(item), expected);
    });
}

// For a known local part set, persisted inline records concatenate those parts;
// Huge records store one part each. Capture normalizes their IDs to PartId=0;
// CheckPart verifies the identities separately. Fresh inline data has no extent yet.
inline void CheckHeaderlessRecords(TEnvironmentSetup& env, const TIntrusivePtr<TBlobStorageGroupInfo>& info,
        ui32 subgroupIdx, const TLogoBlobID& id, ui32 localPartsMask, bool requirePersisted = true) {
    UNIT_ASSERT(info->Type.TotalPartCount() > 8);
    const auto vdiskId = SubgroupDisk(info, id, subgroupIdx);
    auto layout = env.SyncQuery<TEvBlobStorage::TEvCaptureVDiskLayoutResult,
        TEvBlobStorage::TEvCaptureVDiskLayout>(info->GetActorId(vdiskId));
    using T = TEvBlobStorage::TEvCaptureVDiskLayoutResult;
    ui32 observedParts = 0;
    ui32 hugeRecords = 0;
    for (const auto& item : layout->Layout) {
        if (item.Database != T::EDatabase::LogoBlobs || item.RecordType == T::ERecordType::IndexRecord ||
                item.BlobId.FullID() != id.FullID()) {
            continue;
        }
        ui32 expectedSize = 0;
        if (item.RecordType == T::ERecordType::HugeBlob) {
            // All Block82 parts have equal physical size, including the CRC
            // trailer when enabled. The capture API omits their individual IDs.
            expectedSize = info->Type.PartSize(TLogoBlobID(id, 1));
            ++hugeRecords;
        } else {
            for (ui32 partIdx = 0; partIdx < info->Type.TotalPartCount(); ++partIdx) {
                if (localPartsMask & (1u << partIdx)) {
                    expectedSize += info->Type.PartSize(TLogoBlobID(id, partIdx + 1));
                }
            }
            observedParts |= localPartsMask;
        }
        UNIT_ASSERT_VALUES_EQUAL_C(item.Location.Size, expectedSize, item.ToString());
    }
    if (requirePersisted) {
        UNIT_ASSERT(observedParts == localPartsMask || hugeRecords >= static_cast<ui32>(std::popcount(localPartsMask)));
    }
}

inline void CheckMainParts(TEnvironmentSetup& env, const TIntrusivePtr<TBlobStorageGroupInfo>& info,
        const TLogoBlobID& id, const TString& data, bool checkPersisted = false) {
    TDataPartSet parts;
    info->Type.SplitData(static_cast<TErasureType::ECrcMode>(id.CrcMode()), data, parts);
    for (ui32 partIdx = 0; partIdx < info->Type.TotalPartCount(); ++partIdx) {
        CheckPart(env, info, partIdx, id, partIdx, parts.Parts[partIdx].OwnedString);
        if (checkPersisted && info->Type.TotalPartCount() > 8) {
            CheckHeaderlessRecords(env, info, partIdx, id, 1u << partIdx);
        }
    }
}

}
