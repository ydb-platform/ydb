#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/vdisk/huge/blobstorage_hullhuge.h>

namespace {

void AssimilationWaitsForLog(bool huge) {
    TEnvironmentSetup env{{.NodeCount = 1, .Erasure = TBlobStorageGroupType::ErasureNone}};
    env.CreateBoxAndPool(1, 1);
    env.Sim(TDuration::Seconds(30));
    auto& runtime = env.Runtime;
    const auto info = env.GetGroupInfo(env.GetGroups().front());
    const auto vdiskId = info->GetVDiskId(0);
    const auto queue = env.CreateQueueActor(vdiskId, NKikimrBlobStorage::PutTabletLog, 0, 1);
    const auto writer = runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
    const auto reader = runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
    const TString data(huge ? 1024 * 1024 : 100, 'x');
    const TLogoBlobID id(100500, 1, 1, 0, data.size(), 0, 1);
    std::vector<std::unique_ptr<IEventHandle>> delayed;
    bool hold = true;
    bool hugeDataWritten = false;
    std::unique_ptr<IEventHandle> putResult;
    std::unique_ptr<IEventHandle> assimilationResult;
    runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
        if (ev->GetRecipientRewrite() == writer) {
            putResult = std::move(ev);
            return false;
        }
        if (ev->GetRecipientRewrite() == reader) {
            assimilationResult = std::move(ev);
            return false;
        }
        if (ev->GetTypeRewrite() == TEvHullLogHugeBlob::EventType &&
                ev->Get<TEvHullLogHugeBlob>()->LogoBlobID == id) {
            hugeDataWritten = true;
        }
        if (hold && (!huge || hugeDataWritten) && ev->GetTypeRewrite() == NPDisk::TEvLogResult::EventType) {
            delayed.push_back(std::move(ev));
            return false;
        }
        return true;
    };
    runtime->Send(new IEventHandle(queue, writer, new TEvBlobStorage::TEvVPut(id, TRope(data), vdiskId,
        false, nullptr, TInstant::Max(), NKikimrBlobStorage::TabletLog, false)), writer.NodeId());
    env.Sim(TDuration::Seconds(1));
    UNIT_ASSERT(!huge || hugeDataWritten);
    UNIT_ASSERT(!delayed.empty());
    runtime->Send(new IEventHandle(info->GetActorId(0), reader, new TEvBlobStorage::TEvVAssimilate(
        vdiskId, std::nullopt, std::nullopt, std::nullopt, true, true)), reader.NodeId());
    env.Sim(TDuration::Seconds(1));
    hold = false;
    for (auto& ev : delayed) {
        runtime->Send(ev.release(), 1);
    }
    env.Sim(TDuration::Seconds(1));
    UNIT_ASSERT(putResult);
    UNIT_ASSERT(assimilationResult);
    UNIT_ASSERT_VALUES_EQUAL(putResult->Get<TEvBlobStorage::TEvVPutResult>()->Record.GetStatus(), NKikimrProto::OK);
    const auto& record = assimilationResult->Get<TEvBlobStorage::TEvVAssimilateResult>()->Record;
    UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrProto::OK);
    UNIT_ASSERT_VALUES_EQUAL(record.BlobsSize(), 1);
    const auto& blob = record.GetBlobs(0);
    const ui64 raw[] = {blob.GetRawX1(), blob.GetRawX2(), blob.GetRawX3()};
    UNIT_ASSERT_VALUES_EQUAL(TLogoBlobID(raw), id.FullID());
    runtime->FilterFunction = {};
}

void RunHugeDataWriteAcrossGeneration(bool changeGeneration) {
    TEnvironmentSetup env{{.NodeCount = 1, .Erasure = TBlobStorageGroupType::ErasureNone}};
    env.CreateBoxAndPool(1, 1);
    env.Sim(TDuration::Seconds(30));
    auto& runtime = env.Runtime;
    const auto info = env.GetGroupInfo(env.GetGroups().front());
    auto vdiskId = info->GetVDiskId(0);
    const auto queue = env.CreateQueueActor(vdiskId, NKikimrBlobStorage::PutTabletLog, 0, 1);
    const auto writer = runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
    const auto reader = runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
    const TString data(1024 * 1024, 'x');
    const TLogoBlobID id(100500, 1, 1, 0, data.size(), 0, 1);
    std::unique_ptr<IEventHandle> delayed;
    TActorId skeleton;
    std::optional<NKikimrProto::EReplyStatus> status;
    bool hold = true;
    runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
        if (hold && ev->GetTypeRewrite() == TEvHullLogHugeBlob::EventType &&
                ev->Get<TEvHullLogHugeBlob>()->LogoBlobID == id) {
            UNIT_ASSERT(!delayed);
            skeleton = ev->GetRecipientRewrite();
            delayed = std::move(ev);
            return false;
        }
        if (ev->GetTypeRewrite() == TEvBlobStorage::TEvVPutResult::EventType && ev->Sender == skeleton) {
            status = ev->Get<TEvBlobStorage::TEvVPutResult>()->Record.GetStatus();
        }
        return ev->GetRecipientRewrite() != writer;
    };
    runtime->Send(new IEventHandle(queue, writer, new TEvBlobStorage::TEvVPut(id, TRope(data), vdiskId,
        false, nullptr, TInstant::Max(), NKikimrBlobStorage::TabletLog, false)), writer.NodeId());
    env.Sim(TDuration::Seconds(1));
    UNIT_ASSERT(delayed);
    if (changeGeneration) {
        TBlobStorageGroupInfo::TTopology topology(info->Type);
        topology.FailRealms = info->GetTopology().FailRealms;
        TBlobStorageGroupInfo::TDynamicInfo dynamic(info->GroupID, info->GroupGeneration + 1);
        dynamic.ServiceIdForOrderNumber = info->GetDynamicInfo().ServiceIdForOrderNumber;
        const auto updated = MakeIntrusive<TBlobStorageGroupInfo>(std::move(topology), std::move(dynamic),
            TString(), Nothing(), info->GetDeviceType());
        vdiskId = updated->GetVDiskId(0);
        runtime->Send(new IEventHandle(info->GetActorId(0), {}, new TEvVGenerationChange(vdiskId, updated)), 1);
        env.Sim(TDuration::MilliSeconds(100));
    }
    // This is the snapshot used by the new-generation bridge syncer, while the old write has no LSN yet.
    runtime->Send(new IEventHandle(info->GetActorId(0), reader, new TEvBlobStorage::TEvVAssimilate(
        vdiskId, std::nullopt, std::nullopt, std::nullopt, true, true)), reader.NodeId());
    const auto snapshot = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVAssimilateResult>(reader);
    UNIT_ASSERT_VALUES_EQUAL(snapshot->Get()->Record.GetStatus(), NKikimrProto::OK);
    UNIT_ASSERT_VALUES_EQUAL(snapshot->Get()->Record.BlobsSize(), 0);
    hold = false;
    status.reset();
    runtime->Send(delayed.release(), 1);
    env.Sim(TDuration::Seconds(1));
    UNIT_ASSERT(status);
    UNIT_ASSERT_VALUES_EQUAL(*status, changeGeneration ? NKikimrProto::RACE : NKikimrProto::OK);
    runtime->FilterFunction = {};
}

} // namespace

Y_UNIT_TEST_SUITE(BridgeAssimilationFence) {
    Y_UNIT_TEST(HugeDataWriteAcrossGeneration) {
        RunHugeDataWriteAcrossGeneration(true);
    }
    Y_UNIT_TEST(HugeDataWriteSameGeneration) {
        RunHugeDataWriteAcrossGeneration(false);
    }
    Y_UNIT_TEST(SmallWriteInFlight) {
        AssimilationWaitsForLog(false);
    }
    Y_UNIT_TEST(HugeWriteLogInFlight) {
        AssimilationWaitsForLog(true);
    }
}
