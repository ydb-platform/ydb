#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/dsproxy/dsproxy_get_impl.h>
#include <ydb/core/blobstorage/dsproxy/ut/dsproxy_vdisk_mock_ut.h>
#include <ydb/core/blobstorage/dsproxy/group_sessions.h>

#include <util/stream/str.h>
#include <util/string/cast.h>

namespace {

void CheckLongRequestLog(ui64 thresholdMs, bool expectWarning) {
    TStringStream log;
    TEnvironmentSetup env({
        .NodeCount = 1,
        .Erasure = TBlobStorageGroupType::ErasureNone,
        .SlowDiskThreshold = 4,
        .VDiskPredictedDelayMultiplier = 3,
        .MaxNumOfSlowDisks = 2,
    });
    env.Runtime->LogStream = &log;
    env.Runtime->SetLogPriority(NKikimrServices::BS_PROXY_GET, NLog::PRI_WARN);
    env.CreateBoxAndPool(1, 1);
    env.Sim(TDuration::Minutes(1));

    const auto groups = env.GetGroups();
    UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
    const ui32 groupId = groups.front();
    const TActorId edge = env.Runtime->AllocateEdgeActor(1);
    const TString data = "get request prediction history";
    const TLogoBlobID blobId(1, 1, 1, 0, data.size(), 0);
    env.Runtime->WrapInActorContext(edge, [&] {
        SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvPut(blobId, data, TInstant::Max()));
    });
    auto put = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false);
    UNIT_ASSERT_VALUES_EQUAL(put->Get()->Status, NKikimrProto::OK);

    TAtomic previousValue;
    env.Runtime->GetNode(1)->AppData->Icb->SetValue("DSProxyControls.LongRequestThresholdMs", thresholdMs, previousValue);
    // Refresh the proxy's memorizable control wrapper before issuing the Get.
    env.Sim(TDuration::Seconds(16));
    env.Runtime->WrapInActorContext(edge, [&] {
        SendToBSProxy(edge, groupId, new TEvGetQueuesInfo(NKikimrBlobStorage::GetFastRead));
    });
    auto queues = env.WaitForEdgeActorEvent<TEvQueuesInfo>(edge, false);
    UNIT_ASSERT_VALUES_EQUAL(queues->Get()->Queues.size(), 1);
    UNIT_ASSERT(queues->Get()->Queues[0]);
    auto flowRecord = queues->Get()->Queues[0]->FlowRecord;
    UNIT_ASSERT(flowRecord);
    flowRecord->SetPredictedDelayNs(20'000'000);
    log.Clear();

    std::vector<std::unique_ptr<IEventHandle>> delayed;
    env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
        if (ev->GetTypeRewrite() == TEvBlobStorage::TEvVGetResult::EventType) {
            delayed.push_back(std::move(ev));
            return false;
        }
        return true;
    };
    env.Runtime->WrapInActorContext(edge, [&] {
        SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvGet(
            blobId, 0, data.size(), TInstant::Max(), NKikimrBlobStorage::FastRead));
    });
    env.Sim(TDuration::MilliSeconds(200));
    env.Runtime->FilterFunction = {};
    UNIT_ASSERT(!delayed.empty());
    for (auto& ev : delayed) {
        const ui32 nodeId = ev->GetRecipientRewrite().NodeId();
        env.Runtime->Send(ev.release(), nodeId);
    }

    auto get = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge, false);
    UNIT_ASSERT_VALUES_EQUAL(get->Get()->Status, NKikimrProto::OK);
    UNIT_ASSERT_VALUES_EQUAL(get->Get()->ResponseSz, 1);
    UNIT_ASSERT_VALUES_EQUAL(get->Get()->Responses[0].Status, NKikimrProto::OK);
    UNIT_ASSERT_VALUES_EQUAL(get->Get()->Responses[0].Buffer.ConvertToString(), data);
    const TString text = log.Str();
    UNIT_ASSERT_VALUES_EQUAL_C(text.Contains("BPG71"), expectWarning, text);
    if (!expectWarning) {
        return;
    }
    UNIT_ASSERT_C(text.Contains("MaxNumOfSlowDisks# 2"), text);
    UNIT_ASSERT_C(text.Contains("SlowDiskThreshold# 4"), text);
    UNIT_ASSERT_C(text.Contains("PredictedDelayMultiplier# 3"), text);
    const size_t initial = text.find("Stage# Initial");
    const size_t schedule = text.find("GetAccelerationSchedule{");
    const size_t final = text.find("Stage# Final");
    UNIT_ASSERT_C(initial != TString::npos && schedule != TString::npos && final != TString::npos, text);
    UNIT_ASSERT_C(initial < text.find("TEvVGet{") && initial < schedule && schedule < final, text);
    const TStringBuf initialSnapshot(text.data() + initial, schedule - initial);
    UNIT_ASSERT_C(initialSnapshot.Contains("Predictions# [20000000]"), text);
    const size_t scheduleEnd = text.find('}', schedule);
    const TStringBuf firstSchedule(text.data() + schedule, scheduleEnd - schedule);
    UNIT_ASSERT_C(firstSchedule.Contains("Attempt# 1"), text);
    const auto predictionText = firstSchedule.SubStr(
        firstSchedule.find("PredictedDelayNs# ") + TStringBuf("PredictedDelayNs# ").size());
    const ui64 predictionNs = FromString<ui64>(predictionText.Before(' '));
    // ICB converts the multiplier through float, which can add a few nanoseconds.
    UNIT_ASSERT_C(predictionNs >= 59'999'900 && predictionNs <= 60'000'100, text);
    UNIT_ASSERT_C(firstSchedule.Contains("DelayMs# "), text);
    const auto delayText = firstSchedule.SubStr(firstSchedule.find("DelayMs# ") + TStringBuf("DelayMs# ").size());
    const double delayMs = FromString<double>(delayText.Before(' '));
    UNIT_ASSERT_C(delayMs > 0 && delayMs <= 60, text);
}

} // namespace

Y_UNIT_TEST_SUITE(GetRequestHistory) {
    Y_UNIT_TEST(LongGetLogsPredictionsAndScheduling) {
        CheckLongRequestLog(100, true);
    }

    Y_UNIT_TEST(FastGetDoesNotEmitLongRequestLog) {
        CheckLongRequestLog(1000, false);
    }

    Y_UNIT_TEST(SnapshotsKeepValuesForEveryVDisk) {
        TEnvironmentSetup env({
            .NodeCount = 1,
            .Erasure = TBlobStorageGroupType::ErasureNone,
        });
        const TActorId edge = env.Runtime->AllocateEdgeActor(1);
        env.Runtime->WrapInActorContext(edge, [&] {
            NKikimr::TGroupMock group(0, TErasureType::Erasure4Plus2Block, 8, 1, 1);
            for (ui32 disk = 0; disk < 8; ++disk) {
                group.SetPredictedDelayNs(disk, (disk + 1) * 1'000'000);
            }
            const auto info = group.GetInfo();
            const auto queues = group.MakeGroupQueues();
            const TLogoBlobID blob(1, 1, 1, 0, 4096, 0);
            TEvBlobStorage::TEvGet request(blob, 0, blob.BlobSize(), TInstant::Max(), NKikimrBlobStorage::FastRead);
            NKikimr::TGetImpl impl(info, queues, &request, nullptr, NKikimr::TAccelerationParams{});
            NKikimr::TLogContext logCtx(NKikimrServices::BS_PROXY_GET, false);
            logCtx.LogAcc.IsLogEnabled = false;
            TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> gets;
            impl.GenerateInitialRequests(logCtx, gets);
            UNIT_ASSERT(!gets.empty());

            for (ui32 disk = 0; disk < 8; ++disk) {
                group.SetPredictedDelayNs(disk, (disk + 11) * 1'000'000);
            }
            impl.RecordPredictions(false, true);
            const TString text = impl.PrintHistory();
            const size_t initial = text.find("Stage# Initial");
            const size_t final = text.find("Stage# Final");
            UNIT_ASSERT_C(initial != TString::npos && final != TString::npos && initial < final, text);
            const TStringBuf initialSnapshot(text.data() + initial, final - initial);
            const TStringBuf finalSnapshot(text.data() + final, text.size() - final);
            UNIT_ASSERT_C(initialSnapshot.Contains("Predictions# [1000000, 2000000, 3000000, 4000000, "
                "5000000, 6000000, 7000000, 8000000]"), text);
            UNIT_ASSERT_C(finalSnapshot.Contains("Predictions# [11000000, 12000000, 13000000, 14000000, "
                "15000000, 16000000, 17000000, 18000000]"), text);
        });
    }
}
