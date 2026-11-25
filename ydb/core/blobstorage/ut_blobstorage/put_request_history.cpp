#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/dsproxy/dsproxy_put_impl.h>
#include <ydb/core/blobstorage/dsproxy/dsproxy_request_reporting.h>
#include <ydb/core/blobstorage/dsproxy/ut/dsproxy_vdisk_mock_ut.h>
#include <ydb/core/blobstorage/dsproxy/group_sessions.h>

#include <util/stream/str.h>
#include <util/string/cast.h>

namespace {

// The runtime's built-in logger prints only TEvLog::Line. Use the production
// structured text writer so assertions also cover the warning's fields.
class TLogCapture : public TActor<TLogCapture> {
    TStringStream& Log;

    void StateFunc(TAutoPtr<IEventHandle>& ev) {
        auto* msg = ev->Get<NLog::TEvLog>();
        TStringBuilder text;
        text << msg->Line;
        if (msg->StructuredMessage) {
            text << " ";
            NStructuredLog::TTextWriter().Write(text, *msg->StructuredMessage);
        }
        Log << text << Endl;
    }

public:
    explicit TLogCapture(TStringStream& log)
        : TActor(&TThis::StateFunc)
        , Log(log)
    {}
};

double ReadNumber(TStringBuf text, TString name) {
    size_t pos = text.find(name);
    if (pos == TStringBuf::npos && name[0] >= 'a' && name[0] <= 'z') {
        name[0] = name[0] - ('a' - 'A');
        pos = text.find(name);
    }
    UNIT_ASSERT_C(pos != TStringBuf::npos, text);
    pos = text.find_first_of("0123456789", pos + name.size());
    UNIT_ASSERT_C(pos != TStringBuf::npos, text);
    const auto end = text.find_first_not_of("0123456789.eE+-", pos);
    return FromString<double>(text.SubStr(pos, end - pos));
}

TVDiskID ResultVDisk(IEventHandle& ev) {
    return ev.GetTypeRewrite() == TEvBlobStorage::TEvVPutResult::EventType
        ? VDiskIDFromVDiskID(ev.Get<TEvBlobStorage::TEvVPutResult>()->Record.GetVDiskID())
        : VDiskIDFromVDiskID(ev.Get<TEvBlobStorage::TEvVMultiPutResult>()->Record.GetVDiskID());
}

void CheckPutLog(ui64 thresholdMs, bool expectWarning) {
    TStringStream log;
    TEnvironmentSetup env({
        .NodeCount = 8,
        .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        .SlowDiskThreshold = 4,
        .VDiskPredictedDelayMultiplier = 3,
        .MaxNumOfSlowDisks = 2,
    });
    env.Runtime->LogStream = &log;
    env.Runtime->LoggerSettings()->LoggerActorId = env.Runtime->Register(new TLogCapture(log), 1);
    env.Runtime->SetLogPriority(NKikimrServices::BS_PROXY_PUT, NLog::PRI_WARN);
    env.CreateBoxAndPool(1, 1);
    env.Sim(TDuration::Minutes(1));
    const auto groups = env.GetGroups();
    UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
    const ui32 groupId = groups.front();
    const TActorId edge = env.Runtime->AllocateEdgeActor(1);
    env.Runtime->WrapInActorContext(edge, [&] {
        SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvStatus(TInstant::Max()));
    });
    env.WaitForEdgeActorEvent<TEvBlobStorage::TEvStatusResult>(edge, false);

    env.SetIcbControl(1, "DSProxyControls.LongRequestThresholdMs", thresholdMs);
    env.Sim(TDuration::Seconds(16));
    // Allow both intermediate and final warnings from the same long request.
    env.Runtime->Register(CreateRequestReportingThrottler(100, 60'000, 100), 1);
    env.Sim(TDuration::MilliSeconds(1));
    env.Runtime->WrapInActorContext(edge, [&] {
        SendToBSProxy(edge, groupId, new TEvGetQueuesInfo(NKikimrBlobStorage::PutAsyncBlob));
    });
    auto queues = env.WaitForEdgeActorEvent<TEvQueuesInfo>(edge, false);
    UNIT_ASSERT_VALUES_EQUAL(queues->Get()->Queues.size(), 8);
    auto resetPredictions = [&] {
        for (const auto& queue : queues->Get()->Queues) {
            UNIT_ASSERT(queue && queue->FlowRecord);
            queue->FlowRecord->SetPredictedDelayNs(20'000'000);
        }
    };
    resetPredictions();
    log.Clear();

    std::set<TVDiskID> fastDisks;
    std::vector<std::unique_ptr<IEventHandle>> delayed;
    env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
        if (ev->GetTypeRewrite() == TEvBlobStorage::TEvVPutResult::EventType
                || ev->GetTypeRewrite() == TEvBlobStorage::TEvVMultiPutResult::EventType) {
            const auto vdisk = ResultVDisk(*ev);
            if (vdisk.GroupID.GetRawId() == groupId) {
                resetPredictions();
                if (!fastDisks.contains(vdisk)) {
                    delayed.push_back(std::move(ev));
                    return false;
                }
            }
        }
        return true;
    };
    const TString data(4096, 'x');
    const TLogoBlobID blob(1, 1, 1, 0, data.size(), 0);
    env.Runtime->WrapInActorContext(edge, [&] {
        SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvPut(blob, data, TInstant::Max(),
            NKikimrBlobStorage::AsyncBlob, TEvBlobStorage::TEvPut::TacticMaxThroughput));
    });
    env.Sim(TDuration::MilliSeconds(1));
    UNIT_ASSERT_VALUES_EQUAL(delayed.size(), 6);
    auto initialReplies = std::move(delayed);
    delayed.clear();
    for (size_t i = 0; i < 4; ++i) {
        fastDisks.insert(ResultVDisk(*initialReplies[i]));
    }
    for (size_t i = 0; i < initialReplies.size(); ++i) {
        if (i < 4) {
            const auto nodeId = initialReplies[i]->GetRecipientRewrite().NodeId();
            env.Runtime->Send(initialReplies[i].release(), nodeId);
        } else {
            delayed.push_back(std::move(initialReplies[i]));
        }
    }
    env.Sim(TDuration::MilliSeconds(200));
    env.Runtime->FilterFunction = {};
    UNIT_ASSERT(!delayed.empty());
    for (auto& ev : delayed) {
        const auto nodeId = ev->GetRecipientRewrite().NodeId();
        env.Runtime->Send(ev.release(), nodeId);
    }
    auto put = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false);
    UNIT_ASSERT_VALUES_EQUAL(put->Get()->Status, NKikimrProto::OK);
    env.Sim(TDuration::MilliSeconds(1)); // Drain the final warning after the client reply.
    const TString text = log.Str();
    UNIT_ASSERT_VALUES_EQUAL_C(text.Contains("BPP71"), expectWarning, text);
    if (!expectWarning) {
        return;
    }
    UNIT_ASSERT_DOUBLES_EQUAL(ReadNumber(text, "maxNumOfSlowDisks"), 2, 0.001);
    UNIT_ASSERT_DOUBLES_EQUAL(ReadNumber(text, "slowDiskThreshold"), 4, 0.001);
    UNIT_ASSERT_DOUBLES_EQUAL(ReadNumber(text, "predictedDelayMultiplier"), 3, 0.001);
    const auto initial = text.find("Stage# Initial");
    const auto schedule = text.find("PutAccelerationSchedule{");
    const auto final = text.find("Stage# Final");
    UNIT_ASSERT_C(initial != TString::npos && schedule != TString::npos && final != TString::npos, text);
    UNIT_ASSERT_C(initial < text.find("TEvVPut{") && initial < schedule && schedule < final, text);
    UNIT_ASSERT_C(text.Contains("Predictions# [20000000, 20000000, 20000000, 20000000, "
        "20000000, 20000000, 20000000, 20000000]"), text);
    const TStringBuf firstSchedule(text.data() + schedule, text.find('}', schedule) - schedule);
    UNIT_ASSERT_DOUBLES_EQUAL(ReadNumber(firstSchedule, "Attempt"), 1, 0.001);
    UNIT_ASSERT_DOUBLES_EQUAL(ReadNumber(firstSchedule, "PredictedDelayNs"), 60'000'000, 100);
    UNIT_ASSERT_DOUBLES_EQUAL(ReadNumber(firstSchedule, "TimeToAccelerateMs"), 60, 0.001);
    const auto delayMs = ReadNumber(firstSchedule, "DelayMs");
    UNIT_ASSERT_C(delayMs > 0 && delayMs <= 60, text);
}

} // namespace

Y_UNIT_TEST_SUITE(PutRequestHistory) {
    Y_UNIT_TEST(LongPutLogsPredictionsAndScheduling) {
        CheckPutLog(100, true);
    }

    Y_UNIT_TEST(FastPutDoesNotEmitLongRequestLog) {
        CheckPutLog(1000, false);
    }

    Y_UNIT_TEST(SnapshotsKeepQueueValuesAndDoNotAccumulate) {
        TEnvironmentSetup env({.NodeCount = 1});
        const auto edge = env.Runtime->AllocateEdgeActor(1);
        env.Runtime->WrapInActorContext(edge, [&] {
            TGroupMock group(0, TErasureType::Erasure4Plus2Block, 8, 1, 1);
            const auto info = group.GetInfo();
            const auto queues = group.MakeGroupQueues();
            for (ui32 disk = 0; disk < 8; ++disk) {
                group.SetPredictedDelayNs(disk, 999'000'000);
                auto& flow = queues->DisksByOrderNumber[disk]->Queues.FlowRecordForQueueId(NKikimrBlobStorage::PutAsyncBlob);
                flow.Reset(new NBackpressure::TFlowRecord);
                flow->SetPredictedDelayNs((disk + 1) * 1'000'000);
            }
            const TString data(4096, 'x');
            const TLogoBlobID blob(1, 1, 1, 0, data.size(), 0);
            TEvBlobStorage::TEvPut request(blob, data, TInstant::Max(), NKikimrBlobStorage::AsyncBlob);
            TPutImpl impl(info, queues, &request, nullptr, false, TActorId(), 0, NWilson::TTraceId(),
                TAccelerationParams{}, false);
            THistory history(info);
            history.AddPredictions(impl.GetPredictionSnapshot(THistory::EPredictionStage::Initial));
            const TString initial = history.Print();
            UNIT_ASSERT_C(initial.Contains("QueueId# PutAsyncBlob"), initial);
            UNIT_ASSERT_C(initial.Contains("Predictions# [1000000, 2000000, 3000000, 4000000, "
                "5000000, 6000000, 7000000, 8000000]"), initial);
            for (ui32 disk = 0; disk < 8; ++disk) {
                queues->DisksByOrderNumber[disk]->Queues.FlowRecordForQueueId(NKikimrBlobStorage::PutAsyncBlob)
                    ->SetPredictedDelayNs((disk + 11) * 1'000'000);
            }
            const auto current = impl.GetPredictionSnapshot(THistory::EPredictionStage::Current);
            const TString currentText = history.Print(&current);
            UNIT_ASSERT_C(currentText.Contains("Stage# Current"), currentText);
            UNIT_ASSERT_C(!currentText.Contains("Stage# Final"), currentText);
            const auto final = impl.GetPredictionSnapshot(THistory::EPredictionStage::Final);
            const TString finalText = history.Print(&final);
            UNIT_ASSERT_C(finalText.Contains("Stage# Final"), finalText);
            UNIT_ASSERT_C(!finalText.Contains("Stage# Current"), finalText);
            UNIT_ASSERT_C(finalText.Contains("Predictions# [11000000, 12000000, 13000000, 14000000, "
                "15000000, 16000000, 17000000, 18000000]"), finalText);
            UNIT_ASSERT_VALUES_EQUAL(history.Print(), initial);
        });
    }
}
