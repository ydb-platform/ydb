#include "pqrb_ut_common.h"

#include <ydb/core/keyvalue/keyvalue_events.h>
#include <ydb/core/persqueue/common/key.h>
#include <ydb/public/lib/base/msgbus_status.h>

namespace NKikimr::NPQ {
namespace {

struct TMovingPartitionEnv {
    TTestContext Context;
    const TString Key = TKeyPrefix(TKeyPrefix::TypeInfo, TPartitionId(0), TKeyPrefix::MarkTopicMoving).ToString();
    TVector<ui64> Acks;
    THolder<IEventHandle> HeldWrite;
    bool BlockWrites = false;
    size_t SnapshotWrites = 0;

    explicit TMovingPartitionEnv(bool kafka = false) {
        Context.Prepare();
        Context.Runtime->SetScheduledLimit(10000);
        if (kafka) {
            for (ui32 node = 0; node < Context.Runtime->GetNodeCount(); ++node) {
                Context.Runtime->GetAppData(node).FeatureFlags.SetEnableTopicMessagesBatching(true);
                Context.Runtime->GetAppData(node).FeatureFlags.SetEnableTopicWriteOffsetDeltaInKeys(true);
            }
        }
        PQTabletPrepare({}, {{"client", false}, {"second", false}}, Context);
        Context.Runtime->SetObserverFunc([this](TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvKeyValue::TEvRequest::EventType) {
                for (const auto& write : event->Get<TEvKeyValue::TEvRequest>()->Record.GetCmdWrite()) {
                    if (write.GetKey() == Key) {
                        ++SnapshotWrites;
                        if (BlockWrites) {
                            UNIT_ASSERT(!HeldWrite);
                            HeldWrite.Reset(event.Release());
                            return TTestActorRuntimeBase::EEventAction::DROP;
                        }
                        break;
                    }
                }
            } else if (event->GetTypeRewrite() == TEvPQ::TEvPartitionConsumerOffsetsAck::EventType) {
                UNIT_ASSERT_VALUES_EQUAL(event->Get<TEvPQ::TEvPartitionConsumerOffsetsAck>()->Record.GetPartitionId(), 0);
                Acks.push_back(event->Cookie);
                return TTestActorRuntimeBase::EEventAction::DROP;
            }
            return TTestActorRuntimeBase::EEventAction::PROCESS;
        });
    }

    ~TMovingPartitionEnv() {
        Context.Runtime->SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
    }

    void Drain() {
        Context.Runtime->SimulateSleep(TDuration::MilliSeconds(200));
    }

    NKikimrPQ::TEvPartitionConsumerOffsets MakeOffsets(ui64 first = 5, ui64 second = 11) {
        NKikimrPQ::TEvPartitionConsumerOffsets offsets;
        offsets.SetPartitionId(0);
        offsets.SetStartOffset(0);
        offsets.SetEndOffset(12);
        auto* consumer = offsets.AddConsumerOffsets();
        consumer->SetConsumer("client");
        consumer->SetCommittedOffset(first);
        consumer = offsets.AddConsumerOffsets();
        consumer->SetConsumer("second");
        consumer->SetCommittedOffset(second);
        return offsets;
    }

    void SendOffsets(const NKikimrPQ::TEvPartitionConsumerOffsets& offsets, ui64 cookie) {
        auto event = MakeHolder<TEvPQ::TEvPartitionConsumerOffsets>();
        event->Record = offsets;
        Context.Runtime->SendToPipe(Context.TabletId, Context.Edge, event.Release(), 0,
            GetPipeConfigWithRetries(), TActorId(), cookie);
        Drain();
    }

    std::optional<NKikimrPQ::TEvPartitionConsumerOffsets> ReadSnapshot() {
        auto request = MakeHolder<TEvKeyValue::TEvRequest>();
        request->Record.AddCmdRead()->SetKey(Key);
        Context.Runtime->SendToPipe(Context.TabletId, Context.Edge, request.Release());
        auto response = Context.Runtime->GrabEdgeEvent<TEvKeyValue::TEvResponse>(TDuration::Seconds(5));
        UNIT_ASSERT(response);
        UNIT_ASSERT_VALUES_EQUAL(response->Record.GetStatus(), NMsgBusProxy::MSTATUS_OK);
        const auto& read = response->Record.GetReadResult(0);
        if (read.GetStatus() == NKikimrProto::NODATA) {
            return std::nullopt;
        }
        UNIT_ASSERT_VALUES_EQUAL(read.GetStatus(), static_cast<ui32>(NKikimrProto::OK));
        NKikimrPQ::TEvPartitionConsumerOffsets offsets;
        UNIT_ASSERT(offsets.ParseFromString(read.GetValue()));
        return offsets;
    }

    void Fill(bool kafka) {
        if (kafka) {
            CmdWriteBatched(0, "source", 1, "small", 1, Context);
            CmdWriteBatched(0, "source", 2, TString(600_KB, 'x'), 2, Context, 10, false, 3);
        } else {
            CmdWrite(0, "source", {{1, "small"}}, Context);
            CmdWrite(0, "source", {{2, TString(600_KB, 'x')}, {3, "last"}}, Context,
                false, {}, false, "", -1, 10);
        }
    }
};

void CheckDurableImport(bool kafka) {
    TMovingPartitionEnv env(kafka);
    env.Fill(kafka);
    const auto offsets = env.MakeOffsets();
    env.BlockWrites = true;
    env.SendOffsets(offsets, 101);
    UNIT_ASSERT(env.HeldWrite);
    env.SendOffsets(offsets, 102);
    UNIT_ASSERT(env.Acks.empty());
    UNIT_ASSERT(!env.ReadSnapshot());
    CmdGetOffset(0, "client", 0, env.Context);
    CmdGetOffset(0, "second", 0, env.Context);

    env.BlockWrites = false;
    env.Context.Runtime->Send(env.HeldWrite.Release(), 0, true);
    env.Drain();
    UNIT_ASSERT_VALUES_EQUAL(env.Acks.size(), 2);
    CmdGetOffset(0, "client", 5, env.Context); // Offset is inside the mirrored gap.
    CmdGetOffset(0, "second", 11, env.Context);
    auto stored = env.ReadSnapshot();
    UNIT_ASSERT(stored);
    UNIT_ASSERT_VALUES_EQUAL(stored->SerializeAsString(), offsets.SerializeAsString());

    PQTabletRestart(env.Context);
    CmdGetOffset(0, "client", 5, env.Context);
    CmdGetOffset(0, "second", 11, env.Context);
    // A lost ACK can be retried after restart without rewriting or rewinding progress.
    CmdSetOffset(0, "client", 12, false, env.Context);
    const auto writes = env.SnapshotWrites;
    auto reordered = offsets;
    reordered.MutableConsumerOffsets()->SwapElements(0, 1);
    env.SendOffsets(reordered, 103);
    UNIT_ASSERT_VALUES_EQUAL(env.Acks.back(), 103);
    UNIT_ASSERT_VALUES_EQUAL(env.SnapshotWrites, writes);
    CmdGetOffset(0, "client", 12, env.Context);

    const auto replacement = env.MakeOffsets(1, 10);
    env.SendOffsets(replacement, 104);
    UNIT_ASSERT_VALUES_EQUAL(env.Acks.back(), 104);
    PQTabletRestart(env.Context);
    CmdGetOffset(0, "client", 1, env.Context);
    CmdGetOffset(0, "second", 10, env.Context);
    UNIT_ASSERT_VALUES_EQUAL(env.ReadSnapshot()->SerializeAsString(), replacement.SerializeAsString());
}

} // namespace

Y_UNIT_TEST_SUITE(TPartitionMoving) {

Y_UNIT_TEST(DurableOffsetsNativeMessages) {
    CheckDurableImport(false);
}

Y_UNIT_TEST(DurableOffsetsKafkaBatches) {
    CheckDurableImport(true);
}

Y_UNIT_TEST(RestartBeforePersistDoesNotAcknowledgeOrApplyOffsets) {
    TMovingPartitionEnv env;
    env.Fill(false);
    const auto offsets = env.MakeOffsets();
    env.BlockWrites = true;
    env.SendOffsets(offsets, 201);
    UNIT_ASSERT(env.HeldWrite);
    UNIT_ASSERT(env.Acks.empty());
    env.HeldWrite.Reset();
    env.BlockWrites = false;
    PQTabletRestart(env.Context);
    CmdGetOffset(0, "client", 0, env.Context);
    CmdGetOffset(0, "second", 0, env.Context);
    UNIT_ASSERT(!env.ReadSnapshot());
    env.SendOffsets(offsets, 202);
    UNIT_ASSERT_VALUES_EQUAL(env.Acks.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(env.Acks.back(), 202);
    CmdGetOffset(0, "client", 5, env.Context);
    CmdGetOffset(0, "second", 11, env.Context);
}

Y_UNIT_TEST(UnknownConsumerDoesNotPartiallyApplyOffsets) {
    TMovingPartitionEnv env;
    env.Fill(false);
    auto offsets = env.MakeOffsets();
    offsets.MutableConsumerOffsets(1)->SetConsumer("unknown");
    env.SendOffsets(offsets, 301);
    UNIT_ASSERT(env.Acks.empty());
    UNIT_ASSERT(!env.ReadSnapshot());
    CmdGetOffset(0, "client", 0, env.Context);
    env.SendOffsets(env.MakeOffsets(), 302);
    UNIT_ASSERT_VALUES_EQUAL(env.Acks.back(), 302);
    CmdGetOffset(0, "client", 5, env.Context);
}

} // Y_UNIT_TEST_SUITE(TPartitionMoving)
} // namespace NKikimr::NPQ
