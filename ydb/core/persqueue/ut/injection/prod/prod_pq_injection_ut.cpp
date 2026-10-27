#include <ydb/core/keyvalue/keyvalue_events.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/public/write_id.h>
#include <ydb/core/persqueue/ut/common/pq_ut_common.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/tx.h>
#include <ydb/core/tx/tx_processing.h>
#include <ydb/public/lib/base/msgbus_status.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/maybe.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>

#include <unordered_set>

namespace NKikimr::NPQ {
namespace {

using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

// Stock TTestEnv boots FakeCoordinator (PlanStep → tablet pipe; no Mediator queue).
// FakeCoordinator is the plan deliverer in reboot/pipe-reset targets.

constexpr TDuration kEdgeTimeout = TDuration::Seconds(2);
constexpr ui32 kSeedMsgCount = 2;
constexpr ui32 kTxWriteMsgCount = 2;
constexpr ui32 kPqTabletCount = 4;
constexpr ui32 kAlterLifetimeSeconds = 7200;
constexpr ui32 kCreateLifetimeSeconds = 3600;
constexpr const char* kTopicName = "TopicTx";
constexpr const char* kTopicPath = "/MyRoot/DirA/TopicTx";
constexpr const char* kConsumer = "user";
constexpr const char* kTxWriteOwner = "prod-tx-write-owner";
constexpr const char* kTxWriteSourceId = "prod-tx-src";

// Half-range boundary used by schemeshard split UTs (1/2 of ui128 key space).
const unsigned char kSplitBoundHalf[] = {
    0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFE};

class TProdPqRetry : public yexception {
};

struct TPartitionRef {
    ui32 PartitionId = 0;
    ui64 TabletId = 0;
};

TString TopicCreateScheme(
    ui32 partitions,
    ui32 partitionsPerTablet = 1,
    ui32 lifetimeSeconds = kCreateLifetimeSeconds,
    bool canSplitAndMerge = false)
{
    TStringBuilder sb;
    sb << "Name: \"" << kTopicName << "\" "
       << "TotalGroupCount: " << partitions << " "
       << "PartitionPerTablet: " << partitionsPerTablet << " "
       << "PQTabletConfig: {"
       << "  PartitionConfig { LifetimeSeconds: " << lifetimeSeconds << " }"
       << "  Consumers { Name: \"" << kConsumer << "\" Important: true }";
    if (canSplitAndMerge) {
        sb << "  PartitionStrategy { PartitionStrategyType: CAN_SPLIT_AND_MERGE }";
    }
    sb << "}";
    return sb;
}

ui32 GetTopicLifetimeSeconds(TTestActorRuntime& runtime) {
    return DescribePath(runtime, kTopicPath)
        .GetPathDescription()
        .GetPersQueueGroup()
        .GetPQTabletConfig()
        .GetPartitionConfig()
        .GetLifetimeSeconds();
}

ui32 GetTopicPartitionCount(TTestActorRuntime& runtime) {
    return DescribePath(runtime, kTopicPath, /*returnPartitioning=*/true)
        .GetPathDescription()
        .GetPersQueueGroup()
        .PartitionsSize();
}

TVector<TPartitionRef> DescribePartitions(TTestActorRuntime& runtime) {
    auto desc = DescribePath(runtime, kTopicPath, /*returnPartitioning=*/true)
                    .GetPathDescription()
                    .GetPersQueueGroup();
    TVector<TPartitionRef> parts;
    parts.reserve(desc.PartitionsSize());
    for (const auto& p : desc.GetPartitions()) {
        parts.push_back({.PartitionId = p.GetPartitionId(), .TabletId = p.GetTabletId()});
    }
    UNIT_ASSERT_C(!parts.empty(), "topic has no partitions");
    return parts;
}

THashSet<ui64> UniqueTabletIds(const TVector<TPartitionRef>& parts) {
    THashSet<ui64> ids;
    for (const auto& p : parts) {
        ids.insert(p.TabletId);
    }
    return ids;
}

void WriteToPartition(
    TTestActorRuntime& runtime,
    const TPartitionRef& part,
    ui32& msgNo,
    ui64 sourceSeqNo,
    const TString& payload,
    bool isFirst)
{
    const TActorId edge = runtime.AllocateEdgeActor();
    const TString cookie = CmdSetOwner(
        &runtime, part.TabletId, edge, part.PartitionId, "default", true).first;
    TVector<std::pair<ui64, TString>> data{{sourceSeqNo, payload}};
    CmdWrite(
        &runtime, part.TabletId, edge, part.PartitionId, "sourceid0", msgNo, data,
        false, {}, isFirst, cookie, 0);
}

void SeedAllPartitions(TTestActorRuntime& runtime, const TVector<TPartitionRef>& parts) {
    for (const auto& part : parts) {
        ui32 msgNo = 0;
        for (ui32 i = 0; i < kSeedMsgCount; ++i) {
            WriteToPartition(
                runtime, part, msgNo, /*sourceSeqNo=*/i + 1,
                TStringBuilder() << "seed-" << part.PartitionId << "-" << i,
                /*isFirst=*/(i == 0));
        }
    }
}

i64 GetClientOffset(TTestActorRuntime& runtime, const TPartitionRef& part) {
    const TActorId edge = runtime.AllocateEdgeActor();
    auto request = MakeHolder<TEvPersQueue::TEvRequest>();
    auto* req = request->Record.MutablePartitionRequest();
    req->SetPartition(part.PartitionId);
    req->SetCookie(part.TabletId);
    req->MutableCmdGetClientOffset()->SetClientId(kConsumer);
    runtime.SendToPipe(
        part.TabletId, edge, request.Release(), 0, GetPipeConfigWithRetries());
    auto handle = runtime.GrabEdgeEvent<TEvPersQueue::TEvResponse>(edge, kEdgeTimeout);
    UNIT_ASSERT(handle);
    const auto& record = handle->Get()->Record;
    UNIT_ASSERT_VALUES_EQUAL(
        (int)record.GetErrorCode(), (int)NPersQueue::NErrorCode::OK);
    UNIT_ASSERT(record.GetPartitionResponse().HasCmdGetClientOffsetResult());
    return record.GetPartitionResponse().GetCmdGetClientOffsetResult().GetOffset();
}

bool AllOffsetsAt(TTestActorRuntime& runtime, const TVector<TPartitionRef>& parts, i64 expected) {
    for (const auto& part : parts) {
        if (GetClientOffset(runtime, part) != expected) {
            return false;
        }
    }
    return true;
}

void AssertAllOffsetsAt(TTestActorRuntime& runtime, const TVector<TPartitionRef>& parts, i64 expected) {
    for (const auto& part : parts) {
        UNIT_ASSERT_VALUES_EQUAL_C(
            GetClientOffset(runtime, part), expected,
            "partition=" << part.PartitionId << " tablet=" << part.TabletId);
    }
}

void ProposeOffsetCommit(
    TTestActorRuntime& runtime,
    const TActorId& edge,
    ui64 txId,
    const TPartitionRef& part,
    const THashSet<ui64>& allTablets,
    ui64 begin,
    ui64 end)
{
    auto event = MakeHolder<TEvPersQueue::TEvProposeTransactionBuilder>();
    ActorIdToProto(edge, event->Record.MutableSourceActor());
    event->Record.SetTxId(txId);
    auto* body = event->Record.MutableData();
    auto* operation = body->MutableOperations()->Add();
    operation->SetPartitionId(part.PartitionId);
    operation->SetCommitOffsetsBegin(begin);
    operation->SetCommitOffsetsEnd(end);
    operation->SetConsumer(kConsumer);
    operation->SetPath(kTopicPath);
    for (ui64 shard : allTablets) {
        if (shard == part.TabletId) {
            continue;
        }
        body->AddSendingShards(shard);
        body->AddReceivingShards(shard);
    }
    body->SetImmediate(false);
    runtime.SendToPipe(
        part.TabletId, edge, event.Release(), 0, GetPipeConfigWithRetries());
}

void WaitPreparedFromTablets(
    TTestActorRuntime& runtime,
    const TActorId& edge,
    ui64 txId,
    ui32 expectedCount)
{
    THashSet<ui64> origins;
    for (ui32 i = 0; i < expectedCount * 8 && origins.size() < expectedCount; ++i) {
        auto handle = runtime.GrabEdgeEvent<TEvPersQueue::TEvProposeTransactionResult>(
            edge, kEdgeTimeout);
        if (!handle) {
            ythrow TProdPqRetry() << "timeout waiting PREPARED";
        }
        const auto& record = handle->Get()->Record;
        if (record.GetTxId() != txId) {
            continue;
        }
        const auto status = record.GetStatus();
        if (status != NKikimrPQ::TEvProposeTransactionResult::PREPARED &&
            status != NKikimrPQ::TEvProposeTransactionResult::COMPLETE)
        {
            ythrow TProdPqRetry()
                << "unexpected propose status=" << static_cast<int>(status);
        }
        UNIT_ASSERT(record.HasOrigin());
        origins.insert(record.GetOrigin());
    }
    if (origins.size() < expectedCount) {
        ythrow TProdPqRetry()
            << "PREPARED got=" << origins.size() << "/" << expectedCount;
    }
}

void PlanViaFakeCoordinator(
    TTestActorRuntime& runtime,
    const TActorId& edge,
    ui64 txId,
    const THashSet<ui64>& tablets)
{
    auto ev = MakeHolder<TEvTxProxy::TEvProposeTransaction>();
    ev->Record.SetCoordinatorID(TTestTxConfig::Coordinator);
    auto& tx = *ev->Record.MutableTransaction();
    tx.SetTxId(txId);
    for (ui64 tabletId : tablets) {
        auto& item = *tx.MutableAffectedSet()->Add();
        item.SetTabletId(tabletId);
        item.SetFlags(TEvTxProxy::TEvProposeTransaction::AffectedWrite);
    }
    runtime.SendToPipe(
        TTestTxConfig::Coordinator, edge, ev.Release(), 0, GetPipeConfigWithRetries());
}

void WaitCompleteFromTablets(
    TTestActorRuntime& runtime,
    const TActorId& edge,
    ui64 txId,
    ui32 expectedCount)
{
    THashSet<ui64> origins;
    for (ui32 i = 0; i < expectedCount * 16 && origins.size() < expectedCount; ++i) {
        auto handle = runtime.GrabEdgeEvent<TEvPersQueue::TEvProposeTransactionResult>(
            edge, kEdgeTimeout);
        if (!handle) {
            ythrow TProdPqRetry() << "timeout waiting COMPLETE";
        }
        const auto& record = handle->Get()->Record;
        if (record.GetTxId() != txId) {
            continue;
        }
        if (record.GetStatus() == NKikimrPQ::TEvProposeTransactionResult::PREPARED) {
            continue;
        }
        if (record.GetStatus() != NKikimrPQ::TEvProposeTransactionResult::COMPLETE) {
            ythrow TProdPqRetry()
                << "unexpected status after plan="
                << static_cast<int>(record.GetStatus());
        }
        UNIT_ASSERT(record.HasOrigin());
        origins.insert(record.GetOrigin());
    }
    if (origins.size() < expectedCount) {
        ythrow TProdPqRetry()
            << "COMPLETE got=" << origins.size() << "/" << expectedCount;
    }
}

void DrainStaleProposeResults(TTestActorRuntime& runtime, const TActorId& edge) {
    for (;;) {
        auto handle = runtime.GrabEdgeEvent<TEvPersQueue::TEvProposeTransactionResult>(
            edge, TDuration::MilliSeconds(1));
        if (!handle) {
            break;
        }
    }
}

struct TPipeSession {
    TActorId Pipe;

    void Reset(TTestActorRuntime& runtime, const TActorId& edge) {
        if (Pipe) {
            runtime.ClosePipe(Pipe, edge, 0);
            Pipe = {};
        }
    }

    void Ensure(TTestActorRuntime& runtime, const TActorId& edge, ui64 tabletId) {
        if (!Pipe) {
            Pipe = runtime.ConnectToPipe(tabletId, edge, 0, GetPipeConfigWithRetries());
        }
        Y_ABORT_UNLESS(Pipe);
    }

    void Send(TTestActorRuntime& runtime, const TActorId& edge, IEventBase* event) {
        runtime.SendToPipe(Pipe, edge, event, 0, 0);
    }
};

ui64 GetEndOffset(TTestActorRuntime& runtime, const TPartitionRef& part) {
    const TActorId edge = runtime.AllocateEdgeActor();
    auto request = MakeHolder<TEvPersQueue::TEvOffsets>();
    runtime.SendToPipe(
        part.TabletId, edge, request.Release(), 0, GetPipeConfigWithRetries());
    auto handle = runtime.GrabEdgeEvent<TEvPersQueue::TEvOffsetsResponse>(edge, kEdgeTimeout);
    if (!handle) {
        ythrow TProdPqRetry() << "timeout GetEndOffset partition=" << part.PartitionId;
    }
    for (const auto& pr : handle->Get()->Record.GetPartResult()) {
        if (static_cast<ui32>(pr.GetPartition()) == part.PartitionId) {
            if (pr.GetErrorCode() == NPersQueue::NErrorCode::INITIALIZING) {
                ythrow TProdPqRetry() << "GetEndOffset INITIALIZING";
            }
            return pr.GetEndOffset();
        }
    }
    ythrow TProdPqRetry() << "GetEndOffset: partition not found id=" << part.PartitionId;
}

bool AllEndOffsetsAt(
    TTestActorRuntime& runtime,
    const TVector<TPartitionRef>& parts,
    ui64 expected)
{
    for (const auto& part : parts) {
        if (GetEndOffset(runtime, part) != expected) {
            return false;
        }
    }
    return true;
}

NKikimrClient::TCmdReadResult CaptureCmdReadResult(
    TTestActorRuntime& runtime,
    const TPartitionRef& part,
    ui64 offset,
    ui32 count)
{
    const TActorId edge = runtime.AllocateEdgeActor();
    auto request = MakeHolder<TEvPersQueue::TEvRequest>();
    auto* req = request->Record.MutablePartitionRequest();
    req->SetPartition(part.PartitionId);
    req->SetCookie(part.TabletId);
    auto* read = req->MutableCmdRead();
    read->SetClientId(kConsumer);
    read->SetSessionId("");
    read->SetOffset(offset);
    read->SetCount(count);
    read->SetBytes(16_MB);
    read->SetReadToBlobEnd(true);
    runtime.SendToPipe(
        part.TabletId, edge, request.Release(), 0, GetPipeConfigWithRetries());
    auto handle = runtime.GrabEdgeEvent<TEvPersQueue::TEvResponse>(edge, kEdgeTimeout);
    if (!handle) {
        ythrow TProdPqRetry() << "timeout CmdRead partition=" << part.PartitionId;
    }
    const auto& record = handle->Get()->Record;
    if (record.GetErrorCode() == NPersQueue::NErrorCode::INITIALIZING) {
        ythrow TProdPqRetry() << "CmdRead INITIALIZING";
    }
    UNIT_ASSERT_VALUES_EQUAL(
        (int)record.GetErrorCode(), (int)NPersQueue::NErrorCode::OK);
    UNIT_ASSERT(record.GetPartitionResponse().HasCmdReadResult());
    return record.GetPartitionResponse().GetCmdReadResult();
}

TString CreateSupportivePartition(
    TTestActorRuntime& runtime,
    TPipeSession& pipe,
    const TActorId& edge,
    const TPartitionRef& part,
    const TWriteId& writeId)
{
    for (i32 retriesLeft = 5; retriesLeft > 0; --retriesLeft) {
        try {
            runtime.ResetScheduledCount();
            pipe.Reset(runtime, edge);
            pipe.Ensure(runtime, edge, part.TabletId);

            auto event = std::make_unique<TEvPersQueue::TEvRequest>();
            auto* request = event->Record.MutablePartitionRequest();
            request->SetPartition(part.PartitionId);
            request->SetCookie(4);
            request->SetNeedSupportivePartition(true);
            SetWriteId(*request, writeId);
            ActorIdToProto(pipe.Pipe, request->MutablePipeClient());
            auto* cmd = request->MutableCmdGetOwnership();
            cmd->SetOwner(kTxWriteOwner);
            cmd->SetForce(true);

            pipe.Send(runtime, edge, event.release());
            auto response = runtime.GrabEdgeEvent<TEvPersQueue::TEvResponse>(edge, kEdgeTimeout);
            if (!response) {
                continue;
            }
            const auto& record = response->Get()->Record;
            if (record.GetErrorCode() == NPersQueue::NErrorCode::INITIALIZING) {
                runtime.DispatchEvents();
                retriesLeft = 5;
                continue;
            }
            UNIT_ASSERT_VALUES_EQUAL(
                (int)record.GetStatus(), (int)NMsgBusProxy::MSTATUS_OK);
            return record.GetPartitionResponse().GetCmdGetOwnershipResult().GetOwnerCookie();
        } catch (const NActors::TSchedulingLimitReachedException&) {
        } catch (const NActors::TEmptyEventQueueException&) {
        }
    }
    ythrow TProdPqRetry() << "CreateSupportivePartition exhausted partition=" << part.PartitionId;
}

void WriteToSupportive(
    TTestActorRuntime& runtime,
    TPipeSession& pipe,
    const TActorId& edge,
    const TPartitionRef& part,
    const TWriteId& writeId,
    const TString& ownerCookie,
    ui64 seqNo,
    ui64 messageNo,
    const TString& data,
    ui64 cookie)
{
    for (i32 retriesLeft = 5; retriesLeft > 0; --retriesLeft) {
        try {
            runtime.ResetScheduledCount();
            pipe.Ensure(runtime, edge, part.TabletId);

            auto event = MakeHolder<TEvPersQueue::TEvRequest>();
            auto* request = event->Record.MutablePartitionRequest();
            request->SetTopic(kTopicPath);
            request->SetPartition(part.PartitionId);
            request->SetCookie(cookie);
            request->SetOwnerCookie(ownerCookie);
            request->SetMessageNo(messageNo);
            SetWriteId(*request, writeId);
            ActorIdToProto(pipe.Pipe, request->MutablePipeClient());

            auto* cmdWrite = request->AddCmdWrite();
            cmdWrite->SetSourceId(kTxWriteSourceId);
            cmdWrite->SetSeqNo(seqNo);
            cmdWrite->SetData(data);
            cmdWrite->SetCreateTimeMS(TInstant::Now().MilliSeconds());
            cmdWrite->SetDisableDeduplication(true);
            cmdWrite->SetUncompressedSize(data.size());
            cmdWrite->SetIgnoreQuotaDeadline(true);
            cmdWrite->SetExternalOperation(true);

            pipe.Send(runtime, edge, event.Release());
            auto response = runtime.GrabEdgeEvent<TEvPersQueue::TEvResponse>(edge, kEdgeTimeout);
            if (!response) {
                pipe.Reset(runtime, edge);
                continue;
            }
            const auto& record = response->Get()->Record;
            if (record.GetErrorCode() == NPersQueue::NErrorCode::INITIALIZING) {
                pipe.Reset(runtime, edge);
                runtime.DispatchEvents();
                retriesLeft = 5;
                continue;
            }
            if (record.GetErrorCode() != NPersQueue::NErrorCode::OK) {
                ythrow TProdPqRetry()
                    << "WriteToSupportive error="
                    << static_cast<int>(record.GetErrorCode());
            }
            UNIT_ASSERT_VALUES_EQUAL(
                record.GetPartitionResponse().GetCookie(), cookie);
            return;
        } catch (const NActors::TSchedulingLimitReachedException&) {
            pipe.Reset(runtime, edge);
        } catch (const NActors::TEmptyEventQueueException&) {
            pipe.Reset(runtime, edge);
        }
    }
    ythrow TProdPqRetry() << "WriteToSupportive exhausted partition=" << part.PartitionId;
}

ui32 WaitSupportivePartitionId(
    TTestActorRuntime& runtime,
    TPipeSession& pipe,
    const TActorId& edge,
    const TPartitionRef& part,
    const TWriteId& writeId)
{
    for (size_t i = 0; i < 40; ++i) {
        try {
            runtime.ResetScheduledCount();
            pipe.Ensure(runtime, edge, part.TabletId);
            auto request = std::make_unique<TEvKeyValue::TEvRequest>();
            request->Record.SetCookie(12345);
            request->Record.AddCmdRead()->SetKey("_txinfo");
            pipe.Send(runtime, edge, request.release());

            auto response = runtime.GrabEdgeEvent<TEvKeyValue::TEvResponse>(edge, kEdgeTimeout);
            if (!response) {
                pipe.Reset(runtime, edge);
                continue;
            }
            const auto& kvRecord = response->Get()->Record;
            UNIT_ASSERT_VALUES_EQUAL(kvRecord.GetStatus(), NMsgBusProxy::MSTATUS_OK);
            const auto& result = kvRecord.GetReadResult(0);
            if (result.GetStatus() == static_cast<ui32>(NKikimrProto::OK)) {
                NKikimrPQ::TTabletTxInfo info;
                UNIT_ASSERT(info.ParseFromString(result.GetValue()));
                for (const auto& txWrite : info.GetTxWrites()) {
                    if (GetWriteId(txWrite) == writeId) {
                        return txWrite.GetInternalPartitionId();
                    }
                }
            } else if (result.GetStatus() != NKikimrProto::NODATA) {
                ythrow TProdPqRetry()
                    << "WaitSupportivePartitionId KV status=" << result.GetStatus();
            }
            runtime.SimulateSleep(TDuration::MilliSeconds(50));
        } catch (const NActors::TSchedulingLimitReachedException&) {
            ythrow TProdPqRetry() << "WaitSupportivePartitionId scheduling limit";
        } catch (const NActors::TEmptyEventQueueException&) {
            ythrow TProdPqRetry() << "WaitSupportivePartitionId empty queue";
        }
    }
    ythrow TProdPqRetry()
        << "supportive partition id missing in _txinfo partition=" << part.PartitionId;
}

void ProposeTopicWriteCommit(
    TTestActorRuntime& runtime,
    const TActorId& edge,
    ui64 txId,
    const TPartitionRef& part,
    const THashSet<ui64>& allTablets,
    const TWriteId& writeId,
    ui32 supportivePartitionId)
{
    auto event = MakeHolder<TEvPersQueue::TEvProposeTransactionBuilder>();
    ActorIdToProto(edge, event->Record.MutableSourceActor());
    event->Record.SetTxId(txId);
    auto* body = event->Record.MutableData();
    auto* operation = body->MutableOperations()->Add();
    operation->SetPartitionId(part.PartitionId);
    operation->SetPath(kTopicPath);
    operation->SetSupportivePartition(supportivePartitionId);
    for (ui64 shard : allTablets) {
        if (shard == part.TabletId) {
            continue;
        }
        body->AddSendingShards(shard);
        body->AddReceivingShards(shard);
    }
    SetWriteId(*body, writeId);
    body->SetImmediate(false);
    runtime.SendToPipe(
        part.TabletId, edge, event.Release(), 0, GetPipeConfigWithRetries());
}

void AssertTopicWriteCommitted(
    TTestActorRuntime& runtime,
    const TVector<TPartitionRef>& parts,
    ui64 readFrom,
    ui32 txCount,
    TMaybe<ui32> expectedAttempt)
{
    for (const auto& part : parts) {
        UNIT_ASSERT_VALUES_EQUAL_C(
            GetEndOffset(runtime, part), readFrom + txCount,
            "partition=" << part.PartitionId);
        const auto readResult = CaptureCmdReadResult(runtime, part, readFrom, txCount);
        UNIT_ASSERT_VALUES_EQUAL(readResult.ResultSize(), txCount);
        for (ui32 i = 0; i < txCount; ++i) {
            const auto& row = readResult.GetResult(i);
            UNIT_ASSERT_VALUES_EQUAL(row.GetOffset(), readFrom + i);
            if (expectedAttempt.Defined()) {
                UNIT_ASSERT_VALUES_EQUAL(
                    row.GetData(),
                    TStringBuilder()
                        << "prod-tx-write-" << *expectedAttempt
                        << "-" << part.PartitionId << "-" << i);
            } else {
                UNIT_ASSERT_C(
                    row.GetData().StartsWith("prod-tx-write-"),
                    "unexpected payload=" << row.GetData());
            }
        }
    }
}

TTestEnvOptions MakeEnvOptions() {
    return TTestWithReboots::GetDefaultTestEnvOptions().RunFakeConfigDispatcher(true);
}

// Data-plane reboot targets: Hive-owned PQ tablets + FakeCoordinator (plan deliverer).
// FakeHive assigns PQ ids at FakeHiveTablets..+(N-1); balancer follows.
TVector<ui64> DataPlaneRebootTablets(ui32 pqTabletCount = kPqTabletCount) {
    TVector<ui64> ids;
    ids.reserve(pqTabletCount + 1);
    for (ui32 i = 0; i < pqTabletCount; ++i) {
        ids.push_back(TTestTxConfig::FakeHiveTablets + i);
    }
    ids.push_back(TTestTxConfig::Coordinator);
    return ids;
}

// Control-plane: PQ + FakeCoordinator.
// SchemeShard is intentionally not in the reboot vector here: SS-only reboots are
// already covered by schemeshard/ut_pq_reboots, and rebooting SS mid-alter under
// RunTestWithReboots livelocks the runtime (event storm past SetScheduledLimit).
// Injection here focuses on PQ config/split propose + plan delivery.
TVector<ui64> ControlPlaneRebootTablets(ui32 pqTabletCount = kPqTabletCount) {
    TVector<ui64> ids;
    ids.reserve(pqTabletCount + 1);
    for (ui32 i = 0; i < pqTabletCount; ++i) {
        ids.push_back(TTestTxConfig::FakeHiveTablets + i);
    }
    ids.push_back(TTestTxConfig::Coordinator);
    return ids;
}

struct TProdPqEnv {
    THolder<TTestBasicRuntime> Runtime;
    THolder<TTestEnv> Env;
    ui64 TxId = 1000;
    TActorId Edge;
    TVector<TPartitionRef> Parts;
    THashSet<ui64> PqTabletIds;
    ui32 ExpectedPqTabletCount = kPqTabletCount;

    void Prepare(
        const TString& dispatchName,
        std::function<void(TTestActorRuntime&)> setup,
        bool& activeZone,
        bool seedMessages = true,
        ui32 partitions = kPqTabletCount,
        bool canSplitAndMerge = false)
    {
        activeZone = false;
        ExpectedPqTabletCount = partitions;
        Cerr << Endl
             << "====== " << TInstant::Now()
             << " ===== PROD_PQ RUN: " << dispatchName
             << " ===========" << Endl;
        Runtime.Reset(new TTestBasicRuntime());
        Runtime->SetScheduledLimit(50'000);
        setup(*Runtime);
        Env.Reset(new TTestEnv(*Runtime, MakeEnvOptions()));
        Env->SetupLogging(*Runtime);
        TxId = 1000;
        TestMkDir(*Runtime, ++TxId, "/MyRoot", "DirA");
        Edge = Runtime->AllocateEdgeActor();

        // Topic create (+ optional seed) stay outside the reboot/pipe-reset zone.
        // One partition per tablet → N real PQ tablets (+ RS mesh when N>1).
        TestCreatePQGroup(
            *Runtime, ++TxId, "/MyRoot/DirA",
            TopicCreateScheme(partitions, /*perTablet=*/1, kCreateLifetimeSeconds, canSplitAndMerge));
        Env->TestWaitNotification(*Runtime, TxId);
        Parts = DescribePartitions(*Runtime);
        PqTabletIds = UniqueTabletIds(Parts);
        UNIT_ASSERT_VALUES_EQUAL(PqTabletIds.size(), ExpectedPqTabletCount);
        if (seedMessages) {
            SeedAllPartitions(*Runtime, Parts);
        }
        Cerr << "====== PROD_PQ prepared tablets=";
        for (ui64 id : PqTabletIds) {
            Cerr << " " << id;
        }
        Cerr << " ======" << Endl;
    }
};

// Active zone: first TEvProposeTransaction → COMPLETE (commit durable / offsets applied).
// Retries re-propose; never recreate the topic.
void DataPlaneOffsetCommitScenario(TProdPqEnv& env, bool& activeZone) {
    for (ui32 attempt = 0; attempt < 15; ++attempt) {
        try {
            env.Runtime->ResetScheduledCount();
            activeZone = false;
            DrainStaleProposeResults(*env.Runtime, env.Edge);

            if (AllOffsetsAt(*env.Runtime, env.Parts, kSeedMsgCount)) {
                Cerr << "====== PROD_PQ already committed, skip attempt="
                     << attempt << " ======" << Endl;
                return;
            }

            const ui64 commitTxId = 9400 + attempt;
            const auto& tablets = env.PqTabletIds;

            Cerr << "====== PROD_PQ attempt=" << attempt
                 << " txId=" << commitTxId
                 << " enter activeZone ======" << Endl;

            // Injection covers propose → plan → RS mesh → COMPLETE.
            activeZone = true;
            for (const auto& part : env.Parts) {
                ProposeOffsetCommit(
                    *env.Runtime, env.Edge, commitTxId, part, tablets,
                    /*begin=*/0, /*end=*/kSeedMsgCount);
            }
            WaitPreparedFromTablets(*env.Runtime, env.Edge, commitTxId, tablets.size());
            PlanViaFakeCoordinator(*env.Runtime, env.Edge, commitTxId, tablets);
            WaitCompleteFromTablets(*env.Runtime, env.Edge, commitTxId, tablets.size());
            activeZone = false;

            AssertAllOffsetsAt(*env.Runtime, env.Parts, kSeedMsgCount);
            Cerr << "====== PROD_PQ attempt=" << attempt
                 << " txId=" << commitTxId
                 << " COMPLETE ok ======" << Endl;
            return;
        } catch (const TProdPqRetry& ex) {
            activeZone = false;
            Cerr << "====== PROD_PQ attempt=" << attempt
                 << " retry: " << ex.what() << " ======" << Endl;
            try {
                env.Runtime->ResetScheduledCount();
                env.Runtime->SimulateSleep(TDuration::MilliSeconds(50));
            } catch (...) {
            }
        } catch (const NActors::TSchedulingLimitReachedException&) {
            activeZone = false;
            Cerr << "====== PROD_PQ attempt=" << attempt
                 << " scheduling limit ======" << Endl;
        } catch (const NActors::TEmptyEventQueueException&) {
            activeZone = false;
            Cerr << "====== PROD_PQ attempt=" << attempt
                 << " empty event queue ======" << Endl;
        }
    }
    UNIT_FAIL("DataPlaneOffsetCommitScenario: retries exhausted");
}

// Active zone: ProposeTransaction(write) → COMPLETE (messages published to real partitions).
// Ownership + supportive writes stay outside the zone (same discipline as L0 TopicTxWrite*).
void DataPlaneTopicWriteScenario(TProdPqEnv& env, bool& activeZone) {
    THashMap<ui64, TPipeSession> pipes;
    for (ui64 tabletId : env.PqTabletIds) {
        pipes[tabletId] = TPipeSession{};
    }

    auto resetPipes = [&]() {
        for (auto& [_, pipe] : pipes) {
            pipe.Reset(*env.Runtime, env.Edge);
        }
    };

    for (ui32 attempt = 0; attempt < 15; ++attempt) {
        try {
            env.Runtime->ResetScheduledCount();
            activeZone = false;
            resetPipes();
            DrainStaleProposeResults(*env.Runtime, env.Edge);

            // Previous attempt may have committed while a later GrabEdgeEvent failed.
            if (AllEndOffsetsAt(*env.Runtime, env.Parts, kTxWriteMsgCount)) {
                Cerr << "====== PROD_PQ write already committed, skip attempt="
                     << attempt << " ======" << Endl;
                AssertTopicWriteCommitted(
                    *env.Runtime, env.Parts, /*readFrom=*/0,
                    kTxWriteMsgCount, /*expectedAttempt=*/Nothing());
                return;
            }

            const ui64 endBefore = GetEndOffset(*env.Runtime, env.Parts.front());
            for (const auto& part : env.Parts) {
                UNIT_ASSERT_VALUES_EQUAL_C(
                    GetEndOffset(*env.Runtime, part), endBefore,
                    "partitions diverged before write-tx");
            }

            const ui64 writeTxId = 9500 + attempt;
            const auto& tablets = env.PqTabletIds;

            Cerr << "====== PROD_PQ write attempt=" << attempt
                 << " txId=" << writeTxId
                 << " endBefore=" << endBefore
                 << " prepare supportive ======" << Endl;

            struct TPreparedWrite {
                TPartitionRef Part;
                TWriteId WriteId;
                ui32 SupportivePartitionId = 0;
            };
            TVector<TPreparedWrite> prepared;
            prepared.reserve(env.Parts.size());

            for (const auto& part : env.Parts) {
                const TWriteId writeId(part.TabletId, 1000 + attempt);
                auto& pipe = pipes[part.TabletId];
                const TString ownerCookie = CreateSupportivePartition(
                    *env.Runtime, pipe, env.Edge, part, writeId);
                for (ui32 i = 0; i < kTxWriteMsgCount; ++i) {
                    WriteToSupportive(
                        *env.Runtime, pipe, env.Edge, part, writeId, ownerCookie,
                        /*seqNo=*/i, /*messageNo=*/i,
                        TStringBuilder()
                            << "prod-tx-write-" << attempt
                            << "-" << part.PartitionId << "-" << i,
                        /*cookie=*/500 + i);
                }
                const ui32 supportiveId = WaitSupportivePartitionId(
                    *env.Runtime, pipe, env.Edge, part, writeId);
                prepared.push_back({part, writeId, supportiveId});
            }

            Cerr << "====== PROD_PQ write attempt=" << attempt
                 << " txId=" << writeTxId
                 << " enter activeZone ======" << Endl;

            activeZone = true;
            for (const auto& item : prepared) {
                ProposeTopicWriteCommit(
                    *env.Runtime, env.Edge, writeTxId, item.Part, tablets,
                    item.WriteId, item.SupportivePartitionId);
            }
            WaitPreparedFromTablets(*env.Runtime, env.Edge, writeTxId, tablets.size());
            PlanViaFakeCoordinator(*env.Runtime, env.Edge, writeTxId, tablets);
            WaitCompleteFromTablets(*env.Runtime, env.Edge, writeTxId, tablets.size());
            activeZone = false;

            AssertTopicWriteCommitted(
                *env.Runtime, env.Parts, endBefore, kTxWriteMsgCount, attempt);
            Cerr << "====== PROD_PQ write attempt=" << attempt
                 << " txId=" << writeTxId
                 << " COMPLETE ok ======" << Endl;
            return;
        } catch (const TProdPqRetry& ex) {
            activeZone = false;
            Cerr << "====== PROD_PQ write attempt=" << attempt
                 << " retry: " << ex.what() << " ======" << Endl;
            try {
                env.Runtime->ResetScheduledCount();
                env.Runtime->SimulateSleep(TDuration::MilliSeconds(50));
            } catch (...) {
            }
        } catch (const NActors::TSchedulingLimitReachedException&) {
            activeZone = false;
            Cerr << "====== PROD_PQ write attempt=" << attempt
                 << " scheduling limit ======" << Endl;
        } catch (const NActors::TEmptyEventQueueException&) {
            activeZone = false;
            Cerr << "====== PROD_PQ write attempt=" << attempt
                 << " empty event queue ======" << Endl;
        }
    }
    UNIT_FAIL("DataPlaneTopicWriteScenario: retries exhausted");
}

void RunDataPlaneInjection(
    std::function<void(
        const TVector<ui64>&,
        std::function<TTestActorRuntime::TEventFilter()>,
        std::function<void(const TString&, std::function<void(TTestActorRuntime&)>, bool&)>)> runner,
    std::function<void(TProdPqEnv&, bool&)> scenario,
    bool seedMessages = true,
    const std::unordered_set<TString>& extraSkipEventTypes = {})
{
    TInitialEventsFilter filter;
    runner(
        DataPlaneRebootTablets(),
        [&]() {
            return filter.Prepare(
                {TabletPipe, NPDisk, KeyValue, PQ},
                extraSkipEventTypes);
        },
        [&](const TString& dispatchName,
            std::function<void(TTestActorRuntime&)> setup,
            bool& activeZone) {
            TProdPqEnv env;
            activeZone = false;
            env.Prepare(dispatchName, setup, activeZone, seedMessages);
            scenario(env, activeZone);
        });
}

void AlterTopicLifetime(TProdPqEnv& env, ui32 lifetimeSeconds) {
    TestAlterPQGroup(
        *env.Runtime, ++env.TxId, "/MyRoot/DirA",
        TStringBuilder()
            << "Name: \"" << kTopicName << "\" "
            << "PQTabletConfig: {"
            << "  PartitionConfig { LifetimeSeconds: " << lifetimeSeconds << " }"
            << "  Consumers { Name: \"" << kConsumer << "\" Important: true }"
            << "}");
    env.Env->TestWaitNotification(*env.Runtime, env.TxId);
}

void SplitTopicPartition(TProdPqEnv& env, ui32 partitionId, const TString& boundary) {
    NKikimrSchemeOp::TPersQueueGroupDescription scheme;
    scheme.SetName(kTopicName);
    scheme.MutablePQTabletConfig()->MutablePartitionConfig();
    auto* split = scheme.AddSplit();
    split->SetPartition(partitionId);
    split->SetSplitBoundary(boundary);

    TStringBuilder sb;
    sb << scheme;
    const TString schemeText = sb.substr(1, sb.size() - 2);
    Cerr << "====== PROD_PQ split scheme: " << schemeText << " ======" << Endl;
    TestAlterPQGroup(*env.Runtime, ++env.TxId, "/MyRoot/DirA", schemeText);
    env.Env->TestWaitNotification(*env.Runtime, env.TxId);
}

// Active zone: AlterPQGroup → wait notification (SS→PQ config tx through FakeCoordinator).
void ControlPlaneAlterScenario(TProdPqEnv& env, bool& activeZone) {
    activeZone = false;
    UNIT_ASSERT_VALUES_EQUAL(GetTopicLifetimeSeconds(*env.Runtime), kCreateLifetimeSeconds);

    Cerr << "====== PROD_PQ alter enter activeZone ======" << Endl;
    activeZone = true;
    AlterTopicLifetime(env, kAlterLifetimeSeconds);
    activeZone = false;

    UNIT_ASSERT_VALUES_EQUAL(GetTopicLifetimeSeconds(*env.Runtime), kAlterLifetimeSeconds);
    TestDescribeResult(
        DescribePath(*env.Runtime, kTopicPath),
        {NLs::Finished, NLs::RetentionPeriod(TDuration::Seconds(kAlterLifetimeSeconds))});
    Cerr << "====== PROD_PQ alter COMPLETE ok ======" << Endl;
}

// Active zone: split partition 0 → wait notification.
void ControlPlaneSplitScenario(TProdPqEnv& env, bool& activeZone) {
    activeZone = false;
    UNIT_ASSERT_VALUES_EQUAL(GetTopicPartitionCount(*env.Runtime), 1u);

    const TString boundary(reinterpret_cast<const char*>(kSplitBoundHalf), sizeof(kSplitBoundHalf));
    Cerr << "====== PROD_PQ split enter activeZone ======" << Endl;
    activeZone = true;
    SplitTopicPartition(env, /*partitionId=*/0, boundary);
    activeZone = false;

    const ui32 partsAfter = GetTopicPartitionCount(*env.Runtime);
    UNIT_ASSERT_C(partsAfter >= 3, "expected parent+2 children, got=" << partsAfter);
    Cerr << "====== PROD_PQ split COMPLETE ok partitions=" << partsAfter << " ======" << Endl;
}

// DROP mid distributed offset-commit (after PlanStep → WAIT_RS / commit path).
// Success: topic gone, PQ tablets deleted, no hang.
void ControlPlaneDropMidCommitScenario(TProdPqEnv& env, bool& activeZone) {
    activeZone = false;
    DrainStaleProposeResults(*env.Runtime, env.Edge);

    const ui64 commitTxId = 9600;
    const auto tablets = env.PqTabletIds;
    TVector<ui64> tabletIds(tablets.begin(), tablets.end());

    Cerr << "====== PROD_PQ drop-mid prepare propose txId=" << commitTxId << " ======" << Endl;
    for (const auto& part : env.Parts) {
        ProposeOffsetCommit(
            *env.Runtime, env.Edge, commitTxId, part, tablets,
            /*begin=*/0, /*end=*/kSeedMsgCount);
    }
    WaitPreparedFromTablets(*env.Runtime, env.Edge, commitTxId, tablets.size());

    Cerr << "====== PROD_PQ drop-mid enter activeZone (plan+drop) ======" << Endl;
    activeZone = true;
    PlanViaFakeCoordinator(*env.Runtime, env.Edge, commitTxId, tablets);
    // Best-effort: allow plan/RS to start before DROP kills the path.
    try {
        env.Runtime->SimulateSleep(TDuration::MilliSeconds(10));
    } catch (...) {
    }
    TestDropPQGroup(*env.Runtime, ++env.TxId, "/MyRoot/DirA", kTopicName);
    env.Env->TestWaitNotification(*env.Runtime, env.TxId);
    activeZone = false;

    env.Env->TestWaitTabletDeletion(*env.Runtime, tabletIds);
    TestDescribeResult(DescribePath(*env.Runtime, kTopicPath), {NLs::PathNotExist});
    Cerr << "====== PROD_PQ drop-mid COMPLETE ok ======" << Endl;
}

void RunControlPlaneInjection(
    std::function<void(
        const TVector<ui64>&,
        std::function<TTestActorRuntime::TEventFilter()>,
        std::function<void(const TString&, std::function<void(TTestActorRuntime&)>, bool&)>)> runner,
    std::function<void(TProdPqEnv&, bool&)> scenario,
    ui32 partitions,
    bool seedMessages,
    bool canSplitAndMerge = false,
    const std::unordered_set<TString>& extraSkipEventTypes = {})
{
    TInitialEventsFilter filter;
    runner(
        ControlPlaneRebootTablets(partitions),
        [&]() {
            return filter.Prepare(
                {TabletPipe, NPDisk, KeyValue, PQ},
                extraSkipEventTypes);
        },
        [&](const TString& dispatchName,
            std::function<void(TTestActorRuntime&)> setup,
            bool& activeZone) {
            TProdPqEnv env;
            activeZone = false;
            env.Prepare(
                dispatchName, setup, activeZone, seedMessages, partitions, canSplitAndMerge);
            scenario(env, activeZone);
        });
}

} // namespace

Y_UNIT_TEST_SUITE(TProdPqInjectionTests) {

// Baseline without injection matrix: create → propose → FakeCoordinator plan → COMPLETE.
Y_UNIT_TEST(SmokeDistributedOffsetCommitViaFakeCoordinator) {
    TTestBasicRuntime runtime;
    runtime.SetScheduledLimit(50'000);
    TTestEnv env(runtime, MakeEnvOptions());
    env.SetupLogging(runtime);
    ui64 txId = 1000;
    TestMkDir(runtime, ++txId, "/MyRoot", "DirA");

    TestCreatePQGroup(
        runtime, ++txId, "/MyRoot/DirA",
        TopicCreateScheme(/*partitions=*/kPqTabletCount, /*perTablet=*/1));
    env.TestWaitNotification(runtime, txId);
    auto parts = DescribePartitions(runtime);
    UNIT_ASSERT_VALUES_EQUAL(UniqueTabletIds(parts).size(), kPqTabletCount);
    SeedAllPartitions(runtime, parts);

    const TActorId edge = runtime.AllocateEdgeActor();
    const auto tablets = UniqueTabletIds(parts);
    const ui64 commitTxId = 9400;
    for (const auto& part : parts) {
        ProposeOffsetCommit(runtime, edge, commitTxId, part, tablets, 0, kSeedMsgCount);
    }
    WaitPreparedFromTablets(runtime, edge, commitTxId, tablets.size());
    PlanViaFakeCoordinator(runtime, edge, commitTxId, tablets);
    WaitCompleteFromTablets(runtime, edge, commitTxId, tablets.size());
    AssertAllOffsetsAt(runtime, parts, kSeedMsgCount);
}

// Baseline: supportive write on all partitions → distributed propose/plan → published.
Y_UNIT_TEST(SmokeDistributedTopicWriteViaFakeCoordinator) {
    TTestBasicRuntime runtime;
    runtime.SetScheduledLimit(50'000);
    TTestEnv env(runtime, MakeEnvOptions());
    env.SetupLogging(runtime);
    ui64 txId = 1000;
    TestMkDir(runtime, ++txId, "/MyRoot", "DirA");

    TestCreatePQGroup(
        runtime, ++txId, "/MyRoot/DirA",
        TopicCreateScheme(/*partitions=*/kPqTabletCount, /*perTablet=*/1));
    env.TestWaitNotification(runtime, txId);
    auto parts = DescribePartitions(runtime);
    UNIT_ASSERT_VALUES_EQUAL(UniqueTabletIds(parts).size(), kPqTabletCount);

    const TActorId edge = runtime.AllocateEdgeActor();
    const auto tablets = UniqueTabletIds(parts);
    THashMap<ui64, TPipeSession> pipes;
    for (ui64 tabletId : tablets) {
        pipes[tabletId] = TPipeSession{};
    }

    struct TPreparedWrite {
        TPartitionRef Part;
        TWriteId WriteId;
        ui32 SupportivePartitionId = 0;
    };
    TVector<TPreparedWrite> prepared;
    for (const auto& part : parts) {
        const TWriteId writeId(part.TabletId, 1000);
        auto& pipe = pipes[part.TabletId];
        const TString ownerCookie = CreateSupportivePartition(
            runtime, pipe, edge, part, writeId);
        for (ui32 i = 0; i < kTxWriteMsgCount; ++i) {
            WriteToSupportive(
                runtime, pipe, edge, part, writeId, ownerCookie,
                /*seqNo=*/i, /*messageNo=*/i,
                TStringBuilder() << "prod-tx-write-0-" << part.PartitionId << "-" << i,
                /*cookie=*/500 + i);
        }
        prepared.push_back({
            part, writeId,
            WaitSupportivePartitionId(runtime, pipe, edge, part, writeId)});
    }

    const ui64 writeTxId = 9500;
    for (const auto& item : prepared) {
        ProposeTopicWriteCommit(
            runtime, edge, writeTxId, item.Part, tablets,
            item.WriteId, item.SupportivePartitionId);
    }
    WaitPreparedFromTablets(runtime, edge, writeTxId, tablets.size());
    PlanViaFakeCoordinator(runtime, edge, writeTxId, tablets);
    WaitCompleteFromTablets(runtime, edge, writeTxId, tablets.size());
    AssertTopicWriteCommitted(runtime, parts, /*readFrom=*/0, kTxWriteMsgCount, /*attempt=*/0);
}

// Data plane: PQ + FakeCoordinator under RunTestWithReboots.
// Topic create/seed are outside activeZone; zone starts at ProposeTransaction.
Y_UNIT_TEST(DistributedOffsetCommitWithTabletReboots) {
    RunDataPlaneInjection(
        [](const auto& tabletIds, auto filterFactory, auto testFunc) {
            RunTestWithReboots(tabletIds, filterFactory, testFunc);
        },
        DataPlaneOffsetCommitScenario);
}

// Data plane: same scenario under pipe resets (no ReadSet skip — Hive-owned PQ).
Y_UNIT_TEST(DistributedOffsetCommitWithPipeResets) {
    RunDataPlaneInjection(
        [](const auto& tabletIds, auto filterFactory, auto testFunc) {
            RunTestWithPipeResets(tabletIds, filterFactory, testFunc);
        },
        DataPlaneOffsetCommitScenario);
}

// Topic write-tx: supportive prep outside zone; propose/plan/COMPLETE under reboots.
Y_UNIT_TEST(DistributedTopicWriteWithTabletReboots) {
    RunDataPlaneInjection(
        [](const auto& tabletIds, auto filterFactory, auto testFunc) {
            RunTestWithReboots(tabletIds, filterFactory, testFunc);
        },
        DataPlaneTopicWriteScenario,
        /*seedMessages=*/false);
}

Y_UNIT_TEST(DistributedTopicWriteWithPipeResets) {
    RunDataPlaneInjection(
        [](const auto& tabletIds, auto filterFactory, auto testFunc) {
            RunTestWithPipeResets(tabletIds, filterFactory, testFunc);
        },
        DataPlaneTopicWriteScenario,
        /*seedMessages=*/false);
}

// --- Control plane: SS schema txs under PQ+SchemeShard+Coordinator injection ---

Y_UNIT_TEST(SmokeAlterTopicConfigViaSchemeShard) {
    TTestBasicRuntime runtime;
    runtime.SetScheduledLimit(50'000);
    TTestEnv env(runtime, MakeEnvOptions());
    env.SetupLogging(runtime);
    ui64 txId = 1000;
    TestMkDir(runtime, ++txId, "/MyRoot", "DirA");
    TestCreatePQGroup(
        runtime, ++txId, "/MyRoot/DirA",
        TopicCreateScheme(/*partitions=*/2, /*perTablet=*/1));
    env.TestWaitNotification(runtime, txId);
    UNIT_ASSERT_VALUES_EQUAL(GetTopicLifetimeSeconds(runtime), kCreateLifetimeSeconds);

    TestAlterPQGroup(
        runtime, ++txId, "/MyRoot/DirA",
        TStringBuilder()
            << "Name: \"" << kTopicName << "\" "
            << "PQTabletConfig: {"
            << "  PartitionConfig { LifetimeSeconds: " << kAlterLifetimeSeconds << " }"
            << "  Consumers { Name: \"" << kConsumer << "\" Important: true }"
            << "}");
    env.TestWaitNotification(runtime, txId);
    TestDescribeResult(
        DescribePath(runtime, kTopicPath),
        {NLs::Finished, NLs::RetentionPeriod(TDuration::Seconds(kAlterLifetimeSeconds))});
}

Y_UNIT_TEST(AlterTopicConfigWithTabletReboots) {
    RunControlPlaneInjection(
        [](const auto& tabletIds, auto filterFactory, auto testFunc) {
            RunTestWithReboots(tabletIds, filterFactory, testFunc);
        },
        ControlPlaneAlterScenario,
        /*partitions=*/2,
        /*seedMessages=*/false);
}

Y_UNIT_TEST(AlterTopicConfigWithPipeResets) {
    RunControlPlaneInjection(
        [](const auto& tabletIds, auto filterFactory, auto testFunc) {
            RunTestWithPipeResets(tabletIds, filterFactory, testFunc);
        },
        ControlPlaneAlterScenario,
        /*partitions=*/2,
        /*seedMessages=*/false);
}

Y_UNIT_TEST(SmokeSplitPartitionViaSchemeShard) {
    TTestBasicRuntime runtime;
    runtime.SetScheduledLimit(50'000);
    TTestEnv env(runtime, MakeEnvOptions());
    env.SetupLogging(runtime);
    ui64 txId = 1000;
    TestMkDir(runtime, ++txId, "/MyRoot", "DirA");
    TestCreatePQGroup(
        runtime, ++txId, "/MyRoot/DirA",
        TopicCreateScheme(
            /*partitions=*/1, /*perTablet=*/1, kCreateLifetimeSeconds, /*canSplit=*/true));
    env.TestWaitNotification(runtime, txId);
    UNIT_ASSERT_VALUES_EQUAL(GetTopicPartitionCount(runtime), 1u);

    NKikimrSchemeOp::TPersQueueGroupDescription scheme;
    scheme.SetName(kTopicName);
    scheme.MutablePQTabletConfig()->MutablePartitionConfig();
    auto* split = scheme.AddSplit();
    split->SetPartition(0);
    split->SetSplitBoundary(
        TString(reinterpret_cast<const char*>(kSplitBoundHalf), sizeof(kSplitBoundHalf)));
    TStringBuilder sb;
    sb << scheme;
    TestAlterPQGroup(runtime, ++txId, "/MyRoot/DirA", sb.substr(1, sb.size() - 2));
    env.TestWaitNotification(runtime, txId);

    const ui32 partsAfter = GetTopicPartitionCount(runtime);
    UNIT_ASSERT_C(partsAfter >= 3, "expected parent+2 children, got=" << partsAfter);
}

Y_UNIT_TEST(SplitPartitionWithTabletReboots) {
    RunControlPlaneInjection(
        [](const auto& tabletIds, auto filterFactory, auto testFunc) {
            RunTestWithReboots(tabletIds, filterFactory, testFunc);
        },
        ControlPlaneSplitScenario,
        /*partitions=*/1,
        /*seedMessages=*/false,
        /*canSplitAndMerge=*/true);
}

Y_UNIT_TEST(SplitPartitionWithPipeResets) {
    RunControlPlaneInjection(
        [](const auto& tabletIds, auto filterFactory, auto testFunc) {
            RunTestWithPipeResets(tabletIds, filterFactory, testFunc);
        },
        ControlPlaneSplitScenario,
        /*partitions=*/1,
        /*seedMessages=*/false,
        /*canSplitAndMerge=*/true);
}

Y_UNIT_TEST(SmokeDropTopicMidDistributedCommit) {
    TTestBasicRuntime runtime;
    runtime.SetScheduledLimit(50'000);
    TTestEnv env(runtime, MakeEnvOptions());
    env.SetupLogging(runtime);
    ui64 txId = 1000;
    TestMkDir(runtime, ++txId, "/MyRoot", "DirA");
    TestCreatePQGroup(
        runtime, ++txId, "/MyRoot/DirA",
        TopicCreateScheme(/*partitions=*/kPqTabletCount, /*perTablet=*/1));
    env.TestWaitNotification(runtime, txId);
    auto parts = DescribePartitions(runtime);
    SeedAllPartitions(runtime, parts);
    const auto tablets = UniqueTabletIds(parts);
    TVector<ui64> tabletIds(tablets.begin(), tablets.end());

    const TActorId edge = runtime.AllocateEdgeActor();
    const ui64 commitTxId = 9600;
    for (const auto& part : parts) {
        ProposeOffsetCommit(runtime, edge, commitTxId, part, tablets, 0, kSeedMsgCount);
    }
    WaitPreparedFromTablets(runtime, edge, commitTxId, tablets.size());
    PlanViaFakeCoordinator(runtime, edge, commitTxId, tablets);
    try {
        runtime.SimulateSleep(TDuration::MilliSeconds(10));
    } catch (...) {
    }
    TestDropPQGroup(runtime, ++txId, "/MyRoot/DirA", kTopicName);
    env.TestWaitNotification(runtime, txId);
    env.TestWaitTabletDeletion(runtime, tabletIds);
    TestDescribeResult(DescribePath(runtime, kTopicPath), {NLs::PathNotExist});
}

// DROP mid commit under PQ reboots only (Coordinator reboot mid DROP livelocks the event loop).
// Not under pipe-reset matrix — peer removal / Dead path, not transient pipe breaks.
Y_UNIT_TEST(DropTopicMidDistributedCommitWithTabletReboots) {
    TInitialEventsFilter filter;
    TVector<ui64> pqOnly;
    for (ui32 i = 0; i < kPqTabletCount; ++i) {
        pqOnly.push_back(TTestTxConfig::FakeHiveTablets + i);
    }
    RunTestWithReboots(
        pqOnly,
        [&]() {
            return filter.Prepare({TabletPipe, NPDisk, KeyValue, PQ});
        },
        [&](const TString& dispatchName,
            std::function<void(TTestActorRuntime&)> setup,
            bool& activeZone) {
            TProdPqEnv env;
            activeZone = false;
            env.Prepare(
                dispatchName, setup, activeZone,
                /*seedMessages=*/true, kPqTabletCount);
            ControlPlaneDropMidCommitScenario(env, activeZone);
        });
}

} // Y_UNIT_TEST_SUITE(TProdPqInjectionTests)

} // namespace NKikimr::NPQ
