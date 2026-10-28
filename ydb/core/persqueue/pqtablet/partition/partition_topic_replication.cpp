#include "partition.h"

#include <ydb/core/persqueue/public/utils.h>

#include <google/protobuf/util/message_differencer.h>

#define YDB_LOG_THIS_FILE_COMPONENT Service

namespace NKikimr::NPQ {
namespace {

bool SameOffsets(const NKikimrPQ::TEvPartitionConsumerOffsets& left,
    const NKikimrPQ::TEvPartitionConsumerOffsets& right)
{
    google::protobuf::util::MessageDifferencer differencer;
    const auto* consumers = left.GetDescriptor()->FindFieldByNumber(
        NKikimrPQ::TEvPartitionConsumerOffsets::kConsumerOffsetsFieldNumber);
    differencer.TreatAsMap(consumers, consumers->message_type()->FindFieldByNumber(
        NKikimrPQ::TEvPartitionConsumerOffsets::TConsumerOffset::kConsumerFieldNumber));
    return differencer.Compare(left, right);
}

void ReplyOffsetsPersisted(const TActorContext& ctx, const TActorId& recipient, ui64 cookie, ui32 partition) {
    auto ack = MakeHolder<TEvPQ::TEvPartitionConsumerOffsetsAck>();
    ack->Record.SetPartitionId(partition);
    ctx.Send(recipient, ack.Release(), 0, cookie);
}

} // namespace

void TPartition::Handle(TEvPQ::TEvPartitionConsumerOffsets::TPtr& ev, const TActorContext& ctx) {
    const auto& offsets = ev->Get()->Record;
    if (IsSupportive() || !offsets.HasPartitionId() || offsets.GetPartitionId() != Partition.OriginalPartitionId
        || (offsets.HasStartOffset() && offsets.HasEndOffset() && offsets.GetStartOffset() > offsets.GetEndOffset()))
    {
        LOG_W("Received replication offsets for an unexpected partition");
        return;
    }

    if (!PendingReplicationConsumerOffsets.empty() && SameOffsets(PendingReplicationConsumerOffsets.back().Offsets, offsets)) {
        PendingReplicationConsumerOffsets.back().Replies.emplace_back(ev->Sender, ev->Cookie);
    } else if (PendingReplicationConsumerOffsets.empty() && ReplicationConsumerOffsetsWrite
        && SameOffsets(ReplicationConsumerOffsetsWrite->Offsets, offsets))
    {
        ReplicationConsumerOffsetsWrite->Replies.emplace_back(ev->Sender, ev->Cookie);
    } else if (InitDone && PendingReplicationConsumerOffsets.empty() && !ReplicationConsumerOffsetsWrite
        && ReplicationConsumerOffsets && SameOffsets(*ReplicationConsumerOffsets, offsets))
    {
        ReplyOffsetsPersisted(ctx, ev->Sender, ev->Cookie, offsets.GetPartitionId());
        return;
    } else {
        auto& pending = PendingReplicationConsumerOffsets.emplace_back();
        pending.Offsets = offsets;
        pending.Replies.emplace_back(ev->Sender, ev->Cookie);
    }

    if (InitDone) {
        ProcessTxsAndUserActs(ctx);
    }
}

bool TPartition::AddReplicationConsumerOffsetsToPersistRequest(const TActorContext& ctx) {
    if (ReplicationConsumerOffsetsWrite || ChangingConfig || ChangeConfig) {
        return false;
    }
    while (!PendingReplicationConsumerOffsets.empty()) {
        auto& front = PendingReplicationConsumerOffsets.front();
        if (ReplicationConsumerOffsets && SameOffsets(*ReplicationConsumerOffsets, front.Offsets)) {
            auto pending = std::move(front);
            PendingReplicationConsumerOffsets.pop_front();
            for (const auto& [recipient, cookie] : pending.Replies) {
                ReplyOffsetsPersisted(ctx, recipient, cookie, pending.Offsets.GetPartitionId());
            }
            continue;
        }

        // Validate every consumer before changing any pending user state.
        THashSet<TString> consumers;
        for (const auto& offset : front.Offsets.GetConsumerOffsets()) {
            const auto& consumer = offset.GetConsumer();
            if (!offset.HasConsumer() || !offset.HasCommittedOffset() || !consumers.insert(consumer).second
                || offset.GetCommittedOffset() > ui64(Max<i64>())
                || (front.Offsets.HasEndOffset() && offset.GetCommittedOffset() > front.Offsets.GetEndOffset()))
            {
                LOG_W("Invalid replication consumer offset", {"consumer", consumer});
                PendingReplicationConsumerOffsets.pop_front();
                return false;
            }
            if (!GetConsumer(Config, consumer) || offset.GetCommittedOffset() > GetEndOffset()) {
                LOG_W("Cannot apply replication offset before consumer and partition data are ready", {"consumer", consumer});
                PendingReplicationConsumerOffsets.pop_front();
                return false;
            }
            if (TxAffectedConsumers.contains(consumer) || AffectedUsers.contains(consumer)) {
                return false;
            }
        }
        for (const auto& offset : front.Offsets.GetConsumerOffsets()) {
            auto& user = GetOrCreatePendingUser(offset.GetConsumer());
            user.Offset = offset.GetCommittedOffset();
            user.AnyCommits = user.Offset > i64(GetStartOffset());
            user.Session.clear();
            user.PartitionSessionId = 0;
            user.PipeClient = {};
            user.Generation = user.Step = 0;
            user.CommittedMetadata.reset();
        }
        ReplicationConsumerOffsetsWrite.emplace(std::move(front));
        PendingReplicationConsumerOffsets.pop_front();
        const TKeyPrefix key(TKeyPrefix::TypeInfo, Partition, TKeyPrefix::MarkTopicReplication);
        auto* write = PersistRequest->Record.AddCmdWrite();
        write->SetKey(key.Data(), key.Size());
        write->SetValue(ReplicationConsumerOffsetsWrite->Offsets.SerializeAsString());
        write->SetStorageChannel(NKikimrClient::TKeyValueRequest::INLINE);
        return true;
    }
    return false;
}

void TPartition::CompleteReplicationConsumerOffsetsWrite(const TActorContext& ctx) {
    if (!ReplicationConsumerOffsetsWrite) {
        return;
    }
    ReplicationConsumerOffsets.emplace(std::move(ReplicationConsumerOffsetsWrite->Offsets));
    for (const auto& offset : ReplicationConsumerOffsets->GetConsumerOffsets()) {
        if (auto* user = UsersInfoStorage->GetIfExists(offset.GetConsumer())) {
            user->AnyCommits = user->Offset > i64(GetStartOffset());
            user->PartitionSessionId = 0;
            user->PipeClient = {};
            FailStaleSessionReadRequests(offset.GetConsumer(), ctx);
        }
    }
    for (const auto& [recipient, cookie] : ReplicationConsumerOffsetsWrite->Replies) {
        ReplyOffsetsPersisted(ctx, recipient, cookie, ReplicationConsumerOffsets->GetPartitionId());
    }
    ReplicationConsumerOffsetsWrite.reset();
    LOG_I("Source consumer offsets persisted", {"consumers", ReplicationConsumerOffsets->ConsumerOffsetsSize()});
}

} // namespace NKikimr::NPQ
