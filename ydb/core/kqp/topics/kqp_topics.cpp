#include "kqp_topics.h"

#include <ydb/core/base/path.h>
#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/persqueue/utils.h>
#include <ydb/core/kafka_proxy/kafka_producer_instance_id.h>
#include <ydb/library/actors/core/log.h>

#include <util/generic/set.h>

#define LOG_D(msg) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_SESSION, msg)

namespace NKikimr::NKqp::NTopic {

static void UpdateSupportivePartition(TMaybe<ui32>& lhs, const TMaybe<ui32>& rhs)
{
    if (lhs) {
        if ((rhs != Nothing()) && (rhs != lhs)) {
            // we set this to make sure PQ Tablet will not find relevant partition and will abort the transaction
            lhs = Max<ui32>();
        }
    } else {
        lhs = rhs;
    }
}

static void UpdateKafkaProducerInstanceId(TMaybe<NKafka::TProducerInstanceId>& lhs, const TMaybe<NKafka::TProducerInstanceId>& rhs)
{
    if (lhs) {
        if ((rhs != Nothing()) && (rhs != lhs)) {
            // we set this to make sure PQ Tablet will not find relevant Kafka producer instance and will abort the transaction and log correct error
            lhs = NKafka::INVALID_PRODUCER_INSTANCE_ID;
        }
    } else {
        lhs = rhs;
    }
}

//
// TConsumerOperations
//
bool TConsumerOperations::IsValid() const
{
    return Offsets_.GetNumIntervals() <= 1;
}

std::pair<ui64, ui64> TConsumerOperations::GetOffsetsCommitRange() const
{
    Y_ENSURE(IsValid());

    if (Offsets_.Empty()) {
        return {0,0};
    } else {
        return {Offsets_.Min(), Offsets_.Max()};
    }
}

bool TConsumerOperations::GetForceCommit() const
{
    return ForceCommit_;
}

bool TConsumerOperations::GetKillReadSession() const
{
    return KillReadSession_;
}

bool TConsumerOperations::GetOnlyCheckCommitedToFinish() const
{
    return OnlyCheckCommitedToFinish_;
}

TString TConsumerOperations::GetReadSessionId() const
{
    return ReadSessionId_;
}

ui64 TConsumerOperations::GetKafkaCommitOffset() const
{
    Y_ENSURE(KafkaCommitOffset_.Defined());

    return *KafkaCommitOffset_;
}

void TConsumerOperations::AddOperation(const TString& consumer,
                                       const NKikimrKqp::TTopicOperationsRequest_TopicOffsets_PartitionOffsets_OffsetsRange& range,
                                       bool forceCommit,
                                       bool killReadSession,
                                       bool onlyCheckCommitedToFinish,
                                       const TString& readSessionId)
{
    Y_ENSURE(Consumer_.Empty() || Consumer_ == consumer);

    AddOperationImpl(consumer, range.start(), range.end(), forceCommit, killReadSession, onlyCheckCommitedToFinish, readSessionId);
}

void TConsumerOperations::Merge(const TConsumerOperations& rhs)
{
    Y_ENSURE(rhs.Consumer_.Defined());
    Y_ENSURE(Consumer_.Empty() || Consumer_ == rhs.Consumer_);

    if (rhs.IsKafkaApiOperation()) {
        KafkaCommitOffset_ = rhs.KafkaCommitOffset_;
        return;
    }

    if (!rhs.Offsets_.Empty()) {
        for (auto& range : rhs.Offsets_) {
            AddOperationImpl(*rhs.Consumer_, range.first, range.second, rhs.GetForceCommit(), rhs.GetKillReadSession(), rhs.GetOnlyCheckCommitedToFinish(), rhs.GetReadSessionId());
        }
    } else {
        AddOperationImpl(*rhs.Consumer_, 0, 0, rhs.GetForceCommit(), rhs.GetKillReadSession(), rhs.GetOnlyCheckCommitedToFinish(), rhs.GetReadSessionId());
    }
}

void TConsumerOperations::AddOperationImpl(const TString& consumer,
                                           ui64 begin,
                                           ui64 end,
                                           bool forceCommit,
                                           bool killReadSession,
                                           bool onlyCheckCommitedToFinish,
                                           const TString& readSessionId)
{
    if (Offsets_.Intersects(begin, end)) {
        ythrow TOffsetsRangeIntersectExpection() << "offset ranges intersect";
    }

    if (Consumer_.Empty()) {
        Consumer_ = consumer;
    }

    if (end != 0) {
        Offsets_.InsertInterval(begin, end);
    }

    ForceCommit_ = forceCommit;
    KillReadSession_ = killReadSession;
    OnlyCheckCommitedToFinish_ = onlyCheckCommitedToFinish;
    ReadSessionId_ = readSessionId;
}

void TConsumerOperations::AddKafkaApiOffsetCommit(const TString& consumer, ui64 offset) {
    if (Consumer_.Empty()) {
        Consumer_ = consumer;
    }

    KafkaCommitOffset_ = offset;
}

bool TConsumerOperations::IsKafkaApiOperation() const 
{
    return KafkaCommitOffset_.Defined();
}

//
// TTopicPartitionOperations
//
bool TTopicPartitionOperations::IsValid() const
{
    return std::all_of(Operations_.begin(), Operations_.end(),
                       [](auto& x) { return x.second.IsValid(); });
}

void TTopicPartitionOperations::AddOperation(const TString& topic,
                                             ui32 partition,
                                             const TString& consumer,
                                             const NKikimrKqp::TTopicOperationsRequest_TopicOffsets_PartitionOffsets_OffsetsRange& range,
                                             bool forceCommit,
                                             bool killReadSession,
                                             bool onlyCheckCommitedToFinish,
                                             const TString& readSessionId)
{
    Y_ENSURE(Topic_.Empty() || Topic_ == topic);
    Y_ENSURE(Partition_.Empty() || Partition_ == partition);

    if (Topic_.Empty()) {
        Topic_ = topic;
        Partition_ = partition;
    }

    Operations_[consumer].AddOperation(consumer, range, forceCommit, killReadSession, onlyCheckCommitedToFinish, readSessionId);
}

void TTopicPartitionOperations::AddOperation(const TString& topic, ui32 partition,
                                             TMaybe<ui32> supportivePartition)
{
    Y_ENSURE(Topic_.Empty() || Topic_ == topic);
    Y_ENSURE(Partition_.Empty() || Partition_ == partition);

    if (Topic_.Empty()) {
        Topic_ = topic;
        Partition_ = partition;
    }

    UpdateSupportivePartition(SupportivePartition_, supportivePartition);

    HasWriteOperations_ = true;
}

void TTopicPartitionOperations::AddKafkaApiWriteOperation(const TString& topic, ui32 partition, const NKafka::TProducerInstanceId& producerInstanceId) {
    Y_ENSURE(Topic_.Empty() || Topic_ == topic);
    Y_ENSURE(Partition_.Empty() || Partition_ == partition);

    if (Topic_.Empty()) {
        Topic_ = topic;
        Partition_ = partition;
    }

    UpdateKafkaProducerInstanceId(KafkaProducerInstanceId_, producerInstanceId);

    HasWriteOperations_ = true;
}

void TTopicPartitionOperations::AddKafkaApiReadOperation(const TString& topic, ui32 partition, const TString& consumerName, ui64 offset) {
    Y_ENSURE(Topic_.Empty() || Topic_ == topic);
    Y_ENSURE(Partition_.Empty() || Partition_ == partition);

    if (Topic_.Empty()) {
        Topic_ = topic;
        Partition_ = partition;
    }

    Operations_[consumerName].AddKafkaApiOffsetCommit(consumerName, offset);
}

void TTopicPartitionOperations::BuildTopicTxs(TTopicOperationTransactions& txs)
{
    Y_ENSURE(TabletId_.Defined());
    Y_ENSURE(Partition_.Defined());

    auto& t = txs[*TabletId_];

    for (auto& [consumer, operations] : Operations_) {
        NKikimrPQ::TPartitionOperation* o = t.tx.MutableOperations()->Add();
        o->SetPath(*Topic_);
        o->SetPartitionId(*Partition_);
        o->SetConsumer(consumer);
        if (operations.IsKafkaApiOperation()) {
            o->SetCommitOffsetsEnd(operations.GetKafkaCommitOffset());
            o->SetKafkaTransaction(true);
        } else {
            auto [begin, end] = operations.GetOffsetsCommitRange();
            o->SetCommitOffsetsBegin(begin);
            o->SetCommitOffsetsEnd(end);
            o->SetKillReadSession(operations.GetKillReadSession());
            o->SetForceCommit(operations.GetForceCommit());
            o->SetOnlyCheckCommitedToFinish(operations.GetOnlyCheckCommitedToFinish());
            o->SetReadSessionId(operations.GetReadSessionId());
        }
    }

    if (HasWriteOperations_) {
        NKikimrPQ::TPartitionOperation* o = t.tx.MutableOperations()->Add();
        o->SetPartitionId(*Partition_);
        o->SetPath(*Topic_);

        if (KafkaProducerInstanceId_.Defined()) { // kafka transaction
            o->SetKafkaTransaction(true);
            o->MutableKafkaProducerInstanceId()->SetId(KafkaProducerInstanceId_->Id);
            o->MutableKafkaProducerInstanceId()->SetEpoch(KafkaProducerInstanceId_->Epoch);
        } else if (SupportivePartition_.Defined()) {
            o->SetSupportivePartition(*SupportivePartition_);
        }
        t.hasWrite = true;
    }
}

void TTopicPartitionOperations::Merge(const TTopicPartitionOperations& rhs)
{
    Y_ENSURE(Topic_.Empty() || Topic_ == rhs.Topic_);
    Y_ENSURE(Partition_.Empty() || Partition_ == rhs.Partition_);

    if (Topic_.Empty()) {
        Topic_ = rhs.Topic_;
        Partition_ = rhs.Partition_;
    }
    if (TabletId_.Empty()) {
        TabletId_ = rhs.TabletId_;
    }

    UpdateSupportivePartition(SupportivePartition_, rhs.SupportivePartition_);
    UpdateKafkaProducerInstanceId(KafkaProducerInstanceId_, rhs.KafkaProducerInstanceId_);

    for (auto& [key, value] : rhs.Operations_) {
        Operations_[key].Merge(value);
    }

    HasWriteOperations_ |= rhs.HasWriteOperations_;
}

ui64 TTopicPartitionOperations::GetTabletId() const
{
    Y_ENSURE(TabletId_.Defined());

    return *TabletId_;
}

bool TTopicPartitionOperations::HasTabletId() const
{
    return TabletId_.Defined();
}

void TTopicPartitionOperations::SetTabletId(ui64 value)
{
    Y_ENSURE(TabletId_.Empty());

    TabletId_ = value;
}

TMaybe<TString> TTopicPartitionOperations::GetTopicName() const
{
    return Topic_;
}

bool TTopicPartitionOperations::HasReadOperations() const
{
    return !Operations_.empty();
}

bool TTopicPartitionOperations::HasWriteOperations() const
{
    return HasWriteOperations_;
}

//
// TTopicPartition
//
TTopicPartition::TTopicPartition(TString topic, ui32 partition) :
    Topic_{std::move(topic)},
    Partition_{partition}
{
}

bool TTopicPartition::operator==(const TTopicPartition& x) const
{
    return (Topic_ == x.Topic_) && (Partition_ == x.Partition_);
}

size_t TTopicPartition::THash::operator()(const TTopicPartition& x) const
{
    size_t hash = std::hash<TString>{}(x.Topic_);
    hash = CombineHashes(hash, std::hash<ui32>{}(x.Partition_));
    return hash;
}

//
// TTopicOperations
//
bool TTopicOperations::IsValid() const
{
    return std::all_of(Operations_.begin(), Operations_.end(),
                       [](auto& x) { return x.second.IsValid(); });
}

bool TTopicOperations::HasOperations() const
{
    return HasReadOperations() || HasWriteOperations();
}

bool TTopicOperations::HasReadOperations() const
{
    return HasReadOperations_;
}

bool TTopicOperations::HasWriteOperations() const
{
    return HasWriteOperations_;
}

bool TTopicOperations::HasKafkaOperations() const
{
    return HasKafkaOperations_;
}

bool TTopicOperations::HasWriteId() const
{
    return WriteId_.GetLockId();
}

ui64 TTopicOperations::GetWriteId() const
{
    return WriteId_.GetLockId();
}

void TTopicOperations::SetWriteId(NLongTxService::TLockHandle handle)
{
    WriteId_ = std::move(handle);
}

NKafka::TProducerInstanceId TTopicOperations::GetKafkaProducerInstanceId() const
{
    Y_ENSURE(HasKafkaOperations_);
    Y_ENSURE(KafkaProducerInstanceId_.Defined());

    return *KafkaProducerInstanceId_;
}

bool TTopicOperations::TabletHasReadOperations(ui64 tabletId) const
{
    for (auto& [_, value] : Operations_) {
        if (value.GetTabletId() == tabletId) {
            // reading from a topic and writing to a topic contain read operations
            return value.HasReadOperations() || value.HasWriteOperations();
        }
    }
    return false;
}

void TTopicOperations::AddOperation(const TString& topic,
                                    ui32 partition,
                                    const TString& consumer,
                                    const NKikimrKqp::TTopicOperationsRequest_TopicOffsets_PartitionOffsets_OffsetsRange& range,
                                    bool forceCommit,
                                    bool killReadSession,
                                    bool onlyCheckCommitedToFinish,
                                    const TString& readSessionId
                                    )
{
    TTopicPartition key{topic, partition};
    Operations_[key].AddOperation(topic,
                                  partition,
                                  consumer,
                                  range,
                                  forceCommit,
                                  killReadSession,
                                  onlyCheckCommitedToFinish,
                                  readSessionId);
    HasReadOperations_ = true;
}

void TTopicOperations::AddOperation(const TString& topic, ui32 partition,
                                    TMaybe<ui32> supportivePartition)
{
    TTopicPartition key{topic, partition};
    Operations_[key].AddOperation(topic, partition, supportivePartition);
    HasWriteOperations_ = true;
}

void TTopicOperations::AddKafkaApiWriteOperation(const TString& topic, ui32 partition, const NKafka::TProducerInstanceId& producerInstanceId)
{
    Y_ENSURE(!KafkaProducerInstanceId_ || *KafkaProducerInstanceId_ == producerInstanceId);

    if (KafkaProducerInstanceId_.Empty()) {
        KafkaProducerInstanceId_ = producerInstanceId;
    }

    TTopicPartition key{topic, partition};
    Operations_[key].AddKafkaApiWriteOperation(topic, partition, producerInstanceId);
    HasWriteOperations_ = true;
    HasKafkaOperations_ = true;
}

void TTopicOperations::AddKafkaApiReadOperation(const TString& topic, ui32 partition, const TString& consumerName, ui64 offset)
{
    TTopicPartition key{topic, partition};
    Operations_[key].AddKafkaApiReadOperation(topic, partition, consumerName, offset);
    HasReadOperations_ = true;
    HasKafkaOperations_ = true;
}

void TTopicOperations::FillSchemeCacheNavigate(NSchemeCache::TSchemeCacheNavigate& navigate,
                                               TMaybe<TString> consumer)
{
    TSet<TString> topics;
    for (auto& [key, _] : Operations_) {
        topics.insert(key.Topic_);
    }

    for (auto& topic : topics) {
        NSchemeCache::TSchemeCacheNavigate::TEntry entry;
        entry.Path = NKikimr::SplitPath(topic);
        entry.SyncVersion = true;
        entry.ShowPrivatePath = true;
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpList;

        navigate.ResultSet.push_back(std::move(entry));
    }

    Consumer_ = std::move(consumer);
}

bool TTopicOperations::ProcessSchemeCacheNavigate(const NSchemeCache::TSchemeCacheNavigate::TResultSet& results,
                                                  Ydb::StatusIds_StatusCode& status,
                                                  TString& message)
{
    if (results.empty()) {
        status = Ydb::StatusIds::BAD_REQUEST;
        message = "Request is empty";
        return false;
    }

    TStringBuilder builder;

    for (auto& result : results) {
        if (result.Kind != NSchemeCache::TSchemeCacheNavigate::KindTopic) {
            builder << "Path '" << JoinPath(result.Path) << "' is not a topic";

            status = Ydb::StatusIds::SCHEME_ERROR;
            message = std::move(builder);

            return false;
        }

        if (result.PQGroupInfo) {
            const NKikimrSchemeOp::TPersQueueGroupDescription& description =
                result.PQGroupInfo->Description;

            if (Consumer_) {
                if (!NPQ::HasConsumer(description.GetPQTabletConfig(), *Consumer_)) {
                    builder << "Unknown consumer '" << *Consumer_ << "'";

                    status = Ydb::StatusIds::BAD_REQUEST;
                    message = std::move(builder);

                    return false;
                }
            }

            TString path = CanonizePath(result.Path);

            for (auto& partition : description.GetPartitions()) {
                TTopicPartition key{path, partition.GetPartitionId()};

                if (auto p = Operations_.find(key); p != Operations_.end()) {
                    LOG_D(TStringBuilder() << "(topic, partition, tablet): "
                          << "'" << key.Topic_ << "'"
                          << ", " << partition.GetPartitionId()
                          << ", " << partition.GetTabletId());

                    p->second.SetTabletId(partition.GetTabletId());
                }
            }
        } else {
            builder << "Topic '" << JoinPath(result.Path) << "' is missing";

            status = Ydb::StatusIds::SCHEME_ERROR;
            message = std::move(builder);

            return false;
        }
    }

    for (const auto& [key, operations] : Operations_) {
        if (!operations.HasTabletId()) {
            builder << "Topic '" << key.Topic_ << "'. Unknown partition " << key.Partition_;

            status = Ydb::StatusIds::SCHEME_ERROR;
            message = std::move(builder);

            return false;
        }
    }

    status = Ydb::StatusIds::SUCCESS;
    message = "";

    return true;
}

bool TTopicOperations::HasThisPartitionAlreadyBeenAdded(const TString& topicPath, ui32 partitionId)
{
    if (Operations_.contains({topicPath, partitionId})) {
        return true;
    }
    if (!CachedNavigateResult_.contains(topicPath)) {
        return false;
    }

    const NSchemeCache::TSchemeCacheNavigate::TEntry& entry =
        CachedNavigateResult_.at(topicPath);
    const NKikimrSchemeOp::TPersQueueGroupDescription& description =
        entry.PQGroupInfo->Description;

    TString path = CanonizePath(entry.Path);
    Y_ABORT_UNLESS(path == topicPath,
                   "path=%s, topicPath=%s",
                   path.data(), topicPath.data());

    for (const auto& partition : description.GetPartitions()) {
        if (partition.GetPartitionId() == partitionId) {
            TTopicPartition key{topicPath, partitionId};
            Operations_[key].SetTabletId(partition.GetTabletId());
            return true;
        }
    }

    return false;
}

void TTopicOperations::CacheSchemeCacheNavigate(const NSchemeCache::TSchemeCacheNavigate::TResultSet& results)
{
    for (const auto& result : results) {
        if (result.Kind != NSchemeCache::TSchemeCacheNavigate::KindTopic) {
            continue;
        }
        if (!result.PQGroupInfo) {
            continue;
        }
        TString path = CanonizePath(result.Path);
        CachedNavigateResult_[path] = result;
    }
}

void TTopicOperations::BuildTopicTxs(TTopicOperationTransactions& txs)
{
    for (auto& [_, operations] : Operations_) {
        operations.BuildTopicTxs(txs);
    }
}

void TTopicOperations::Merge(const TTopicOperations& rhs)
{
    for (auto& [key, value] : rhs.Operations_) {
        Operations_[key].Merge(value);
    }

    UpdateKafkaProducerInstanceId(KafkaProducerInstanceId_, rhs.KafkaProducerInstanceId_);
    HasReadOperations_ |= rhs.HasReadOperations_;
    HasWriteOperations_ |= rhs.HasWriteOperations_;
    HasKafkaOperations_ |= rhs.HasKafkaOperations_;
}

TSet<ui64> TTopicOperations::GetReceivingTabletIds() const
{
    TSet<ui64> ids;
    for (auto& [_, operations] : Operations_) {
        ids.insert(operations.GetTabletId());
    }
    return ids;
}

TSet<ui64> TTopicOperations::GetSendingTabletIds() const
{
    TSet<ui64> ids;
    for (auto& [_, operations] : Operations_) {
        ids.insert(operations.GetTabletId());
    }
    return ids;
}

TMaybe<TString> TTopicOperations::GetTabletName(ui64 tabletId) const {
    TMaybe<TString> topic;
    for (auto& [_, operations] : Operations_) {
        if (operations.GetTabletId() == tabletId) {
            topic = operations.GetTopicName();
            break;
        }
    }
    return topic;
}

size_t TTopicOperations::GetSize() const
{
    return Operations_.size();
}

}
