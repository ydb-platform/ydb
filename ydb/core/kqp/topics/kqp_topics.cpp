#include "kqp_topics.h"

#include <ydb/core/base/path.h>

#define LOG_D(msg) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_SESSION, msg)

namespace NKikimr::NKqp::NTopic {

//
// TConsumerOperations
//
bool TConsumerOperations::IsValid() const
{
    return Offsets_.GetNumIntervals() == 1;
}

std::pair<ui64, ui64> TConsumerOperations::GetRange() const
{
    Y_VERIFY(IsValid());

    return {Offsets_.Min(), Offsets_.Max()};
}

void TConsumerOperations::AddOperation(const TString& consumer, const Ydb::Topic::OffsetsRange& range)
{
    Y_VERIFY(Consumer_.Empty() || Consumer_ == consumer);

    AddOperationImpl(consumer, range.start(), range.end());
}

void TConsumerOperations::Merge(const TConsumerOperations& rhs)
{
    Y_VERIFY(rhs.Consumer_.Defined());
    Y_VERIFY(Consumer_.Empty() || Consumer_ == rhs.Consumer_);

    for (auto& range : rhs.Offsets_) {
        AddOperationImpl(*rhs.Consumer_, range.first, range.second);
    }
}

void TConsumerOperations::AddOperationImpl(const TString& consumer,
                                           ui64 begin, ui64 end)
{
    if (Offsets_.Intersects(begin, end)) {
        ythrow TOffsetsRangeIntersectExpection() << "offset ranges intersect";
    }

    if (Consumer_.Empty()) {
        Consumer_ = consumer;
    }

    Offsets_.InsertInterval(begin, end);
}

//
// TTopicPartitionOperations
//
bool TTopicPartitionOperations::IsValid() const
{
    return std::all_of(Operations_.begin(), Operations_.end(),
                       [](auto& x) { return x.second.IsValid(); });
}

void TTopicPartitionOperations::AddOperation(const TString& topic, ui32 partition,
                                             const TString& consumer,
                                             const Ydb::Topic::OffsetsRange& range)
{
    Y_VERIFY(Topic_.Empty() || Topic_ == topic);
    Y_VERIFY(Partition_.Empty() || Partition_ == partition);

    if (Topic_.Empty()) {
        Topic_ = topic;
        Partition_ = partition;
    }

    Operations_[consumer].AddOperation(consumer, range);
}

void TTopicPartitionOperations::AddOperation(const TString& topic, ui32 partition)
{
    Y_VERIFY(Topic_.Empty() || Topic_ == topic);
    Y_VERIFY(Partition_.Empty() || Partition_ == partition);

    if (Topic_.Empty()) {
        Topic_ = topic;
        Partition_ = partition;
    }

    HasWriteOperations_ = true;
}

void TTopicPartitionOperations::BuildTopicTxs(THashMap<ui64, NKikimrPQ::TDataTransaction> &txs)
{
    Y_VERIFY(TabletId_.Defined());
    Y_VERIFY(Partition_.Defined());

    auto& tx = txs[*TabletId_];

    for (auto& [consumer, operations] : Operations_) {
        NKikimrPQ::TPartitionOperation* o = tx.MutableOperations()->Add();
        o->SetPartitionId(*Partition_);
        auto [begin, end] = operations.GetRange();
        o->SetBegin(begin);
        o->SetEnd(end);
        o->SetConsumer(consumer);
        o->SetPath(*Topic_);
    }

    if (HasWriteOperations_) {
        NKikimrPQ::TPartitionOperation* o = tx.MutableOperations()->Add();
        o->SetPartitionId(*Partition_);
        o->SetPath(*Topic_);
    }

    tx.MutableCoordinators()->Add(Coordinators_.begin(), Coordinators_.end());
}

void TTopicPartitionOperations::Merge(const TTopicPartitionOperations& rhs)
{
    Y_VERIFY(Topic_.Empty() || Topic_ == rhs.Topic_);
    Y_VERIFY(Partition_.Empty() || Partition_ == rhs.Partition_);
    Y_VERIFY(TabletId_.Empty() || TabletId_ == rhs.TabletId_);

    if (Topic_.Empty()) {
        Topic_ = rhs.Topic_;
        Partition_ = rhs.Partition_;
        TabletId_ = rhs.TabletId_;
    }

    for (auto& [key, value] : rhs.Operations_) {
        Operations_[key].Merge(value);
    }

    HasWriteOperations_ |= rhs.HasWriteOperations_;

    // If the list of coordinators is empty, then we use a ready-made one.
    // Otherwise, we leave the common elements
    if (Coordinators_.empty()) {
        Coordinators_ = rhs.Coordinators_;
    } else {
        for (auto iter = Coordinators_.begin(); iter != Coordinators_.end(); ) {
            if (rhs.Coordinators_.contains(*iter)) {
                ++iter;
            } else {
                iter = Coordinators_.erase(iter);
            }
        }
    }
}

ui64 TTopicPartitionOperations::GetTabletId() const
{
    Y_VERIFY(TabletId_.Defined());

    return *TabletId_;
}

void TTopicPartitionOperations::SetTabletId(ui64 value)
{
    Y_VERIFY(TabletId_.Empty());

    TabletId_ = value;
}

bool TTopicPartitionOperations::HasReadOperations() const
{
    return !Operations_.empty();
}

bool TTopicPartitionOperations::HasWriteOperations() const
{
    return HasWriteOperations_;
}

void TTopicPartitionOperations::SetCoordinators(const TVector<ui64>& coordinators)
{
    Coordinators_ = std::unordered_set<ui64>(coordinators.begin(), coordinators.end());
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

bool TTopicOperations::TabletHasReadOperations(ui64 tabletId) const
{
    for (auto& [_, value] : Operations_) {
        if (value.GetTabletId() == tabletId) {
            return value.HasReadOperations();
        }
    }
    return false;
}

void TTopicOperations::AddOperation(const TString& topic, ui32 partition,
                                    const TString& consumer,
                                    const Ydb::Topic::OffsetsRange& range)
{
    TTopicPartition key{topic, partition};
    Operations_[key].AddOperation(topic, partition,
                                  consumer,
                                  range);
    HasReadOperations_ = true;
}

void TTopicOperations::AddOperation(const TString& topic, ui32 partition)
{
    TTopicPartition key{topic, partition};
    Operations_[key].AddOperation(topic, partition);
    HasWriteOperations_ = true;
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
                bool found = false;

                for (auto& consumer : description.GetPQTabletConfig().GetReadRules()) {
                    if (Consumer_ == consumer) {
                        found = true;
                        break;
                    }
                }

                if (!found) {
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
                    LOG_D("(topic, partition, tablet): "
                          << "'" << key.Topic_ << "'"
                          << ", " << partition.GetPartitionId()
                          << ", " << partition.GetTabletId());

                    p->second.SetTabletId(partition.GetTabletId());
                    p->second.SetCoordinators(result.DomainInfo->Coordinators.List());
                }
            }
        } else {
            builder << "Topic '" << JoinPath(result.Path) << "' is missing";

            status = Ydb::StatusIds::SCHEME_ERROR;
            message = std::move(builder);

            return false;
        }
    }

    status = Ydb::StatusIds::SUCCESS;
    message = "";

    return true;
}

void TTopicOperations::BuildTopicTxs(THashMap<ui64, NKikimrPQ::TDataTransaction> &txs)
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

    HasReadOperations_ |= rhs.HasReadOperations_;
    HasWriteOperations_ |= rhs.HasWriteOperations_;
}

TSet<ui64> TTopicOperations::GetReceivingTabletIds() const
{
    TSet<ui64> ids;
    for (auto& [_, operations] : Operations_) {
        if (operations.HasWriteOperations()) {
            ids.insert(operations.GetTabletId());
        }
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

size_t TTopicOperations::GetSize() const
{
    return Operations_.size();
}

}
