#pragma once

#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <ydb/public/api/protos/ydb_topic.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>

#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/tx/long_tx_service/public/lock_handle.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <ydb/core/kafka_proxy/kafka_producer_instance_id.h>

#include <ydb/library/actors/core/actor.h>

#include <util/system/types.h>

#include <util/generic/fwd.h>
#include <util/generic/hash.h>
#include <util/generic/ylimits.h>

#include <library/cpp/containers/disjoint_interval_tree/disjoint_interval_tree.h>

namespace NKikimr::NKqp::NTopic {

class TOffsetsRangeIntersectExpection : public yexception {
};

class TConsumerOperations {
public:
    bool IsValid() const;

    std::pair<ui64, ui64> GetOffsetsCommitRange() const;

    ui64 GetOffsetCommitBegin() const;
    ui64 GetOffsetCommitEnd() const;

    bool GetForceCommit() const;
    bool GetKillReadSession() const;
    bool GetOnlyCheckCommitedToFinish() const;
    TString GetReadSessionId() const;
    ui64 GetKafkaCommitOffset() const;

    void AddOperation(const TString& consumer,
                      const NKikimrKqp::TTopicOperationsRequest_TopicOffsets_PartitionOffsets_OffsetsRange& range,
                      bool forceCommit = false,
                      bool killReadSession = false,
                      bool onlyCheckCommitedToFinish = false,
                      const TString& readSessionId = {});
    void AddKafkaApiOffsetCommit(const TString& consumer, ui64 offset);
    
    bool IsKafkaApiOperation() const;

    void Merge(const TConsumerOperations& rhs);

private:
    void AddOperationImpl(const TString& consumer,
                          ui64 begin,
                          ui64 end,
                          bool forceCommit = false,
                          bool killReadSession = false,
                          bool onlyCheckCommitedToFinish = false,
                          const TString& readSessionId = {});

    TMaybe<TString> Consumer_;
    TDisjointIntervalTree<ui64> Offsets_;
    bool ForceCommit_ = false;
    bool KillReadSession_ = false;
    bool OnlyCheckCommitedToFinish_ = false;
    TString ReadSessionId_;
    TMaybe<ui64> KafkaCommitOffset_;
};

struct TTopicOperationTransaction {
    NKikimrPQ::TDataTransaction tx;
    bool hasWrite = false;
};

using TTopicOperationTransactions = THashMap<ui64, TTopicOperationTransaction>;

class TTopicPartitionOperations {
public:
    bool IsValid() const;

    void AddOperation(const TString& topic,
                      ui32 partition,
                      const TString& consumer,
                      const NKikimrKqp::TTopicOperationsRequest_TopicOffsets_PartitionOffsets_OffsetsRange& range,
                      bool forceCommit = false,
                      bool killReadSession = false,
                      bool onlyCheckCommitedToFinish = false,
                      const TString& readSessionId = {});
    void AddOperation(const TString& topic, ui32 partition,
                      TMaybe<ui32> supportivePartition);
    void AddKafkaApiWriteOperation(const TString& topic, ui32 partition, const NKafka::TProducerInstanceId& producerInstanceId);
    
    void AddKafkaApiReadOperation(const TString& topic, ui32 partition, const TString& consumerName, ui64 offset);

    void BuildTopicTxs(TTopicOperationTransactions &txs);

    void Merge(const TTopicPartitionOperations& rhs);

    void SetTabletId(ui64 value);
    ui64 GetTabletId() const;
    bool HasTabletId() const;

    TMaybe<TString> GetTopicName() const;

    bool HasReadOperations() const;
    bool HasWriteOperations() const;

private:
    TMaybe<TString> Topic_;
    TMaybe<ui32> Partition_;
    THashMap<TString, TConsumerOperations> Operations_;
    bool HasWriteOperations_ = false;
    TMaybe<ui64> TabletId_;
    TMaybe<ui32> SupportivePartition_;
    TMaybe<NKafka::TProducerInstanceId> KafkaProducerInstanceId_;
};

struct TTopicPartition {
    struct THash {
        size_t operator()(const TTopicPartition& x) const;
    };

    TTopicPartition(TString topic, ui32 partition);

    bool operator==(const TTopicPartition& x) const;

    TString Topic_;
    ui32 Partition_;
};

class TTopicOperations {
public:
    bool IsValid() const;

    bool HasOperations() const;
    bool HasReadOperations() const;
    bool HasWriteOperations() const;
    bool HasKafkaOperations() const;
    bool HasWriteId() const;
    ui64 GetWriteId() const;
    void SetWriteId(NLongTxService::TLockHandle handle);
    NKafka::TProducerInstanceId GetKafkaProducerInstanceId() const;

    bool TabletHasReadOperations(ui64 tabletId) const;

    void AddOperation(const TString& topic, ui32 partition,
                      const TString& consumer,
                      const NKikimrKqp::TTopicOperationsRequest_TopicOffsets_PartitionOffsets_OffsetsRange& range,
                      bool forceCommit,
                      bool killReadSession,
                      bool onlyCheckCommitedToFinish,
                      const TString& readSessionId);
    void AddOperation(const TString& topic, ui32 partition,
                      TMaybe<ui32> supportivePartition);
                      
    void AddKafkaApiWriteOperation(const TString& topic, ui32 partition, const NKafka::TProducerInstanceId& producerInstanceId);
    
    void AddKafkaApiReadOperation(const TString& topic, ui32 partition, const TString& consumerName, ui64 offset);

    void FillSchemeCacheNavigate(NSchemeCache::TSchemeCacheNavigate& navigate,
                                 TMaybe<TString> consumer);
    bool ProcessSchemeCacheNavigate(const NSchemeCache::TSchemeCacheNavigate::TResultSet& results,
                                    Ydb::StatusIds_StatusCode& status,
                                    TString& message);
    void CacheSchemeCacheNavigate(const NSchemeCache::TSchemeCacheNavigate::TResultSet& results);

    void BuildTopicTxs(TTopicOperationTransactions &txs);

    void Merge(const TTopicOperations& rhs);

    TSet<ui64> GetReceivingTabletIds() const;
    TSet<ui64> GetSendingTabletIds() const;

    TMaybe<TString> GetTabletName(ui64 tabletId) const;

    size_t GetSize() const;

    bool HasThisPartitionAlreadyBeenAdded(const TString& topic, ui32 partitionId);

private:
    THashMap<TTopicPartition, TTopicPartitionOperations, TTopicPartition::THash> Operations_;
    bool HasReadOperations_ = false;
    bool HasWriteOperations_ = false;
    bool HasKafkaOperations_ = false;

    TMaybe<TString> Consumer_;
    NLongTxService::TLockHandle WriteId_;
    TMaybe<NKafka::TProducerInstanceId> KafkaProducerInstanceId_;
};

}
