#pragma once

#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <ydb/public/api/protos/ydb_topic.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>

#include <ydb/core/tx/long_tx_service/public/lock_handle.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

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

    std::pair<ui64, ui64> GetRange() const;

    ui64 GetBegin() const;
    ui64 GetEnd() const;

    void AddOperation(const TString& consumer, const Ydb::Topic::OffsetsRange& range);
    void Merge(const TConsumerOperations& rhs);

private:
    void AddOperationImpl(const TString& consumer,
                          ui64 begin, ui64 end);

    TMaybe<TString> Consumer_;
    TDisjointIntervalTree<ui64> Offsets_;
};

class TTopicPartitionOperations {
public:
    bool IsValid() const;

    void AddOperation(const TString& topic, ui32 partition,
                      const TString& consumer,
                      const Ydb::Topic::OffsetsRange& range);
    void AddOperation(const TString& topic, ui32 partition,
                      TMaybe<ui32> supportivePartition);

    void BuildTopicTxs(THashMap<ui64, NKikimrPQ::TDataTransaction> &txs);

    void Merge(const TTopicPartitionOperations& rhs);

    void SetTabletId(ui64 value);
    ui64 GetTabletId() const;

    bool HasReadOperations() const;
    bool HasWriteOperations() const;

private:
    TMaybe<TString> Topic_;
    TMaybe<ui32> Partition_;
    THashMap<TString, TConsumerOperations> Operations_;
    bool HasWriteOperations_ = false;
    TMaybe<ui64> TabletId_;
    TMaybe<ui32> SupportivePartition_;
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
    bool HasWriteId() const;
    ui64 GetWriteId() const;
    void SetWriteId(NLongTxService::TLockHandle handle);

    bool TabletHasReadOperations(ui64 tabletId) const;

    void AddOperation(const TString& topic, ui32 partition,
                      const TString& consumer,
                      const Ydb::Topic::OffsetsRange& range);
    void AddOperation(const TString& topic, ui32 partition,
                      TMaybe<ui32> supportivePartition);

    void FillSchemeCacheNavigate(NSchemeCache::TSchemeCacheNavigate& navigate,
                                 TMaybe<TString> consumer);
    bool ProcessSchemeCacheNavigate(const NSchemeCache::TSchemeCacheNavigate::TResultSet& results,
                                    Ydb::StatusIds_StatusCode& status,
                                    TString& message);

    void BuildTopicTxs(THashMap<ui64, NKikimrPQ::TDataTransaction> &txs);

    void Merge(const TTopicOperations& rhs);

    TSet<ui64> GetReceivingTabletIds() const;
    TSet<ui64> GetSendingTabletIds() const;

    size_t GetSize() const;

private:
    THashMap<TTopicPartition, TTopicPartitionOperations, TTopicPartition::THash> Operations_;
    bool HasReadOperations_ = false;
    bool HasWriteOperations_ = false;

    TMaybe<TString> Consumer_;
    NLongTxService::TLockHandle WriteId_;
};

}
