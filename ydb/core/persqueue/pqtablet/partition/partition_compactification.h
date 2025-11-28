#pragma once

#include <ydb/core/persqueue/common/key.h>
#include <ydb/core/persqueue/pqtablet/blob/blob.h>

#include <ydb/core/keyvalue/keyvalue_events.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/tablet/tablet_counters_protobuf.h>

#include <ydb/core/protos/counters_pq.pb.h>

namespace NKikimr::NPQ {

class TPartition;

struct TBlobInfo {
    TKey Key;
    ui64 LastOffset;
    bool HasDanglingParts;
    bool HasTrailingParts;
    THashMap<ui64, ui64> OffsetToPartNo;
    THashSet<ui64> OffsetsToKeep;

    void DropMessage(ui64 offset);
    bool CanDelete(const TMaybe<TBlobInfo>& previous, const TMaybe<TBlobInfo>& next) const;

};

using TPartitionKeyCompactionCounters = TProtobufTabletLabeledCounters<EPartitionKeyCompactionLabeledCounters_descriptor>;

struct TKeyCompactionCounters {
    ui64 UncompactedSize = 0;
    ui64 CompactedSize = 0;
    ui64 UncompactedCount = 0;
    ui64 CompactedCount = 0;
    // ui64 GetUncompactedRatio() {
    //     if (UncompactedSize > 0) {
    //         return 100.0 * CompactedSize / UncompactedSize;
    //     } else {
    //         return 0;
    //     }
    // }
    TDuration CurrReadCycleDuration = TDuration::Zero();
    ui64 CurrentReadCycleKeys = 0;
    ui64 ReadCyclesCount = 0;
    ui64 WriteCyclesCount = 0;
};

class TPartitionCompaction {
public:
    TPartitionCompaction(ui64 lastCompactedOffset, ui64 partReqestCookie, TPartition* partitionActor);

    enum class EStep {
        PENDING,
        READING,
        COMPACTING,
    };


    struct TReadState {
        friend TPartitionCompaction;
        constexpr static const ui64 MAX_DATA_KEYS = 5000;

        ui64 OffsetToRead;
        ui64 LastOffset = 0;

        ui64 NextPartNo = 0;
        THashMap<TString, ui64> TopicData; //Key -> Offset
        TMaybe<ui64> SkipOffset;

        TPartition* PartitionActor;
        TMaybe<NKikimrClient::TCmdReadResult::TResult> LastMessage;

    public:
        TReadState(ui64 firstOffset, TPartition* partitionActor);

        bool ProcessResponse(TEvPQ::TEvProxyResponse::TPtr& ev);
        EStep ContinueIfPossible(ui64 nextRequestCookie);
        THashMap<TString, ui64>&& GetData();
        ui64 GetLastOffset();
        void UpdateConfig(ui64 maxBurst, ui64 readQuota); //ToDo;
    };

    struct TCompactState {
        friend TPartitionCompaction;
        using TKeysIter = std::deque<TDataKey>::iterator;

        constexpr static const ui64 MAX_REQUEST_DATA_SIZE = 24_MB;
        constexpr static const ui64 MAX_BLOB_SIZE = 8_MB;

        ui64 MaxOffset;
        THashMap<TString, ui64> TopicData;
        TPartition* PartitionActor;

        TMaybe<ui64> LastProcessedOffset; // for ui
        bool CommitDone = false;
        ui64 CommitCookie = 0;
        TKeysIter KeysIter;
        bool Failure = false;

        THolder<TEvKeyValue::TEvRequest> Request;

        TMaybe<NKikimrClient::TCmdReadResult::TResult> CurrentMessage;
        TMaybe<TBatch> LastBatch;
        TKey LastBatchKey;
        TVector<TClientBlob> CurrMsgPartsFromLastBatch;
        TVector<TKey> CurrMsgMiddleBlobKeys;
        ui64 BlobsToWriteInRequest = 0;

        ui64 FirstHeadOffset;
        ui64 FirstHeadPartNo;
        ui64 CommittedOffset;

        std::deque<TDataKey> DataKeysBody;
        TMap<TKey, ui64> UpdatedKeys;
        TSet<TKey> DeletedKeys;

        TMaybe<ui64> SkipOffset;
        TMap<ui64, TKey> EmptyBlobs;
        TKeyCompactionCounters* Counters;

        TCompactState(THashMap<TString, ui64>&& data, ui64 firstUncompactedOffset, ui64 maxOffset, TPartition* partitionActor, TKeyCompactionCounters* counters);

        bool ProcessKVResponse(TEvKeyValue::TEvResponse::TPtr& ev);
        bool ProcessResponse(TEvPQ::TEvProxyResponse::TPtr& ev);

        EStep ContinueIfPossible(ui64 nextCookie);
        void RunKvRequest();
        void AddCmdWrite(const TKey& key, TBatch& batch);
        void AddDeleteRange(const TKey& key);
        void SendCommit(ui64 cookie);
        void SaveLastBatch();
        void UpdateDataKeysBody();
    };

    EStep Step = EStep::PENDING;
    ui64 FirstUncompactedOffset = 0;
    ui64 PartRequestCookie = 0;


    TMaybe<TReadState> ReadState;
    TMaybe<TCompactState> CompactState;
    TPartition* PartitionActor;

    mutable TKeyCompactionCounters Counters;
    TInstant ReadCycleStart = TInstant::Zero();

public:
    void TryCompactionIfPossible();
    void ProcessResponse(TEvPQ::TEvProxyResponse::TPtr& ev);
    void ProcessResponse(TEvKeyValue::TEvResponse::TPtr& ev);
    void ProcessResponse(TEvPQ::TEvError::TPtr& ev);

    TKeyCompactionCounters GetCounters() const;
    void UpdateSizeCounters();
};

} // namespace NKikimr::NPQ