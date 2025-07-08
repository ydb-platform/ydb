#pragma once

#include "key.h"

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

class TPartitionCompaction {
public:
    TPartitionCompaction(ui64 lastCompactedOffset, ui64 startCookie, ui64 endCookie, TPartition* partitionActor);

    enum class EStep {
        PENDING,
        READING,
        COMPACTING,
    };

    class TReadState {
        friend TPartitionCompaction;
        constexpr static const ui64 MAX_DATA_KEYS = 5000;

        ui64 OffsetToRead;
        ui64 LastOffset = 0;
        ui64 NextPartNo = 0;
        THashMap<TString, ui64> TopicData; //Key -> Offset


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

    class TCompactState {
        friend TPartitionCompaction;
        using TKeysIter = std::deque<TDataKey>::iterator;

        THolder<TEvKeyValue::TEvRequest> Request;
        ui64 RequestSize;
        ui64 MaxOffset;
        THashMap<TString, ui64> TopicData;
        TPartition* PartitionActor;

        ui64 LastProcessedOffset;
        TKeysIter KeysIter;
        bool Failure = false;
        ui64 RequestDataSize = 0;
        std::deque<TDataKey> DataKeysBody;
        std::deque<TDataKey> DroppedKeys;
        ui64 FirstHeadOffset;
        ui64 FirstHeadPartNo;

        TCompactState(THashMap<TString, ui64>&& data, ui64 firstUncompactedOffset, ui64 maxOffset, TPartition* partitionActor);

        EStep ProcessKVResponse(TEvKeyValue::TEvResponse::TPtr& ev);
        bool ProcessResponse(TEvPQ::TEvProxyResponse::TPtr& ev);

        EStep ContinueIfPossible(ui64 nextCookie);
        void RunKvRequest();
        void AddDeleteRange(const TKey& key);
    };

    EStep Step = EStep::PENDING;
    ui64 FirstUncompactedOffset = 0;
    bool HaveRequestInflight = false;
    ui64 StartCookie = 0;
    ui64 EndCookie = 0;
    ui64 CurrentCookie = 0;


    std::optional<TReadState> ReadState;
    std::optional<TCompactState> CompactState;
    TPartition* PartitionActor;

    ui64 GetNextCookie() {
        CurrentCookie++;
        if (CurrentCookie > EndCookie) {
            CurrentCookie = StartCookie;
        }
        return CurrentCookie;
    }

    void TryCompactionIfPossible();
    void UpdatePartitionConifg();
    void ProcessResponse(TEvPQ::TEvProxyResponse::TPtr& ev);
    void ProcessResponse(TEvKeyValue::TEvResponse::TPtr& ev);

};

} // namespace NKikimr::NPQ