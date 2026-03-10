#pragma once

#include <ydb/core/kqp/common/kqp_resolve.h>
#include <ydb/core/protos/tx_datashard.pb.h>

namespace NKikimrTxDataShard {
    class TKqpTransaction_TDataTaskMeta_TKeyRange;
    class TKqpTransaction_TScanTaskMeta_TReadOpMeta;
    class TKqpReadRangesSourceSettings;
}

namespace NKikimr::NKqp {

struct TShardKeyRanges {
    // ordered ranges and points
    TVector<TSerializedPointOrRange> Ranges;
    std::optional<TSerializedTableRange> FullRange;

    void AddPoint(TSerializedCellVec&& point);
    void AddRange(TSerializedTableRange&& range);
    void Add(TSerializedPointOrRange&& pointOrRange);

    void CopyFrom(const TVector<TSerializedPointOrRange>& ranges);

    void MakeFullRange(TSerializedTableRange&& range);
    void MakeFullPoint(TSerializedCellVec&& range);
    void MakeFull(TSerializedPointOrRange&& pointOrRange);

    bool HasRanges() const;

    bool IsFullRange() const { return FullRange.has_value(); }
    TVector<TSerializedPointOrRange>& GetRanges() { return Ranges; }

    void MergeWritePoints(TShardKeyRanges&& other, const TVector<NScheme::TTypeInfo>& keyTypes);

    TString ToString(const TVector<NScheme::TTypeInfo>& keyTypes, const NScheme::TTypeRegistry& typeRegistry) const;
    void SerializeTo(NKikimrTxDataShard::TKqpTransaction_TDataTaskMeta_TKeyRange* proto) const;
    void SerializeTo(NKikimrTxDataShard::TKqpTransaction_TScanTaskMeta_TReadOpMeta* proto) const;
    void SerializeTo(NKikimrTxDataShard::TKqpReadRangesSourceSettings* proto, bool allowPoints = true) const;
    void ParseFrom(const NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta::TReadOpMeta& proto);

    std::pair<const TSerializedCellVec*, bool> GetRightBorder() const;
};

struct TShardInfo {
    struct TColumnWriteInfo {
        ui32 MaxValueSizeBytes = 0;
    };

    TMaybe<TShardKeyRanges> KeyReadRanges;  // empty -> no reads
    TMaybe<TShardKeyRanges> KeyWriteRanges; // empty -> no writes
    THashMap<TString, TColumnWriteInfo> ColumnWrites;

    TString ToString(const TVector<NScheme::TTypeInfo>& keyTypes, const NScheme::TTypeRegistry& typeRegistry) const;
};

using TShardIdToInfoMap = THashMap<ui64 /* shardId */, TShardInfo>;

class TShardInfoWithId: public TShardInfo {
public:
    ui64 ShardId;
    TShardInfoWithId(const ui64 shardId, TShardInfo&& base)
        : TShardInfo(std::move(base))
        , ShardId(shardId) {

    }
};

} // namespace NKikimr::NKqp
