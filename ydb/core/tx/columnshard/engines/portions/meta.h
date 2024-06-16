#pragma once
#include <ydb/core/tx/columnshard/common/portion.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>
#include <ydb/core/tx/columnshard/engines/scheme/statistics/abstract/portion_storage.h>
#include <ydb/core/formats/arrow/replace_key.h>
#include <ydb/core/formats/arrow/special_keys.h>
#include <ydb/core/protos/tx_columnshard.pb.h>
#include <ydb/library/accessor/accessor.h>
#include <util/stream/output.h>

namespace NKikimr::NOlap {

struct TIndexInfo;

struct TPortionMeta {
private:
    std::shared_ptr<NArrow::TFirstLastSpecialKeys> ReplaceKeyEdges; // first and last PK rows
    YDB_ACCESSOR_DEF(TString, TierName);
    YDB_READONLY_DEF(NStatistics::TPortionStorage, StatisticsStorage);
public:
    using EProduced = NPortion::EProduced;

    std::optional<NArrow::TReplaceKey> IndexKeyStart;
    std::optional<NArrow::TReplaceKey> IndexKeyEnd;

    std::optional<TSnapshot> RecordSnapshotMin;
    std::optional<TSnapshot> RecordSnapshotMax;
    EProduced Produced{EProduced::UNSPECIFIED};
    ui32 FirstPkColumn = 0;

    ui64 GetMetadataMemorySize() const {
        return sizeof(TPortionMeta) + ReplaceKeyEdges->GetMemorySize();
    }

    void SetStatisticsStorage(NStatistics::TPortionStorage&& storage) {
        AFL_VERIFY(StatisticsStorage.IsEmpty());
        StatisticsStorage = std::move(storage);
    }

    void ResetStatisticsStorage(NStatistics::TPortionStorage&& storage) {
        StatisticsStorage = std::move(storage);
    }

    bool IsChunkWithPortionInfo(const ui32 columnId, const ui32 chunkIdx) const {
        return columnId == FirstPkColumn && chunkIdx == 0;
    }

    bool DeserializeFromProto(const NKikimrTxColumnShard::TIndexPortionMeta& portionMeta, const TIndexInfo& indexInfo);

    std::optional<NKikimrTxColumnShard::TIndexPortionMeta> SerializeToProto(const ui32 columnId, const ui32 chunk) const;
    NKikimrTxColumnShard::TIndexPortionMeta SerializeToProto() const;

    void FillBatchInfo(const NArrow::TFirstLastSpecialKeys& primaryKeys, const NArrow::TMinMaxSpecialKeys& snapshotKeys, const TIndexInfo& indexInfo);

    EProduced GetProduced() const {
        return Produced;
    }

    TString DebugString() const;

    friend IOutputStream& operator << (IOutputStream& out, const TPortionMeta& info) {
        out << info.DebugString();
        return out;
    }

    bool HasSnapshotMinMax() const {
        return !!RecordSnapshotMax && !!RecordSnapshotMin;
    }

    bool HasPrimaryKeyBorders() const {
        return !!IndexKeyStart && !!IndexKeyEnd;
    }
};

class TPortionAddress {
private:
    YDB_READONLY(ui64, PathId, 0);
    YDB_READONLY(ui64, PortionId, 0);
public:
    TPortionAddress(const ui64 pathId, const ui64 portionId)
        : PathId(pathId)
        , PortionId(portionId)
    {

    }

    TString DebugString() const;

    bool operator<(const TPortionAddress& item) const {
        return std::tie(PathId, PortionId) < std::tie(item.PathId, item.PortionId);
    }

    bool operator==(const TPortionAddress& item) const {
        return std::tie(PathId, PortionId) == std::tie(item.PathId, item.PortionId);
    }
};

} // namespace NKikimr::NOlap

template<>
struct THash<NKikimr::NOlap::TPortionAddress> {
    inline ui64 operator()(const NKikimr::NOlap::TPortionAddress& x) const noexcept {
        return CombineHashes(x.GetPortionId(), x.GetPathId());
    }
};

