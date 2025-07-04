#pragma once
#include "common.h"
#include "meta.h"

#include <ydb/core/formats/arrow/special_keys.h>
#include <ydb/core/tx/columnshard/common/portion.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>

namespace NKikimr::NOlap {
class TPortionInfoConstructor;
struct TIndexInfo;

class TPortionMetaConstructor {
private:
    std::optional<NArrow::TFirstLastSpecialKeys> FirstAndLastPK;
    std::optional<TString> TierName;
    std::optional<TSnapshot> RecordSnapshotMin;
    std::optional<TSnapshot> RecordSnapshotMax;
    std::optional<ui64> CompactionLevel;

    std::optional<ui32> RecordsCount;
    std::optional<ui64> ColumnRawBytes;
    std::optional<ui32> ColumnBlobBytes;
    std::optional<ui32> IndexRawBytes;
    std::optional<ui32> IndexBlobBytes;

    std::optional<ui32> DeletionsCount;

    friend class TPortionInfoConstructor;
    friend class TPortionAccessorConstructor;
    void FillMetaInfo(const NArrow::TFirstLastSpecialKeys& primaryKeys, const ui32 deletionsCount,
        const std::optional<NArrow::TMinMaxSpecialKeys>& snapshotKeys, const TIndexInfo& indexInfo);

public:
    TPortionMetaConstructor() = default;
    TPortionMetaConstructor(const TPortionMeta& meta);

    const NArrow::TFirstLastSpecialKeys& GetFirstAndLastPK() const {
        AFL_VERIFY(FirstAndLastPK);
        return *FirstAndLastPK;
    }

    ui64 GetTotalBlobBytes() const {
        AFL_VERIFY(ColumnBlobBytes);
        AFL_VERIFY(IndexBlobBytes);
        return *ColumnBlobBytes + *IndexBlobBytes;
    }

    void SetCompactionLevel(const ui64 level) {
        CompactionLevel = level;
    }

    void SetTierName(const TString& tierName);
    void ResetTierName(const TString& tierName) {
        TierName.reset();
        SetTierName(tierName);
    }

    TPortionMeta Build();

    [[nodiscard]] bool LoadMetadata(
        const NKikimrTxColumnShard::TIndexPortionMeta& portionMeta, const TIndexInfo& indexInfo, const IBlobGroupSelector& groupSelector);
};

}   // namespace NKikimr::NOlap
