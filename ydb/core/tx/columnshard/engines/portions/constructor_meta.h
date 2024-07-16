#pragma once
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
    std::optional<NPortion::EProduced> Produced;
    std::optional<ui32> DeletionsCount;
    friend class TPortionInfoConstructor;
    void FillMetaInfo(const NArrow::TFirstLastSpecialKeys& primaryKeys, const ui32 deletionsCount, const NArrow::TMinMaxSpecialKeys& snapshotKeys, const TIndexInfo& indexInfo);

public:
    TPortionMetaConstructor() = default;
    TPortionMetaConstructor(const TPortionMeta& meta);

    void SetTierName(const TString& tierName);
    void ResetTierName(const TString& tierName) {
        TierName.reset();
        SetTierName(tierName);
    }

    void UpdateRecordsMeta(const NPortion::EProduced prod) {
        Produced = prod;
    }

    TPortionMeta Build();

    [[nodiscard]] bool LoadMetadata(const NKikimrTxColumnShard::TIndexPortionMeta& portionMeta, const TIndexInfo& indexInfo);

};

}