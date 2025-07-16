#pragma once
#include <ydb/core/formats/arrow/rows/view.h>

#include <util/generic/string.h>
#include <util/system/types.h>

namespace NKikimr::NOlap::NPortion {
// NOTE: These values are persisted in LocalDB so they must be stable
enum EProduced : ui32 {
    UNSPECIFIED = 0,
    INSERTED,
    COMPACTED,
    SPLIT_COMPACTED,
    INACTIVE,
    EVICTED
};

class TSpecialColumns {
public:
    static constexpr const char* SPEC_COL_PLAN_STEP = "_yql_plan_step";
    static constexpr const char* SPEC_COL_TX_ID = "_yql_tx_id";
    static constexpr const char* SPEC_COL_WRITE_ID = "_yql_write_id";
    static constexpr const char* SPEC_COL_DELETE_FLAG = "_yql_delete_flag";
    static constexpr const ui32 SPEC_COL_PLAN_STEP_INDEX = 0xffffff00;
    static constexpr const ui32 SPEC_COL_TX_ID_INDEX = SPEC_COL_PLAN_STEP_INDEX + 1;
    static constexpr const ui32 SPEC_COL_WRITE_ID_INDEX = SPEC_COL_PLAN_STEP_INDEX + 2;
    static constexpr const ui32 SPEC_COL_DELETE_FLAG_INDEX = SPEC_COL_PLAN_STEP_INDEX + 3;
};

class TPortionInfoForCompaction {
private:
    YDB_READONLY(ui64, TotalBlobBytes, 0);
    const NArrow::TSimpleRow FirstPK;
    const NArrow::TSimpleRow LastPK;

public:
    TPortionInfoForCompaction(const ui64 totalBlobBytes, const NArrow::TSimpleRow& firstPK, const NArrow::TSimpleRow& lastPK)
        : TotalBlobBytes(totalBlobBytes)
        , FirstPK(firstPK)
        , LastPK(lastPK) {
    }

    const NArrow::TSimpleRow& GetFirstPK() const {
        return FirstPK;
    }
    const NArrow::TSimpleRow& GetLastPK() const {
        return LastPK;
    }
};

}   // namespace NKikimr::NOlap::NPortion
