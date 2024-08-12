#pragma once
#include <util/system/types.h>
#include <util/generic/string.h>

namespace NKikimr::NOlap::NPortion {
// NOTE: These values are persisted in LocalDB so they must be stable
enum EProduced: ui32 {
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
    static constexpr const char* SPEC_COL_DELETE_FLAG = "_yql_delete_flag";
    static const ui32 SPEC_COL_PLAN_STEP_INDEX = 0xffffff00;
    static const ui32 SPEC_COL_TX_ID_INDEX = SPEC_COL_PLAN_STEP_INDEX + 1;
    static const ui32 SPEC_COL_DELETE_FLAG_INDEX = SPEC_COL_PLAN_STEP_INDEX + 2;
};

}
