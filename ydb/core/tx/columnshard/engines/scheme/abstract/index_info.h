#pragma once
#include "loader.h"

#include <ydb/core/tx/columnshard/common/portion.h>

namespace NKikimr::NOlap {

class IIndexInfo {
public:
    static constexpr const char* SPEC_COL_PLAN_STEP = NOlap::NPortion::TSpecialColumns::SPEC_COL_PLAN_STEP;
    static constexpr const char* SPEC_COL_TX_ID = NOlap::NPortion::TSpecialColumns::SPEC_COL_TX_ID;
    static const TString STORE_INDEX_STATS_TABLE;
    static const TString TABLE_INDEX_STATS_TABLE;

    virtual std::shared_ptr<TColumnLoader> GetColumnLoaderOptional(const ui32 columnId) const = 0;
    std::shared_ptr<TColumnLoader> GetColumnLoaderVerified(const ui32 columnId) const;
    virtual ~IIndexInfo() = default;
};

} // namespace NKikimr::NOlap
