#pragma once
#include "loader.h"

#include <ydb/core/tx/columnshard/common/portion.h>

namespace NKikimr::NOlap {

class IIndexInfo {
public:
    enum class ESpecialColumn: ui32 {
        PLAN_STEP = NOlap::NPortion::TSpecialColumns::SPEC_COL_PLAN_STEP_INDEX,
        TX_ID = NOlap::NPortion::TSpecialColumns::SPEC_COL_TX_ID_INDEX,
        DELETE_FLAG = NOlap::NPortion::TSpecialColumns::SPEC_COL_DELETE_FLAG_INDEX
    };

    static constexpr const char* SPEC_COL_PLAN_STEP = NOlap::NPortion::TSpecialColumns::SPEC_COL_PLAN_STEP;
    static constexpr const char* SPEC_COL_TX_ID = NOlap::NPortion::TSpecialColumns::SPEC_COL_TX_ID;
    static constexpr const char* SPEC_COL_DELETE_FLAG = NOlap::NPortion::TSpecialColumns::SPEC_COL_DELETE_FLAG;

    static const char* GetDeleteFlagColumnName() {
        return SPEC_COL_DELETE_FLAG;
    }

    static const std::vector<std::string>& GetSnapshotColumnNames() {
        static const std::vector<std::string> result = { std::string(SPEC_COL_PLAN_STEP), std::string(SPEC_COL_TX_ID) };
        return result;
    }

    static const std::vector<ui32>& GetSnapshotColumnIds() {
        static const std::vector<ui32> result = { (ui32)ESpecialColumn::PLAN_STEP, (ui32)ESpecialColumn::TX_ID };
        return result;
    }

    static const std::set<ui32>& GetSnapshotColumnIdsSet() {
        static const std::set<ui32> result = { (ui32)ESpecialColumn::PLAN_STEP, (ui32)ESpecialColumn::TX_ID };
        return result;
    }

    static const std::vector<std::string>& GetSystemColumnNames() {
        static const std::vector<std::string> result = { std::string(SPEC_COL_PLAN_STEP), std::string(SPEC_COL_TX_ID), std::string(SPEC_COL_DELETE_FLAG) };
        return result;
    }

    static const std::vector<ui32>& GetSystemColumnIds() {
        static const std::vector<ui32> result = { (ui32)ESpecialColumn::PLAN_STEP, (ui32)ESpecialColumn::TX_ID, (ui32)ESpecialColumn::DELETE_FLAG };
        return result;
    }

    virtual std::shared_ptr<TColumnLoader> GetColumnLoaderOptional(const ui32 columnId) const = 0;
    std::shared_ptr<TColumnLoader> GetColumnLoaderVerified(const ui32 columnId) const;
    virtual ~IIndexInfo() = default;
};

} // namespace NKikimr::NOlap
