#pragma once
#include <ydb/core/formats/arrow/program/abstract.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>

namespace NKikimr::NOlap::NReader::NCommon {

class TIndexColumnResolver: public NArrow::NSSA::IColumnResolver {
    const NOlap::TIndexInfo& IndexInfo;

public:
    std::shared_ptr<arrow::Field> GetColumnFieldOptional(ui32 id) const override {
        return IndexInfo.GetColumnFieldOptional(id);
    }

    explicit TIndexColumnResolver(const NOlap::TIndexInfo& indexInfo)
        : IndexInfo(indexInfo)
    {
    }

    virtual std::optional<ui32> GetColumnIdOptional(const TString& name) const override {
        return IndexInfo.GetColumnIdOptional(name);
    }

    TString GetColumnName(ui32 id, bool required) const override {
        return IndexInfo.GetColumnName(id, required);
    }

    NArrow::NSSA::TColumnInfo GetDefaultColumn() const override {
        return NArrow::NSSA::TColumnInfo::Original((ui32)NOlap::TIndexInfo::ESpecialColumn::PLAN_STEP, NOlap::TIndexInfo::SPEC_COL_PLAN_STEP);
    }
};

}   // namespace NKikimr::NOlap::NReader::NCommon
