#pragma once
#include <ydb/core/tx/program/program.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>

namespace NKikimr::NOlap::NReader::NPlain {

class TIndexColumnResolver: public IColumnResolver {
    const NOlap::TIndexInfo& IndexInfo;

public:
    explicit TIndexColumnResolver(const NOlap::TIndexInfo& indexInfo)
        : IndexInfo(indexInfo) {
    }

    virtual std::optional<ui32> GetColumnIdOptional(const TString& name) const override {
        return IndexInfo.GetColumnIdOptional(name);
    }

    TString GetColumnName(ui32 id, bool required) const override {
        return IndexInfo.GetColumnName(id, required);
    }

    NSsa::TColumnInfo GetDefaultColumn() const override {
        return NSsa::TColumnInfo::Original((ui32)NOlap::TIndexInfo::ESpecialColumn::PLAN_STEP, NOlap::TIndexInfo::SPEC_COL_PLAN_STEP);
    }
};

}