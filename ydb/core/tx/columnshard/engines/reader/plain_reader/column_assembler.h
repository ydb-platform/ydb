#pragma once
#include "source.h"
#include <ydb/core/tx/columnshard/engines/reader/conveyor_task.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/counters/common/object_counter.h>
#include <ydb/core/tx/columnshard/counters/scan.h>
#include <ydb/core/formats/arrow/arrow_filter.h>
#include "filter_assembler.h"

namespace NKikimr::NOlap::NPlainReader {
class TBatch;
class TAssembleBatch: public TAssemblerCommon, public NColumnShard::TMonitoringObjectsCounter<TAssembleBatch, true, true> {
private:
    using TBase = TAssemblerCommon;
    std::shared_ptr<NArrow::TColumnFilter> Filter;
protected:
    std::shared_ptr<arrow::RecordBatch> Result;
    const NColumnShard::TCounterGuard TaskGuard;
    const std::set<ui32> FetchColumnIds;
    virtual bool DoExecute() override;
public:
    virtual TString GetTaskClassIdentifier() const override {
        return "PlainReader::TAssembleBatch";
    }

    TAssembleBatch(const std::shared_ptr<TSpecialReadContext>& context, const std::shared_ptr<TPortionInfo>& portionInfo,
        const std::shared_ptr<IDataSource>& source, const std::set<ui32>& columnIds, const THashMap<TBlobRange, TPortionInfo::TAssembleBlobInfo>& blobs, const std::shared_ptr<NArrow::TColumnFilter>& filter);
};

class TAssembleFFBatch: public TAssembleBatch {
private:
    using TBase = TAssembleBatch;
protected:
    virtual bool DoApply(IDataReader& owner) const override;
public:
    using TBase::TBase;
};
}
