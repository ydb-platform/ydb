#pragma once
#include <ydb/core/sys_view/common/schema.h>
#include <ydb/core/tx/columnshard/engines/reader/sys_view/abstract/abstract.h>
#include <ydb/core/tx/columnshard/engines/reader/sys_view/constructor/constructor.h>

namespace NKikimr::NOlap::NReader::NSysView::NPortions {

class TConstructor: public TStatScannerConstructor<NKikimr::NSysView::Schema::PrimaryIndexPortionStats> {
private:
    using TBase = TStatScannerConstructor<NKikimr::NSysView::Schema::PrimaryIndexPortionStats>;
protected:
    virtual std::shared_ptr<NAbstract::TReadStatsMetadata> BuildMetadata(const NColumnShard::TColumnShard* self, const TReadDescription& read) const override;

public:
    using TBase::TBase;
};

struct TReadStatsMetadata: public NAbstract::TReadStatsMetadata {
private:
    using TBase = NAbstract::TReadStatsMetadata;
    using TSysViewSchema = NKikimr::NSysView::Schema::PrimaryIndexPortionStats;
public:
    using TBase::TBase;

    virtual std::unique_ptr<TScanIteratorBase> StartScan(const std::shared_ptr<TReadContext>& readContext) const override;
    virtual std::vector<std::pair<TString, NScheme::TTypeInfo>> GetKeyYqlSchema() const override;
};

class TStatsIterator : public NAbstract::TStatsIterator<NKikimr::NSysView::Schema::PrimaryIndexPortionStats> {
private:
    using TBase = NAbstract::TStatsIterator<NKikimr::NSysView::Schema::PrimaryIndexPortionStats>;
    virtual void AppendStats(const std::vector<std::unique_ptr<arrow::ArrayBuilder>>& builders, const TPortionInfo& portion) const override;
    virtual ui32 GetConstructionRecordsCount(const TPortionInfo& /*portion*/) const override {
        return 1;
    }
public:
    using TBase::TBase;
};

}
