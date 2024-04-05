#pragma once
#include <ydb/core/tx/columnshard/engines/reader/abstract/abstract.h>
#include <ydb/core/tx/columnshard/engines/reader/sys_view/abstract/abstract.h>
#include <ydb/core/tx/columnshard/engines/reader/sys_view/constructor/constructor.h>
#include <ydb/core/sys_view/common/schema.h>

namespace NKikimr::NOlap::NReader::NSysView::NChunks {

class TConstructor: public TStatScannerConstructor<NKikimr::NSysView::Schema::PrimaryIndexStats> {
private:
    using TBase = TStatScannerConstructor<NKikimr::NSysView::Schema::PrimaryIndexStats>;
protected:
    virtual std::shared_ptr<NAbstract::TReadStatsMetadata> BuildMetadata(const NColumnShard::TColumnShard* self, const TReadDescription& read) const override;
public:
    using TBase::TBase;
};

class TReadStatsMetadata: public NAbstract::TReadStatsMetadata, std::enable_shared_from_this<TReadStatsMetadata> {
private:
    using TBase = NAbstract::TReadStatsMetadata;
    using TSysViewSchema = NKikimr::NSysView::Schema::PrimaryIndexStats;
public:
    using TBase::TBase;

    virtual std::unique_ptr<TScanIteratorBase> StartScan(const std::shared_ptr<TReadContext>& /*readContext*/) const override;
    virtual std::vector<std::pair<TString, NScheme::TTypeInfo>> GetKeyYqlSchema() const override;
};

class TStatsIterator: public NAbstract::TStatsIterator<NKikimr::NSysView::Schema::PrimaryIndexStats> {
private:
    using TBase = NAbstract::TStatsIterator<NKikimr::NSysView::Schema::PrimaryIndexStats>;
    virtual void AppendStats(const std::vector<std::unique_ptr<arrow::ArrayBuilder>>& builders, NAbstract::TGranuleMetaView& granule) const override;
    virtual ui32 PredictRecordsCount(const NAbstract::TGranuleMetaView& granule) const override;
    void AppendStats(const std::vector<std::unique_ptr<arrow::ArrayBuilder>>& builders, const TPortionInfo& portion) const;
public:
    using TBase::TBase;
};

}
