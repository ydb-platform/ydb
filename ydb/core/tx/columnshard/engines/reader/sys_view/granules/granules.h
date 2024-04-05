#pragma once
#include <ydb/core/sys_view/common/schema.h>
#include <ydb/core/tx/columnshard/engines/reader/sys_view/abstract/abstract.h>
#include <ydb/core/tx/columnshard/engines/reader/sys_view/constructor/constructor.h>
#include <util/system/hostname.h>

namespace NKikimr::NOlap::NReader::NSysView::NGranules {

class TConstructor: public TStatScannerConstructor<NKikimr::NSysView::Schema::PrimaryIndexGranuleStats> {
private:
    using TBase = TStatScannerConstructor<NKikimr::NSysView::Schema::PrimaryIndexGranuleStats>;
protected:
    virtual std::shared_ptr<NAbstract::TReadStatsMetadata> BuildMetadata(const NColumnShard::TColumnShard* self, const TReadDescription& read) const override;

public:
    using TBase::TBase;
};

struct TReadStatsMetadata: public NAbstract::TReadStatsMetadata {
private:
    using TBase = NAbstract::TReadStatsMetadata;
    using TSysViewSchema = NKikimr::NSysView::Schema::PrimaryIndexGranuleStats;
public:
    using TBase::TBase;

    virtual std::unique_ptr<TScanIteratorBase> StartScan(const std::shared_ptr<TReadContext>& readContext) const override;
    virtual std::vector<std::pair<TString, NScheme::TTypeInfo>> GetKeyYqlSchema() const override;
};

class TStatsIterator : public NAbstract::TStatsIterator<NKikimr::NSysView::Schema::PrimaryIndexGranuleStats> {
private:
    const std::string HostNameField = HostName();
    using TBase = NAbstract::TStatsIterator<NKikimr::NSysView::Schema::PrimaryIndexGranuleStats>;
    virtual ui32 PredictRecordsCount(const NAbstract::TGranuleMetaView& /*granule*/) const override {
        return 1;
    }
    virtual void AppendStats(const std::vector<std::unique_ptr<arrow::ArrayBuilder>>& builders, NAbstract::TGranuleMetaView& granule) const override;
public:
    using TBase::TBase;
};

}
