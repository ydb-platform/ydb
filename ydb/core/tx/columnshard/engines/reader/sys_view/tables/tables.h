#pragma once
#include <ydb/core/sys_view/common/schema.h>
#include <ydb/core/tx/columnshard/engines/reader/sys_view/abstract/iterator.h>
#include <ydb/core/tx/columnshard/engines/reader/sys_view/constructor/constructor.h>

namespace NKikimr::NOlap::NReader::NSysView::NTables {

class TConstructor: public TStatScannerConstructor<NKikimr::NSysView::Schema::TablePathIdMapping> {
private:
    using TBase = TStatScannerConstructor<NKikimr::NSysView::Schema::TablePathIdMapping>;
protected:
    virtual std::shared_ptr<NAbstract::TReadStatsMetadata> BuildMetadata(const NColumnShard::TColumnShard* self, const TReadDescription& read) const override;

public:
    using TBase::TBase;
};

struct TReadStatsMetadata: public NAbstract::TReadStatsMetadata {
private:
    using TBase = NAbstract::TReadStatsMetadata;
    using TSysViewSchema = NKikimr::NSysView::Schema::TablePathIdMapping;
public:
    using TBase::TBase;

    virtual std::unique_ptr<TScanIteratorBase> StartScan(const std::shared_ptr<TReadContext>& readContext) const override;
    virtual std::vector<std::pair<TString, NScheme::TTypeInfo>> GetKeyYqlSchema() const override;
};

class TStatsIterator : public NAbstract::TStatsIterator<NKikimr::NSysView::Schema::TablePathIdMapping> {
private:
    using TBase = NAbstract::TStatsIterator<NKikimr::NSysView::Schema::TablePathIdMapping>;
    virtual bool AppendStats(const std::vector<std::unique_ptr<arrow::ArrayBuilder>>& builders, NAbstract::TGranuleMetaView& granule) const override;
    virtual ui32 PredictRecordsCount(const NAbstract::TGranuleMetaView& granule) const override;
    void AppendStats(const std::vector<std::unique_ptr<arrow::ArrayBuilder>>& builders, const TPortionInfo& portion) const;
public:
    using TBase::TBase;
};


class TTableSysViewPolicy: public NAbstract::ISysViewPolicy {
protected:
    virtual std::unique_ptr<IScannerConstructor> DoCreateConstructor(const TScannerConstructorContext& request) const override {
        return std::make_unique<TConstructor>(request);
    }
    virtual std::shared_ptr<NAbstract::IMetadataFiller> DoCreateMetadataFiller() const override {
        return std::make_shared<NAbstract::TMetadataFromTable>();
    }
public:
    static const inline TFactory::TRegistrator<TTableSysViewPolicy> Registrator = TFactory::TRegistrator<TTableSysViewPolicy>(TString(::NKikimr::NSysView::TablePathIdMappingName));

};

}
