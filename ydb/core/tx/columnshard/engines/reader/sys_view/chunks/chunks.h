#pragma once
#include <ydb/core/tx/columnshard/engines/reader/abstract/abstract.h>
#include <ydb/core/tx/columnshard/engines/reader/sys_view/abstract/iterator.h>
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
    class TViewContainer {
    private:
        TString Data;
        std::string STLData;
        arrow::util::string_view View;

    public:
        const arrow::util::string_view& GetView() const {
            return View;
        }

        TViewContainer(const TString& data)
            : Data(data)
            , View(arrow::util::string_view(Data.data(), Data.size())) {
        }

        TViewContainer(const std::string& data)
            : STLData(data)
            , View(arrow::util::string_view(STLData.data(), STLData.size())) {
        }
    };

    mutable THashMap<ui32, TViewContainer> ColumnNamesById;
    mutable THashMap<NPortion::EProduced, TViewContainer> PortionType;
    mutable THashMap<TString, THashMap<ui32, TViewContainer>> EntityStorageNames;

    using TBase = NAbstract::TStatsIterator<NKikimr::NSysView::Schema::PrimaryIndexStats>;
    virtual bool AppendStats(const std::vector<std::unique_ptr<arrow::ArrayBuilder>>& builders, NAbstract::TGranuleMetaView& granule) const override;
    virtual ui32 PredictRecordsCount(const NAbstract::TGranuleMetaView& granule) const override;
    void AppendStats(const std::vector<std::unique_ptr<arrow::ArrayBuilder>>& builders, const TPortionInfo& portion) const;
public:
    using TBase::TBase;
};

class TStoreSysViewPolicy: public NAbstract::ISysViewPolicy {
protected:
    virtual std::unique_ptr<IScannerConstructor> DoCreateConstructor(const TSnapshot& snapshot, const ui64 itemsLimit, const bool reverse) const override {
        return std::make_unique<TConstructor>(snapshot, itemsLimit, reverse);
    }
    virtual std::shared_ptr<NAbstract::IMetadataFiller> DoCreateMetadataFiller() const override {
        return std::make_shared<NAbstract::TMetadataFromStore>();
    }
public:
    static const inline TFactory::TRegistrator<TStoreSysViewPolicy> Registrator = TFactory::TRegistrator<TStoreSysViewPolicy>(TString(::NKikimr::NSysView::StorePrimaryIndexStatsName));

};

class TTableSysViewPolicy: public NAbstract::ISysViewPolicy {
protected:
    virtual std::unique_ptr<IScannerConstructor> DoCreateConstructor(const TSnapshot& snapshot, const ui64 itemsLimit, const bool reverse) const override {
        return std::make_unique<TConstructor>(snapshot, itemsLimit, reverse);
    }
    virtual std::shared_ptr<NAbstract::IMetadataFiller> DoCreateMetadataFiller() const override {
        return std::make_shared<NAbstract::TMetadataFromTable>();
    }
public:
    static const inline TFactory::TRegistrator<TTableSysViewPolicy> Registrator = TFactory::TRegistrator<TTableSysViewPolicy>(TString(::NKikimr::NSysView::TablePrimaryIndexStatsName));

};

}
