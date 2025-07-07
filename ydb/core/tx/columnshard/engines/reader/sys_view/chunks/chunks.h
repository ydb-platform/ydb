#pragma once
#include <ydb/core/formats/arrow/arrow_batch_builder.h>
#include <ydb/core/sys_view/common/schema.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/abstract.h>
#include <ydb/core/tx/columnshard/engines/reader/sys_view/abstract/iterator.h>
#include <ydb/core/tx/columnshard/engines/reader/sys_view/constructor/constructor.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/service.h>

namespace NKikimr::NOlap::NReader::NSysView::NChunks {

template <class TTableMetadata>
class TSchemaAdapter {};

class TBaseSchemaAdapter {
public:
    static ui64 GetPresetId() {
        static TAtomicCounter Counter = 0;
        static const ui64 result = Max<ui32>() - Counter.Inc();
        return result;
    }
};

template <>
class TSchemaAdapter<NKikimr::NSysView::Schema::PrimaryIndexStats>: public TBaseSchemaAdapter {
public:
    static NArrow::TSimpleRow GetPKSimpleRow(
        const NColumnShard::TUnifiedPathId pathId, const ui64 tabletId, const ui64 portionId, const ui32 entityId, const ui64 chunkIdx) {
        NArrow::TRecordBatchConstructor rbConstructor;
        {
            auto record = rbConstructor.InitColumns(GetPKSchema()).StartRecord();
            record.AddRecordValue(std::make_shared<arrow::UInt64Scalar>(pathId.SchemeShardLocalPathId.GetRawValue()))
                .AddRecordValue(std::make_shared<arrow::UInt64Scalar>(tabletId))
                .AddRecordValue(std::make_shared<arrow::UInt64Scalar>(portionId))
                .AddRecordValue(std::make_shared<arrow::UInt32Scalar>(entityId))
                .AddRecordValue(std::make_shared<arrow::UInt64Scalar>(chunkIdx));
        }
        return NArrow::TSimpleRow(rbConstructor.Finish().GetBatch(), 0);
    }

    static std::shared_ptr<arrow::Schema> GetPKSchema() {
        static std::shared_ptr<arrow::Schema> schema = []() {
            arrow::FieldVector fields = { std::make_shared<arrow::Field>("PathId", arrow::uint64()),
                std::make_shared<arrow::Field>("TabletId", arrow::uint64()), std::make_shared<arrow::Field>("PortionId", arrow::uint64()),
                std::make_shared<arrow::Field>("InternalEntityId", arrow::uint32()),
                std::make_shared<arrow::Field>("ChunkIdx", arrow::uint64()) };
            return std::make_shared<arrow::Schema>(std::move(fields));
        }();
        return schema;
    }

    static TIndexInfo GetIndexInfo(
        const std::shared_ptr<IStoragesManager>& storagesManager, const std::shared_ptr<TSchemaObjectsCache>& schemaObjectsCache) {
        static NKikimrSchemeOp::TColumnTableSchema proto = []() {
            NKikimrSchemeOp::TColumnTableSchema proto;
            ui32 currentId = 0;
            const auto pred = [&](const TString& name, const NScheme::TTypeId typeId, const std::optional<ui32> entityId = std::nullopt) {
                auto* col = proto.AddColumns();
                col->SetId(entityId.value_or(++currentId));
                col->SetName(name);
                col->SetTypeId(typeId);
            };
            pred("PathId", NScheme::NTypeIds::Uint64);
            pred("Kind", NScheme::NTypeIds::Utf8);
            pred("TabletId", NScheme::NTypeIds::Uint64);
            pred("Rows", NScheme::NTypeIds::Uint64);
            pred("RawBytes", NScheme::NTypeIds::Uint64);
            pred("PortionId", NScheme::NTypeIds::Uint64);
            pred("ChunkIdx", NScheme::NTypeIds::Uint64);
            pred("EntityName", NScheme::NTypeIds::Utf8);
            pred("InternalEntityId", NScheme::NTypeIds::Uint32);
            pred("BlobId", NScheme::NTypeIds::Utf8);
            pred("BlobRangeOffset", NScheme::NTypeIds::Uint64);
            pred("BlobRangeSize", NScheme::NTypeIds::Uint64);
            pred("Activity", NScheme::NTypeIds::Uint8);
            pred("TierName", NScheme::NTypeIds::Utf8);
            pred("EntityType", NScheme::NTypeIds::Utf8);
            proto.AddKeyColumnNames("PathId");
            proto.AddKeyColumnNames("TabletId");
            proto.AddKeyColumnNames("PortionId");
            proto.AddKeyColumnNames("InternalEntityId");
            proto.AddKeyColumnNames("ChunkIdx");
            return proto;
        }();

        auto indexInfo = TIndexInfo::BuildFromProto(GetPresetId(), proto, storagesManager, schemaObjectsCache);
        AFL_VERIFY(indexInfo);
        return std::move(*indexInfo);
    }
};

class TConstructor: public TStatScannerConstructor<NKikimr::NSysView::Schema::PrimaryIndexStats> {
private:
    using TBase = TStatScannerConstructor<NKikimr::NSysView::Schema::PrimaryIndexStats>;

protected:
    virtual std::shared_ptr<NAbstract::TReadStatsMetadata> BuildMetadata(
        const NColumnShard::TColumnShard* self, const TReadDescription& read) const override;

public:
    using TBase::TBase;
};

class TReadStatsMetadata: public NAbstract::TReadStatsMetadata {
private:
    using TBase = NAbstract::TReadStatsMetadata;
    using TSysViewSchema = NKikimr::NSysView::Schema::PrimaryIndexStats;

public:
    using TBase::TBase;

    virtual std::unique_ptr<TScanIteratorBase> StartScan(const std::shared_ptr<TReadContext>& readContext) const override;
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

    virtual bool IsReadyForBatch() const override;
    virtual bool AppendStats(
        const std::vector<std::unique_ptr<arrow::ArrayBuilder>>& builders, NAbstract::TGranuleMetaView& granule) const override;
    virtual ui32 PredictRecordsCount(const NAbstract::TGranuleMetaView& granule) const override;
    void AppendStats(const std::vector<std::unique_ptr<arrow::ArrayBuilder>>& builders,
        const NColumnShard::TSchemeShardLocalPathId schemshardLocalPathId, const TPortionDataAccessor& portion) const;

    virtual void Apply(const std::shared_ptr<IApplyAction>& task) override;

    virtual TConclusionStatus Start() override;

public:
    using TBase::TBase;
};

class TStoreSysViewPolicy: public NAbstract::ISysViewPolicy {
protected:
    virtual std::unique_ptr<IScannerConstructor> DoCreateConstructor(const TScannerConstructorContext& request) const override {
        return std::make_unique<TConstructor>(request);
    }
    virtual std::shared_ptr<NAbstract::IMetadataFiller> DoCreateMetadataFiller() const override {
        return std::make_shared<NAbstract::TMetadataFromStore>();
    }

public:
    static const inline TFactory::TRegistrator<TStoreSysViewPolicy> Registrator =
        TFactory::TRegistrator<TStoreSysViewPolicy>(TString(::NKikimr::NSysView::StorePrimaryIndexStatsName));
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
    static const inline TFactory::TRegistrator<TTableSysViewPolicy> Registrator =
        TFactory::TRegistrator<TTableSysViewPolicy>(TString(::NKikimr::NSysView::TablePrimaryIndexStatsName));
};

}   // namespace NKikimr::NOlap::NReader::NSysView::NChunks
