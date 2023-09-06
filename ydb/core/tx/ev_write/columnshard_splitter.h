#pragma once

#include "shards_splitter.h"

#include <ydb/core/tx/sharding/sharding.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/formats/arrow/size_calcer.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>


namespace NKikimr::NEvWrite {

class TColumnShardShardsSplitter : public IShardsSplitter {
    class TShardInfo : public IShardInfo {
    private:
        const TString SchemaData;
        const TString Data;
        const ui32 RowsCount;
    public:
        TShardInfo(const TString& schemaData, const TString& data, const ui32 rowsCount)
            : SchemaData(schemaData)
            , Data(data)
            , RowsCount(rowsCount)
        {}

        ui64 GetBytes() const override {
            return Data.size();
        }

        ui32 GetRowsCount() const override {
            return RowsCount;
        }

        void Serialize(TEvWrite& evWrite) const override {
            evWrite.SetArrowData(SchemaData, Data);
        }
    };

private:
    TYdbConclusionStatus DoSplitData(const NSchemeCache::TSchemeCacheNavigate::TEntry& schemeEntry, const IEvWriteDataAccessor& data) override {
        if (schemeEntry.Kind != NSchemeCache::TSchemeCacheNavigate::KindColumnTable) {
            return TYdbConclusionStatus::Fail(Ydb::StatusIds::SCHEME_ERROR, "The specified path is not an column table");
        }

        if (!schemeEntry.ColumnTableInfo) {
            return TYdbConclusionStatus::Fail(Ydb::StatusIds::SCHEME_ERROR, "Column table expected");
        }

        const auto& description = schemeEntry.ColumnTableInfo->Description;
        if (!description.HasSharding()) {
            return TYdbConclusionStatus::Fail(Ydb::StatusIds::SCHEME_ERROR, "Unknown sharding method is not supported");
        }
        if (!description.HasSchema()) {
            return TYdbConclusionStatus::Fail(Ydb::StatusIds::SCHEME_ERROR, "Unknown schema for column table");
        }

        const auto& scheme = description.GetSchema();
        const auto& sharding = description.GetSharding();

        if (sharding.ColumnShardsSize() == 0) {
            return TYdbConclusionStatus::Fail(Ydb::StatusIds::SCHEME_ERROR, "No shards to write to");
        }
        if (!scheme.HasEngine() || scheme.GetEngine() == NKikimrSchemeOp::COLUMN_ENGINE_NONE) {
            return TYdbConclusionStatus::Fail(Ydb::StatusIds::SCHEME_ERROR, "Wrong column table configuration");
        }

        if (sharding.HasRandomSharding()) {
            return SplitRandom(data, scheme, sharding);
        } else if (sharding.HasHashSharding()) {
            return SplitByHash(data, scheme, sharding);
        }
        return TYdbConclusionStatus::Fail(Ydb::StatusIds::SCHEME_ERROR, "Sharding method is not supported");
    }

private:
    TYdbConclusionStatus SplitByHash(const IEvWriteDataAccessor& data, const NKikimrSchemeOp::TColumnTableSchema& scheme, const NKikimrSchemeOp::TColumnTableSharding& sharding) {
        if (scheme.GetEngine() != NKikimrSchemeOp::COLUMN_ENGINE_REPLACING_TIMESERIES) {
            return TYdbConclusionStatus::Fail(Ydb::StatusIds::SCHEME_ERROR, "Wrong column table configuration fro hash sharding");
        }

        if (data.HasDeserializedBatch()) {
            return SplitByHashImpl(data.GetDeserializedBatch(), sharding);
        }

        std::shared_ptr<arrow::Schema> arrowScheme = ExtractArrowSchema(scheme);
        std::shared_ptr<arrow::RecordBatch> batch = NArrow::DeserializeBatch(data.GetSerializedData(), arrowScheme);
        if (!batch) {
            return TYdbConclusionStatus::Fail(Ydb::StatusIds::SCHEME_ERROR, TString("cannot deserialize batch with schema ") + arrowScheme->ToString());
        }

        auto res = batch->ValidateFull();
        if (!res.ok()) {
            return TYdbConclusionStatus::Fail(Ydb::StatusIds::SCHEME_ERROR, TString("deserialize batch is not valid: ") + res.ToString());
        }
        return SplitByHashImpl(batch, sharding);
    }

    TYdbConclusionStatus SplitRandom(const IEvWriteDataAccessor& data, const NKikimrSchemeOp::TColumnTableSchema& scheme, const NKikimrSchemeOp::TColumnTableSharding& sharding) {
        const ui64 shardId = sharding.GetColumnShards(RandomNumber<ui32>(sharding.ColumnShardsSize()));
        FullSplitData.emplace(sharding.ColumnShardsSize());
        if (data.HasDeserializedBatch()) {
            FullSplitData->AddShardInfo(shardId, std::make_shared<TShardInfo>(NArrow::SerializeSchema(*data.GetDeserializedBatch()->schema()), data.GetSerializedData(), 0));
        } else {
            FullSplitData->AddShardInfo(shardId, std::make_shared<TShardInfo>(NArrow::SerializeSchema(*ExtractArrowSchema(scheme)), data.GetSerializedData(), 0));
        }
        return TYdbConclusionStatus::Success();
    }

    TYdbConclusionStatus SplitByHashImpl(const std::shared_ptr<arrow::RecordBatch>& batch, const NKikimrSchemeOp::TColumnTableSharding& descSharding) {
        Y_VERIFY(descSharding.HasHashSharding());
        Y_VERIFY(batch);

        TVector<ui64> tabletIds(descSharding.GetColumnShards().begin(), descSharding.GetColumnShards().end());
        const ui32 numShards = tabletIds.size();
        Y_VERIFY(numShards);

        TFullSplitData result(numShards);
        if (numShards == 1) {
            NArrow::TSplitBlobResult blobsSplitted = NArrow::SplitByBlobSize(batch, NColumnShard::TLimits::GetMaxBlobSize() * 0.875);
            if (!blobsSplitted) {
                return TYdbConclusionStatus::Fail(Ydb::StatusIds::SCHEME_ERROR, "cannot split batch in according to limits: " + blobsSplitted.GetErrorMessage());
            }
            for (auto&& b : blobsSplitted.GetResult()) {
                result.AddShardInfo(tabletIds.front(), std::make_shared<TShardInfo>(b.GetSchemaData(), b.GetData(), b.GetRowsCount()));
            }
            FullSplitData = result;
            return TYdbConclusionStatus::Success();
        }

        auto sharding = NSharding::TShardingBase::BuildShardingOperator(descSharding);
        std::vector<ui32> rowSharding;
        if (sharding) {
            rowSharding = sharding->MakeSharding(batch);
        }
        if (rowSharding.empty()) {
            return TYdbConclusionStatus::Fail(Ydb::StatusIds::SCHEME_ERROR, "empty "
                + NKikimrSchemeOp::TColumnTableSharding::THashSharding::EHashFunction_Name(descSharding.GetHashSharding().GetFunction())
                + " sharding (" + (sharding ? sharding->DebugString() : "no sharding object") + ")");
        }

        std::vector<std::shared_ptr<arrow::RecordBatch>> sharded = NArrow::ShardingSplit(batch, rowSharding, numShards);
        Y_VERIFY(sharded.size() == numShards);

        THashMap<ui64, TString> out;
        for (size_t i = 0; i < sharded.size(); ++i) {
            if (sharded[i]) {
                NArrow::TSplitBlobResult blobsSplitted = NArrow::SplitByBlobSize(sharded[i], NColumnShard::TLimits::GetMaxBlobSize() * 0.875);
                if (!blobsSplitted) {
                    return TYdbConclusionStatus::Fail(Ydb::StatusIds::SCHEME_ERROR, "cannot split batch in according to limits: " + blobsSplitted.GetErrorMessage());
                }
                for (auto&& b : blobsSplitted.GetResult()) {
                    result.AddShardInfo(tabletIds[i], std::make_shared<TShardInfo>(b.GetSchemaData(), b.GetData(), b.GetRowsCount()));
                }
            }
        }

        Y_VERIFY(result.GetShardsInfo().size());
        FullSplitData = result;
        return TYdbConclusionStatus::Success();
    }

    std::shared_ptr<arrow::Schema> ExtractArrowSchema(const NKikimrSchemeOp::TColumnTableSchema& schema) {
        TVector<std::pair<TString, NScheme::TTypeInfo>> columns;
        for (auto& col : schema.GetColumns()) {
            Y_VERIFY(col.HasTypeId());
            auto typeInfoMod = NScheme::TypeInfoModFromProtoColumnType(col.GetTypeId(),
                col.HasTypeInfo() ? &col.GetTypeInfo() : nullptr);
            columns.emplace_back(col.GetName(), typeInfoMod.TypeInfo);
        }
        return NArrow::MakeArrowSchema(columns);
    }
};
}
