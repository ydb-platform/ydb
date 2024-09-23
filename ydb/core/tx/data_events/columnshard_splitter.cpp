#include "columnshard_splitter.h"

namespace NKikimr::NEvWrite {

NKikimr::NEvWrite::IShardsSplitter::TYdbConclusionStatus TColumnShardShardsSplitter::DoSplitData(const NSchemeCache::TSchemeCacheNavigate::TEntry& schemeEntry, const IEvWriteDataAccessor& data) {
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

    std::shared_ptr<arrow::RecordBatch> batch;
    if (data.HasDeserializedBatch()) {
        batch = data.GetDeserializedBatch();
    } else {
        std::shared_ptr<arrow::Schema> arrowScheme = ExtractArrowSchema(scheme);
        batch = NArrow::DeserializeBatch(data.GetSerializedData(), arrowScheme);
        if (!batch) {
            return TYdbConclusionStatus::Fail(Ydb::StatusIds::SCHEME_ERROR, TString("cannot deserialize batch with schema ") + arrowScheme->ToString());
        }

        auto res = batch->ValidateFull();
        if (!res.ok()) {
            return TYdbConclusionStatus::Fail(Ydb::StatusIds::SCHEME_ERROR, TString("deserialize batch is not valid: ") + res.ToString());
        }
    }

    NSchemeShard::TOlapSchema olapSchema;
    olapSchema.ParseFromLocalDB(scheme);
    auto shardingConclusion = NSharding::IShardingBase::BuildFromProto(olapSchema, sharding);
    if (shardingConclusion.IsFail()) {
        return TYdbConclusionStatus::Fail(Ydb::StatusIds::SCHEME_ERROR, shardingConclusion.GetErrorMessage());
    }
    AFL_VERIFY(!!shardingConclusion.GetResult());
    return SplitImpl(batch, shardingConclusion.DetachResult());
}

NKikimr::NEvWrite::IShardsSplitter::TYdbConclusionStatus TColumnShardShardsSplitter::SplitImpl(const std::shared_ptr<arrow::RecordBatch>& batch,
    const std::shared_ptr<NSharding::IShardingBase>& sharding)
{
    Y_ABORT_UNLESS(batch);

    auto split = sharding->SplitByShards(batch, NColumnShard::TLimits::GetMaxBlobSize() * 0.875);
    if (split.IsFail()) {
        return TYdbConclusionStatus::Fail(Ydb::StatusIds::SCHEME_ERROR, split.GetErrorMessage());
    }

    TFullSplitData result(sharding->GetShardsCount());
    for (auto&& [shardId, chunks] : split.GetResult()) {
        for (auto&& c : chunks) {
            result.AddShardInfo(shardId, std::make_shared<TShardInfo>(c.GetSchemaData(), c.GetData(), c.GetRowsCount(), sharding->GetShardInfoVerified(shardId).GetShardingVersion()));
        }
    }

    Y_ABORT_UNLESS(result.GetShardsInfo().size());
    FullSplitData = result;
    return TYdbConclusionStatus::Success();
}

std::shared_ptr<arrow::Schema> TColumnShardShardsSplitter::ExtractArrowSchema(const NKikimrSchemeOp::TColumnTableSchema& schema) {
    TVector<std::pair<TString, NScheme::TTypeInfo>> columns;
    for (auto& col : schema.GetColumns()) {
        Y_ABORT_UNLESS(col.HasTypeId());
        auto typeInfoMod = NScheme::TypeInfoModFromProtoColumnType(col.GetTypeId(), col.HasTypeInfo() ? &col.GetTypeInfo() : nullptr);
        columns.emplace_back(col.GetName(), typeInfoMod.TypeInfo);
    }
    return NArrow::TStatusValidator::GetValid(NArrow::MakeArrowSchema(columns));
}

}
