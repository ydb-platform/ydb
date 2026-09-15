#include "db_wrapper.h"
#include "defs.h"

#include "portions/constructor_portion.h"

#include <ydb/core/protos/config.pb.h>
#include <ydb/core/scheme_types/scheme_type_info.h>
#include <ydb/core/tablet_flat/flat_database.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/sharding/sharding.h>

namespace NKikimr::NOlap {
namespace {

std::pair<std::unique_ptr<NOlap::TPortionInfoConstructor>, NKikimrTxColumnShard::TIndexPortionMeta> MakePortionInfoConstructor(
    const auto& rowset) {
    AFL_VERIFY(rowset.IsReady());
    AFL_VERIFY(!rowset.EndOfSet());

    using IndexPortions = NColumnShard::Schema::IndexPortions;
    std::unique_ptr<NOlap::TPortionInfoConstructor> portion;
    NKikimrTxColumnShard::TIndexPortionMeta metaProto;
    const TString metadata = rowset.template GetValue<IndexPortions::Metadata>();
    AFL_VERIFY(metaProto.ParseFromArray(metadata.data(), metadata.size()))("event", "cannot parse metadata as protobuf");

    if (rowset.template GetValueOrDefault<IndexPortions::InsertWriteId>(0)) {
        auto portionImpl =
            std::make_unique<TWrittenPortionInfoConstructor>(TInternalPathId::FromRawValue(rowset.template GetValue<IndexPortions::PathId>()),
                rowset.template GetValue<IndexPortions::PortionId>());
        portionImpl->SetInsertWriteId((TInsertWriteId)rowset.template GetValue<IndexPortions::InsertWriteId>());
        if (rowset.template GetValueOrDefault<IndexPortions::CommitPlanStep>(0)) {
            portionImpl->SetCommitSnapshot(
                TSnapshot(rowset.template GetValue<IndexPortions::CommitPlanStep>(), rowset.template GetValue<IndexPortions::CommitTxId>()));
        } else {
            AFL_VERIFY(!rowset.template GetValueOrDefault<IndexPortions::CommitTxId>(0));
        }
        portion.reset(portionImpl.release());
    } else {
        AFL_VERIFY(metaProto.HasCompactedPortion());
        AFL_VERIFY(metaProto.GetCompactedPortion().HasAppearanceSnapshot());
        auto cPortion =
            std::make_unique<TCompactedPortionInfoConstructor>(TInternalPathId::FromRawValue(rowset.template GetValue<IndexPortions::PathId>()),
                rowset.template GetValue<IndexPortions::PortionId>());
        TSnapshot snapshot = TSnapshot::Zero();
        snapshot.DeserializeFromProto(metaProto.GetCompactedPortion().GetAppearanceSnapshot()).Validate();
        cPortion->SetAppearanceSnapshot(snapshot);
        portion = std::move(cPortion);
    }
    portion->SetSchemaVersion(rowset.template GetValue<IndexPortions::SchemaVersion>());
    if (rowset.template HaveValue<IndexPortions::ShardingVersion>() && rowset.template GetValue<IndexPortions::ShardingVersion>()) {
        portion->SetShardingVersion(rowset.template GetValue<IndexPortions::ShardingVersion>());
    }
    portion->SetRemoveSnapshot(rowset.template GetValue<IndexPortions::XPlanStep>(), rowset.template GetValue<IndexPortions::XTxId>());
    return std::make_pair(std::move(portion), std::move(metaProto));
}

class TRawRowAdapter {
public:
    TRawRowAdapter(const NTable::TRowState& row, NTable::TTagsRef tags)
        : Row_(row)
        , Tags_(tags)
    {
    }

    bool IsReady() const {
        return true;
    }

    bool EndOfSet() const {
        return false;
    }

    template <typename ColumnType>
    typename ColumnType::Type GetValue() const {
        return CellValue<ColumnType>(Row_.Get(FindIdx(ColumnType::ColumnId)));
    }

    template <typename ColumnType>
    typename ColumnType::Type GetValueOrDefault(typename ColumnType::Type defaultValue = {}) const {
        const auto& cell = Row_.Get(FindIdx(ColumnType::ColumnId));
        if (cell.IsNull()) {
            return defaultValue;
        }
        return CellValue<ColumnType>(cell);
    }

    template <typename ColumnType>
    bool HaveValue() const {
        return !Row_.Get(FindIdx(ColumnType::ColumnId)).IsNull();
    }

private:
    size_t FindIdx(NTable::TTag tag) const {
        for (size_t i = 0; i < Tags_.size(); ++i) {
            if (Tags_[i] == tag) {
                return i;
            }
        }
        Y_ABORT("tag %u not in row adapter", (unsigned)tag);
    }

    template <typename ColumnType>
    static typename ColumnType::Type CellValue(const TCell& cell) {
        if constexpr (std::is_same_v<typename ColumnType::Type, TString>) {
            return TString(cell.Data(), cell.Size());
        } else {
            return cell.AsValue<typename ColumnType::Type>();
        }
    }

    const NTable::TRowState& Row_;
    NTable::TTagsRef Tags_;
};

}   // namespace

void TDbWrapper::WriteColumn(
    const TPortionDataAccessor& acc, const NOlap::TPortionInfo& portion, const TColumnRecord& row, const ui32 firstPKColumnId) {
    if (!AppDataVerified().ColumnShardConfig.GetColumnChunksV1Usage() && !AppDataVerified().ColumnShardConfig.GetColumnChunksV0Usage()) {
        return;
    }
    NIceDb::TNiceDb db(Database);
    using IndexColumnsV1 = NColumnShard::Schema::IndexColumnsV1;
    auto rowProto = row.GetMeta().SerializeToProto();
    if (AppDataVerified().ColumnShardConfig.GetColumnChunksV1Usage()) {
        db.Table<IndexColumnsV1>()
            .Key(portion.GetPathId().GetRawValue(), portion.GetPortionId(), row.ColumnId, row.Chunk)
            .Update(NIceDb::TUpdate<IndexColumnsV1::BlobIdx>(row.GetBlobRange().GetBlobIdxVerified()),
                NIceDb::TUpdate<IndexColumnsV1::Metadata>(rowProto.SerializeAsString()),
                NIceDb::TUpdate<IndexColumnsV1::Offset>(row.BlobRange.Offset), NIceDb::TUpdate<IndexColumnsV1::Size>(row.BlobRange.Size));
    }
    if (AppDataVerified().ColumnShardConfig.GetColumnChunksV0Usage()) {
        if (row.GetChunkIdx() == 0 && row.GetColumnId() == firstPKColumnId) {
            *rowProto.MutablePortionMeta() = portion.GetMeta().SerializeToProto(acc.GetBlobIds(),
                portion.GetPortionType() == EPortionType::Compacted ? NPortion::EProduced::SPLIT_COMPACTED : NPortion::EProduced::INSERTED);
        }
        using IndexColumns = NColumnShard::Schema::IndexColumns;
        auto removeSnapshot = portion.GetRemoveSnapshotOptional();
        db.Table<IndexColumns>()
            .Key(0, 0, row.ColumnId, 1, 1, portion.GetPortionId(), row.Chunk)
            .Update(NIceDb::TUpdate<IndexColumns::XPlanStep>(removeSnapshot ? removeSnapshot->GetPlanStep() : 0),
                NIceDb::TUpdate<IndexColumns::XTxId>(removeSnapshot ? removeSnapshot->GetTxId() : 0),
                NIceDb::TUpdate<IndexColumns::Blob>(acc.GetBlobId(row.GetBlobRange().GetBlobIdxVerified()).SerializeBinary()),
                NIceDb::TUpdate<IndexColumns::BlobIdx>(row.GetBlobRange().GetBlobIdxVerified()),
                NIceDb::TUpdate<IndexColumns::Metadata>(rowProto.SerializeAsString()),
                NIceDb::TUpdate<IndexColumns::Offset>(row.BlobRange.Offset), NIceDb::TUpdate<IndexColumns::Size>(row.BlobRange.Size),
                NIceDb::TUpdate<IndexColumns::PathId>(portion.GetPathId().GetRawValue()));
    }
}

void TDbWrapper::WritePortion(const std::vector<TUnifiedBlobId>& blobIds, const NOlap::TPortionInfo& portion) {
    NIceDb::TNiceDb db(Database);
    portion.SaveMetaToDatabase(blobIds, db);
}

void TDbWrapper::CommitPortion(const NOlap::TPortionInfo& portion, const TSnapshot& commitSnapshot) {
    NIceDb::TNiceDb db(Database);
    using IndexPortions = NColumnShard::Schema::IndexPortions;
    if (portion.HasRemoveSnapshot()) {
        db.Table<IndexPortions>()
            .Key(portion.GetPathId().GetRawValue(), portion.GetPortionId())
            .Update(NIceDb::TUpdate<IndexPortions::CommitPlanStep>(commitSnapshot.GetPlanStep()),
                NIceDb::TUpdate<IndexPortions::CommitTxId>(commitSnapshot.GetTxId()),
                NIceDb::TUpdate<IndexPortions::XPlanStep>(portion.GetRemoveSnapshotVerified().GetPlanStep()),
                NIceDb::TUpdate<IndexPortions::XTxId>(portion.GetRemoveSnapshotVerified().GetTxId()));
    } else {
        db.Table<IndexPortions>()
            .Key(portion.GetPathId().GetRawValue(), portion.GetPortionId())
            .Update(NIceDb::TUpdate<IndexPortions::CommitPlanStep>(commitSnapshot.GetPlanStep()),
                NIceDb::TUpdate<IndexPortions::CommitTxId>(commitSnapshot.GetTxId()));
    }
}

void TDbWrapper::ErasePortion(const NOlap::TPortionInfo& portion) {
    NIceDb::TNiceDb db(Database);
    db.Table<NColumnShard::Schema::IndexPortions>().Key(portion.GetPathId().GetRawValue(), portion.GetPortionId()).Delete();
    db.Table<NColumnShard::Schema::IndexColumnsV2>().Key(portion.GetPathId().GetRawValue(), portion.GetPortionId()).Delete();
}

void TDbWrapper::EraseColumn(const NOlap::TPortionInfo& portion, const TColumnRecord& row) {
    NIceDb::TNiceDb db(Database);
    if (AppDataVerified().ColumnShardConfig.GetColumnChunksV1Usage()) {
        using IndexColumnsV1 = NColumnShard::Schema::IndexColumnsV1;
        db.Table<IndexColumnsV1>().Key(portion.GetPathId().GetRawValue(), portion.GetPortionId(), row.ColumnId, row.Chunk).Delete();
    }
    if (AppDataVerified().ColumnShardConfig.GetColumnChunksV0Usage()) {
        using IndexColumns = NColumnShard::Schema::IndexColumns;
        db.Table<IndexColumns>().Key(0, 0, row.ColumnId, 1, 1, portion.GetPortionId(), row.Chunk).Delete();
    }
}

bool TDbWrapper::LoadColumns(const std::function<void(TColumnChunkLoadContextV2&&)>& callback, const std::optional<TInternalPathId> prefixPathId,
    const std::optional<ui64> fromPortionId, const std::optional<TInternalPathId> toPathId, const std::optional<ui64> toPortionId) {
    NIceDb::TNiceDb db(Database);
    using IndexColumnsV2 = NColumnShard::Schema::IndexColumnsV2;
    const auto pred = [&](auto& rowset) {
        if (!rowset.IsReady()) {
            return false;
        }

        while (!rowset.EndOfSet()) {
            AFL_VERIFY(DsGroupSelector);
            NOlap::TColumnChunkLoadContextV2 chunkLoadContext(rowset, *DsGroupSelector);
            callback(std::move(chunkLoadContext));

            if (!rowset.Next()) {
                return false;
            }
        }
        return true;
    };
    if (toPathId && toPortionId && prefixPathId && fromPortionId) {
        auto rowset = db.Table<IndexColumnsV2>()
                          .GreaterOrEqual(prefixPathId->GetRawValue(), fromPortionId.value())
                          .LessOrEqual(toPathId->GetRawValue(), toPortionId.value())
                          .Select();
        return pred(rowset);
    } else if (prefixPathId && fromPortionId) {
        auto rowset = db.Table<IndexColumnsV2>().GreaterOrEqual(prefixPathId->GetRawValue(), fromPortionId.value()).Select();
        return pred(rowset);
    } else if (prefixPathId) {
        auto rowset = db.Table<IndexColumnsV2>().Prefix(prefixPathId->GetRawValue()).Select();
        return pred(rowset);
    } else {
        auto rowset = db.Table<IndexColumnsV2>().Select();
        return pred(rowset);
    }
}

bool TDbWrapper::LoadPortions(
    const std::function<bool(std::unique_ptr<NOlap::TPortionInfoConstructor>&&, const NKikimrTxColumnShard::TIndexPortionMeta&)>& callback,
    const std::optional<TInternalPathId> pathId, const std::optional<ui64> portionId) {
    NIceDb::TNiceDb db(Database);
    using IndexPortions = NColumnShard::Schema::IndexPortions;
    const auto pred = [&](auto& rowset) {
        if (!rowset.IsReady()) {
            return false;
        }

        while (!rowset.EndOfSet()) {
            auto [portion, metaProto] = MakePortionInfoConstructor(rowset);
            if (!callback(std::move(portion), metaProto)) {
                return false;
            }
            if (!rowset.Next()) {
                return false;
            }
        }
        return true;
    };
    if (pathId && portionId) {
        auto rowset = db.Table<IndexPortions>().GreaterOrEqual(pathId->GetRawValue(), portionId.value()).Select();
        return pred(rowset);
    } else if (pathId) {
        auto rowset = db.Table<IndexPortions>().Prefix(pathId->GetRawValue()).Select();
        return pred(rowset);
    } else {
        auto rowset = db.Table<IndexPortions>().Select();
        return pred(rowset);
    }
}

void TDbWrapper::WriteIndex(const TPortionDataAccessor& acc, const TPortionInfo& portion, const TIndexChunk& row) {
    using IndexIndexes = NColumnShard::Schema::IndexIndexes;
    NIceDb::TNiceDb db(Database);
    if (auto bRange = row.GetBlobRangeOptional()) {
        AFL_VERIFY(bRange->IsValid());
        db.Table<IndexIndexes>()
            .Key(portion.GetPathId().GetRawValue(), portion.GetPortionId(), row.GetIndexId(), row.GetChunkIdx())
            .Update(NIceDb::TUpdate<IndexIndexes::Blob>(acc.GetBlobId(bRange->GetBlobIdxVerified()).SerializeBinary()),
                NIceDb::TUpdate<IndexIndexes::BlobIdx>(bRange->GetBlobIdxVerified()), NIceDb::TUpdate<IndexIndexes::Offset>(bRange->Offset),
                NIceDb::TUpdate<IndexIndexes::Size>(row.GetDataSize()), NIceDb::TUpdate<IndexIndexes::RecordsCount>(row.GetRecordsCount()),
                NIceDb::TUpdate<IndexIndexes::RawBytes>(row.GetRawBytes()));
    } else if (auto bData = row.GetBlobDataOptional()) {
        db.Table<IndexIndexes>()
            .Key(portion.GetPathId().GetRawValue(), portion.GetPortionId(), row.GetIndexId(), row.GetChunkIdx())
            .Update(NIceDb::TUpdate<IndexIndexes::BlobData>(*bData), NIceDb::TUpdate<IndexIndexes::RecordsCount>(row.GetRecordsCount()),
                NIceDb::TUpdate<IndexIndexes::RawBytes>(row.GetRawBytes()));
    } else {
        AFL_VERIFY(false);
    }
}

void TDbWrapper::EraseIndex(const TPortionInfo& portion, const TIndexChunk& row) {
    NIceDb::TNiceDb db(Database);
    using IndexIndexes = NColumnShard::Schema::IndexIndexes;
    db.Table<IndexIndexes>().Key(portion.GetPathId().GetRawValue(), portion.GetPortionId(), row.GetIndexId(), row.GetChunkIdx()).Delete();
}

bool TDbWrapper::LoadIndexes(const std::function<void(const TInternalPathId pathId, const ui64 portionId, TIndexChunkLoadContext&&)>& callback,
    const std::optional<TInternalPathId> prefixPathId, const std::optional<ui64> prefixPortionId, const std::optional<TInternalPathId> toPathId,
    const std::optional<ui64> toPortionId) {
    NIceDb::TNiceDb db(Database);
    using IndexIndexes = NColumnShard::Schema::IndexIndexes;
    const auto pred = [&](auto& rowset) {
        if (!rowset.IsReady()) {
            return false;
        }

        while (!rowset.EndOfSet()) {
            NOlap::TIndexChunkLoadContext chunkLoadContext(rowset, DsGroupSelector);
            callback(TInternalPathId::FromRawValue(rowset.template GetValue<IndexIndexes::PathId>()),
                rowset.template GetValue<IndexIndexes::PortionId>(), std::move(chunkLoadContext));

            if (!rowset.Next()) {
                return false;
            }
        }
        return true;
    };
    if (toPathId && toPortionId && prefixPathId && prefixPortionId) {
        auto rowset = db.Table<IndexIndexes>()
                          .GreaterOrEqual(prefixPathId->GetRawValue(), prefixPortionId.value())
                          .LessOrEqual(toPathId->GetRawValue(), toPortionId.value())
                          .Select();
        return pred(rowset);
    } else if (prefixPathId && prefixPortionId) {
        auto rowset = db.Table<IndexIndexes>().Prefix(prefixPathId->GetRawValue(), prefixPortionId.value()).Select();
        return pred(rowset);
    } else if (prefixPathId) {
        auto rowset = db.Table<IndexIndexes>().Prefix(prefixPathId->GetRawValue()).Select();
        return pred(rowset);
    } else {
        auto rowset = db.Table<IndexIndexes>().Select();
        return pred(rowset);
    }
}

void TDbWrapper::WriteCounter(ui32 counterId, ui64 value) {
    NIceDb::TNiceDb db(Database);
    return NColumnShard::Schema::IndexCounters_Write(db, counterId, value);
}

bool TDbWrapper::LoadCounters(const std::function<void(ui32 id, ui64 value)>& callback) {
    NIceDb::TNiceDb db(Database);
    return NColumnShard::Schema::IndexCounters_Load(db, callback);
}

TConclusion<THashMap<TInternalPathId, std::map<NOlap::TSnapshot, TGranuleShardingInfo>>> TDbWrapper::LoadGranulesShardingInfo() {
    using Schema = NColumnShard::Schema;
    NIceDb::TNiceDb db(Database);
    auto rowset = db.Table<Schema::ShardingInfo>().Select();
    if (!rowset.IsReady()) {
        return TConclusionStatus::Fail("cannot read rowset");
    }
    THashMap<TInternalPathId, std::map<TSnapshot, TGranuleShardingInfo>> result;
    while (!rowset.EndOfSet()) {
        NOlap::TSnapshot snapshot = NOlap::TSnapshot::Zero();
        snapshot.DeserializeFromString(rowset.GetValue<Schema::ShardingInfo::Snapshot>()).Validate();
        NSharding::TGranuleShardingLogicContainer logic;
        logic.DeserializeFromString(rowset.GetValue<Schema::ShardingInfo::Logic>()).Validate();
        TGranuleShardingInfo gShardingInfo(logic, snapshot, rowset.GetValue<Schema::ShardingInfo::VersionId>(),
            TInternalPathId::FromRawValue(rowset.GetValue<Schema::ShardingInfo::PathId>()));
        AFL_VERIFY(result[gShardingInfo.GetPathId()].emplace(gShardingInfo.GetSinceSnapshot(), gShardingInfo).second);

        if (!rowset.Next()) {
            return TConclusionStatus::Fail("cannot read rowset");
        }
    }
    return result;
}

void TDbWrapper::WriteColumns(const NOlap::TPortionInfo& portion, const NKikimrTxColumnShard::TIndexPortionAccessor& proto,
    const NKikimrTxColumnShard::TIndexPortionBlobsInfo& protoBlobs) {
    NIceDb::TNiceDb db(Database);
    using IndexColumnsV2 = NColumnShard::Schema::IndexColumnsV2;
    db.Table<IndexColumnsV2>()
        .Key(portion.GetPathId().GetRawValue(), portion.GetPortionId())
        .Update(NIceDb::TUpdate<IndexColumnsV2::Metadata>(proto.SerializeAsString()))
        .Update(NIceDb::TUpdate<IndexColumnsV2::BlobIds>(protoBlobs.SerializeAsString()));
}

TSeedingBatchResult TDbWrapper::LoadPortionsSeeding(std::pair<TInternalPathId, ui64> startKey, ui64 maxRows, ui64 bytesLimit,
    const std::function<bool(std::unique_ptr<NOlap::TPortionInfoConstructor>&&, const NKikimrTxColumnShard::TIndexPortionMeta&)>& callback)
{
    AFL_VERIFY(bytesLimit > 0)("event", "seeding loader called with zero bytesLimit");
    using IndexPortions = NColumnShard::Schema::IndexPortions;
    static const auto& kTagIds = IndexPortions::Columns<IndexPortions::TColumns>::GetColumnIds();
    NTable::TTagsRef tags(kTagIds.data(), kTagIds.size());
    ui64 startPathId = startKey.first.GetRawValue();
    ui64 startPortionId = startKey.second;
    const NKikimr::TRawTypeValue minKeyArr[2] = {
        { &startPathId, sizeof(ui64), NScheme::NTypeIds::Uint64 },
        { &startPortionId, sizeof(ui64), NScheme::NTypeIds::Uint64 },
    };
    NTable::TRawVals minKey(minKeyArr, 2);
    auto precharge = Database.Precharge(IndexPortions::TableId, minKey, {}, tags, 0, maxRows, bytesLimit);
    if (!precharge.Ready) {
        return TSeedingBatchResult{ false, precharge.BytesPrecharged, TConclusionStatus::Success(), {} };
    }
    NTable::TKeyRange range;
    range.MinKey = minKey;
    auto it = Database.IterateRange(IndexPortions::TableId, range, tags);
    ui64 rowsRead = 0;
    std::optional<std::pair<TInternalPathId, ui64>> lastKey;
    bool endOfRange = false;
    while (rowsRead < maxRows) {
        auto ready = it->Next(NTable::ENext::Data);
        if (ready == NTable::EReady::Page) {
            return TSeedingBatchResult{ false, precharge.BytesPrecharged, TConclusionStatus::Success(), lastKey };
        }
        if (ready == NTable::EReady::Gone) {
            endOfRange = true;
            break;
        }
        TRawRowAdapter adapter(it->Row(), tags);
        auto pathId = TInternalPathId::FromRawValue(adapter.GetValue<IndexPortions::PathId>());
        auto portionId = adapter.GetValue<IndexPortions::PortionId>();
        lastKey = { pathId, portionId };
        auto [portion, meta] = MakePortionInfoConstructor(adapter);
        ++rowsRead;
        if (!callback(std::move(portion), meta)) {
            break;
        }
    }
    TSeedingBatchResult result{ true, precharge.BytesPrecharged, TConclusionStatus::Success(), lastKey };
    result.EndOfRange = endOfRange;
    return result;
}

TSeedingBatchResult TDbWrapper::LoadColumnsSeeding(std::pair<TInternalPathId, ui64> startKey, std::pair<TInternalPathId, ui64> endKey,
    ui64 bytesLimit, const std::function<void(TColumnChunkLoadContextV2&&)>& callback)
{
    AFL_VERIFY(bytesLimit > 0)("event", "seeding loader called with zero bytesLimit");
    using IndexColumnsV2 = NColumnShard::Schema::IndexColumnsV2;
    static const auto& kTagIds = IndexColumnsV2::Columns<IndexColumnsV2::TColumns>::GetColumnIds();
    NTable::TTagsRef tags(kTagIds.data(), kTagIds.size());
    ui64 startPathId = startKey.first.GetRawValue();
    ui64 startPortionId = startKey.second;
    ui64 endPathId = endKey.first.GetRawValue();
    ui64 endPortionId = endKey.second;
    const NKikimr::TRawTypeValue minKeyArr[2] = {
        { &startPathId, sizeof(ui64), NScheme::NTypeIds::Uint64 },
        { &startPortionId, sizeof(ui64), NScheme::NTypeIds::Uint64 },
    };
    const NKikimr::TRawTypeValue maxKeyArr[2] = {
        { &endPathId, sizeof(ui64), NScheme::NTypeIds::Uint64 },
        { &endPortionId, sizeof(ui64), NScheme::NTypeIds::Uint64 },
    };
    NTable::TRawVals minKey(minKeyArr, 2);
    NTable::TRawVals maxKey(maxKeyArr, 2);
    auto precharge = Database.Precharge(IndexColumnsV2::TableId, minKey, maxKey, tags, 0, 0, bytesLimit);
    if (!precharge.Ready) {
        return TSeedingBatchResult{ false, precharge.BytesPrecharged, TConclusionStatus::Success(), {} };
    }
    NTable::TKeyRange range;
    range.MinKey = minKey;
    range.MaxKey = maxKey;
    AFL_VERIFY(DsGroupSelector);
    auto it = Database.IterateRange(IndexColumnsV2::TableId, range, tags);
    std::optional<std::pair<TInternalPathId, ui64>> lastKey;
    while (true) {
        auto ready = it->Next(NTable::ENext::Data);
        if (ready == NTable::EReady::Page) {
            return TSeedingBatchResult{ false, precharge.BytesPrecharged, TConclusionStatus::Success(), lastKey };
        }
        if (ready == NTable::EReady::Gone) {
            break;
        }
        TRawRowAdapter adapter(it->Row(), tags);
        const TString blobIdsData = adapter.GetValue<IndexColumnsV2::BlobIds>();
        auto blobIds = TColumnChunkLoadContextV2::TryParseBlobIds(blobIdsData, *DsGroupSelector);
        if (!blobIds.IsSuccess()) {
            return TSeedingBatchResult{ true, precharge.BytesPrecharged, TConclusionStatus::Fail(blobIds.GetErrorMessage()), lastKey };
        }
        auto pathId = TInternalPathId::FromRawValue(adapter.GetValue<IndexColumnsV2::PathId>());
        auto portionId = adapter.GetValue<IndexColumnsV2::PortionId>();
        lastKey = { pathId, portionId };
        callback(TColumnChunkLoadContextV2(adapter, *DsGroupSelector));
    }
    return TSeedingBatchResult{ true, precharge.BytesPrecharged, TConclusionStatus::Success(), lastKey };
}

TSeedingBatchResult TDbWrapper::LoadIndexesSeeding(std::pair<TInternalPathId, ui64> startKey, std::pair<TInternalPathId, ui64> endKey,
    ui64 bytesLimit, const std::function<void(const TInternalPathId, const ui64, TIndexChunkLoadContext&&)>& callback)
{
    AFL_VERIFY(bytesLimit > 0)("event", "seeding loader called with zero bytesLimit");
    using IndexIndexes = NColumnShard::Schema::IndexIndexes;
    static const auto& kTagIds = IndexIndexes::Columns<IndexIndexes::TColumns>::GetColumnIds();
    NTable::TTagsRef tags(kTagIds.data(), kTagIds.size());
    ui64 startPathId = startKey.first.GetRawValue();
    ui64 startPortionId = startKey.second;
    ui64 endPathId = endKey.first.GetRawValue();
    ui64 endPortionId = endKey.second;
    // IndexIndexes key has 4 columns; provide all 4 to avoid "incomplete MinKey" VERIFY.
    ui32 minIndexId = 0;
    ui32 minChunkIdx = 0;
    ui32 maxIndexId = std::numeric_limits<ui32>::max();
    ui32 maxChunkIdx = std::numeric_limits<ui32>::max();
    const NKikimr::TRawTypeValue minKeyArr[4] = {
        { &startPathId, sizeof(ui64), NScheme::NTypeIds::Uint64 },
        { &startPortionId, sizeof(ui64), NScheme::NTypeIds::Uint64 },
        { &minIndexId, sizeof(ui32), NScheme::NTypeIds::Uint32 },
        { &minChunkIdx, sizeof(ui32), NScheme::NTypeIds::Uint32 },
    };
    const NKikimr::TRawTypeValue maxKeyArr[4] = {
        { &endPathId, sizeof(ui64), NScheme::NTypeIds::Uint64 },
        { &endPortionId, sizeof(ui64), NScheme::NTypeIds::Uint64 },
        { &maxIndexId, sizeof(ui32), NScheme::NTypeIds::Uint32 },
        { &maxChunkIdx, sizeof(ui32), NScheme::NTypeIds::Uint32 },
    };
    NTable::TRawVals minKey(minKeyArr, 4);
    NTable::TRawVals maxKey(maxKeyArr, 4);
    auto precharge = Database.Precharge(IndexIndexes::TableId, minKey, maxKey, tags, 0, 0, bytesLimit);
    if (!precharge.Ready) {
        return TSeedingBatchResult{ false, precharge.BytesPrecharged, TConclusionStatus::Success(), {} };
    }
    NTable::TKeyRange range;
    range.MinKey = minKey;
    range.MaxKey = maxKey;
    auto it = Database.IterateRange(IndexIndexes::TableId, range, tags);
    std::optional<std::pair<TInternalPathId, ui64>> lastKey;
    while (true) {
        auto ready = it->Next(NTable::ENext::Data);
        if (ready == NTable::EReady::Page) {
            return TSeedingBatchResult{ false, precharge.BytesPrecharged, TConclusionStatus::Success(), lastKey };
        }
        if (ready == NTable::EReady::Gone) {
            break;
        }
        TRawRowAdapter adapter(it->Row(), tags);
        TInternalPathId pathId = TInternalPathId::FromRawValue(adapter.GetValue<IndexIndexes::PathId>());
        ui64 portionId = adapter.GetValue<IndexIndexes::PortionId>();
        // validate Blob column when present without BlobIdx (legacy direct-address rows)
        if (adapter.HaveValue<IndexIndexes::Blob>() && !adapter.HaveValue<IndexIndexes::BlobIdx>()) {
            const TString strBlobId = adapter.GetValue<IndexIndexes::Blob>();
            auto blobId = TIndexChunkLoadContext::TryParseBlobAddress(strBlobId, *DsGroupSelector);
            if (!blobId.IsSuccess()) {
                return TSeedingBatchResult{ true, precharge.BytesPrecharged, TConclusionStatus::Fail(blobId.GetErrorMessage()), lastKey };
            }
        }
        lastKey = { pathId, portionId };
        callback(pathId, portionId, TIndexChunkLoadContext(adapter, DsGroupSelector));
    }
    return TSeedingBatchResult{ true, precharge.BytesPrecharged, TConclusionStatus::Success(), lastKey };
}

}   // namespace NKikimr::NOlap
