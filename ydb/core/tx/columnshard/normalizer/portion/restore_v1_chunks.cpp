#include "normalizer.h"
#include "restore_v1_chunks.h"

#include <ydb/core/formats/arrow/size_calcer.h>
#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/tables_manager.h>

namespace NKikimr::NOlap::NRestoreV1Chunks {

class TPatchItemAddV1 {
private:
    TPortionLoadContext PortionInfo;
    std::map<TFullChunkAddress, TColumnChunkLoadContext> ChunksInfo;
    THashMap<TUnifiedBlobId, ui32> IndexByBlob;
    std::vector<TUnifiedBlobId> BlobIds;

public:
    const ui32& GetIndexByBlob(const TUnifiedBlobId& blobId) const {
        auto it = IndexByBlob.find(blobId);
        AFL_VERIFY(it != IndexByBlob.end());
        return it->second;
    }
    const std::vector<TUnifiedBlobId>& GetBlobIds() const {
        return BlobIds;
    }

    const TPortionLoadContext& GetPortionInfo() const {
        return PortionInfo;
    }

    const std::map<TFullChunkAddress, TColumnChunkLoadContext>& GetChunksInfo() const {
        return ChunksInfo;
    }

    TPatchItemAddV1(const TPortionLoadContext& portionInfo, std::map<TFullChunkAddress, TColumnChunkLoadContext>&& chunksInfo)
        : PortionInfo(portionInfo)
        , ChunksInfo(std::move(chunksInfo)) {
        for (auto&& i : ChunksInfo) {
            auto it = IndexByBlob.find(i.second.GetBlobRange().GetBlobId());
            if (it == IndexByBlob.end()) {
                IndexByBlob.emplace(i.second.GetBlobRange().GetBlobId(), IndexByBlob.size());
                BlobIds.emplace_back(i.second.GetBlobRange().GetBlobId());
            }
        }
    }
};

class TChangesAddV1: public INormalizerChanges {
private:
    std::vector<TPatchItemAddV1> Patches;

public:
    TChangesAddV1(std::vector<TPatchItemAddV1>&& patches)
        : Patches(std::move(patches)) {
    }
    virtual bool ApplyOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const TNormalizationController&) const override {
        using namespace NColumnShard;
        NIceDb::TNiceDb db(txc.DB);
        using IndexPortions = NColumnShard::Schema::IndexPortions;
        using IndexColumnsV1 = NColumnShard::Schema::IndexColumnsV1;
        for (auto&& i : Patches) {
            auto metaProto = i.GetPortionInfo().GetMetaProto();
            metaProto.ClearBlobIds();
            AFL_VERIFY(!metaProto.GetBlobIds().size());
            for (auto&& b : i.GetBlobIds()) {
                *metaProto.AddBlobIds() = b.GetLogoBlobId().AsBinaryString();
            }
            ui32 idx = 0;
            for (auto&& b : metaProto.GetBlobIds()) {
                auto logo = TLogoBlobID::FromBinary(b);
                AFL_VERIFY(i.GetBlobIds()[idx++].GetLogoBlobId() == logo);
            }
            db.Table<IndexPortions>()
                .Key(i.GetPortionInfo().GetPathId().GetRawValue(), i.GetPortionInfo().GetPortionId())
                .Update(NIceDb::TUpdate<IndexPortions::Metadata>(metaProto.SerializeAsString()));
            for (auto&& [_, c] : i.GetChunksInfo()) {
                db.Table<IndexColumnsV1>()
                    .Key(c.GetPathId().GetRawValue(), c.GetPortionId(), c.GetAddress().GetColumnId(), c.GetAddress().GetChunkIdx())
                    .Update(NIceDb::TUpdate<IndexColumnsV1::Metadata>(c.GetMetaProto().SerializeAsString()),
                        NIceDb::TUpdate<IndexColumnsV1::BlobIdx>(i.GetIndexByBlob(c.GetBlobRange().GetBlobId())),
                        NIceDb::TUpdate<IndexColumnsV1::Offset>(c.GetBlobRange().GetOffset()),
                        NIceDb::TUpdate<IndexColumnsV1::Size>(c.GetBlobRange().GetSize()));
            }
        }

        return true;
    }

    virtual ui64 GetSize() const override {
        return Patches.size();
    }
};

class TPatchItemRemoveV1 {
private:
    TColumnChunkLoadContextV1 ChunkInfo;

public:
    const TColumnChunkLoadContextV1& GetChunkInfo() const {
        return ChunkInfo;
    }

    TPatchItemRemoveV1(const TColumnChunkLoadContextV1& chunkInfo)
        : ChunkInfo(chunkInfo) {
    }
};

class TChangesRemoveV1: public INormalizerChanges {
private:
    std::vector<TPatchItemRemoveV1> Patches;

public:
    TChangesRemoveV1(std::vector<TPatchItemRemoveV1>&& patches)
        : Patches(std::move(patches)) {
    }
    virtual bool ApplyOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const TNormalizationController&) const override {
        using namespace NColumnShard;
        NIceDb::TNiceDb db(txc.DB);
        using IndexColumnsV1 = NColumnShard::Schema::IndexColumnsV1;
        for (auto&& i : Patches) {
            db.Table<IndexColumnsV1>()
                .Key(i.GetChunkInfo().GetPathId().GetRawValue(), i.GetChunkInfo().GetPortionId(), i.GetChunkInfo().GetAddress().GetEntityId(),
                    i.GetChunkInfo().GetAddress().GetChunkIdx())
                .Delete();
        }

        return true;
    }

    virtual ui64 GetSize() const override {
        return Patches.size();
    }
};

TConclusion<std::vector<INormalizerTask::TPtr>> TNormalizer::DoInit(
    const TNormalizationController& /*controller*/, NTabletFlatExecutor::TTransactionContext& txc) {
    using namespace NColumnShard;
    NIceDb::TNiceDb db(txc.DB);

    bool ready = true;
    ready = ready & Schema::Precharge<Schema::IndexPortions>(db, txc.DB.GetScheme());
    ready = ready & Schema::Precharge<Schema::IndexColumns>(db, txc.DB.GetScheme());
    ready = ready & Schema::Precharge<Schema::IndexColumnsV1>(db, txc.DB.GetScheme());
    if (!ready) {
        return TConclusionStatus::Fail("Not ready");
    }
    THashMap<ui64, TPortionLoadContext> portions0;
    THashSet<ui64> existPortions0;
    THashMap<ui64, std::map<TFullChunkAddress, TColumnChunkLoadContext>> columns0;
    THashMap<TFullChunkAddress, TColumnChunkLoadContextV1> columns1Remove;

    {
        auto rowset = db.Table<Schema::IndexPortions>().Select();
        if (!rowset.IsReady()) {
            return TConclusionStatus::Fail("Not ready");
        }

        while (!rowset.EndOfSet()) {
            TPortionLoadContext portion(rowset);
            existPortions0.emplace(portion.GetPortionId());
            if (!portion.GetMetaProto().BlobIdsSize()) {
                AFL_VERIFY(portions0.emplace(portion.GetPortionId(), portion).second);
            }

            if (!rowset.Next()) {
                return TConclusionStatus::Fail("Not ready");
            }
        }
    }

    {
        auto rowset = db.Table<Schema::IndexColumns>().Select();
        if (!rowset.IsReady()) {
            return TConclusionStatus::Fail("Not ready");
        }

        while (!rowset.EndOfSet()) {
            TColumnChunkLoadContext chunk(rowset, &DsGroupSelector);
            if (portions0.contains(chunk.GetPortionId())) {
                AFL_VERIFY(columns0[chunk.GetPortionId()].emplace(chunk.GetFullChunkAddress(), chunk).second);
            }

            if (!rowset.Next()) {
                return TConclusionStatus::Fail("Not ready");
            }
        }
    }

    {
        auto rowset = db.Table<Schema::IndexColumnsV1>().Select();
        if (!rowset.IsReady()) {
            return TConclusionStatus::Fail("Not ready");
        }

        while (!rowset.EndOfSet()) {
            TColumnChunkLoadContextV1 chunk(rowset);
            //AFL_VERIFY(!portions0.contains(chunk.GetPortionId()));
            if (!existPortions0.contains(chunk.GetPortionId())) {
                AFL_VERIFY(columns1Remove.emplace(chunk.GetFullChunkAddress(), chunk).second);
            }

            if (!rowset.Next()) {
                return TConclusionStatus::Fail("Not ready");
            }
        }
    }

    std::vector<INormalizerTask::TPtr> tasks;
    if (columns1Remove.empty() && portions0.empty()) {
        return tasks;
    }
    if (!AppDataVerified().ColumnShardConfig.GetColumnChunksV0Usage()) {
        return tasks;
    }

    AFL_VERIFY(AppDataVerified().ColumnShardConfig.GetColumnChunksV1Usage());
    {
        std::vector<TPatchItemRemoveV1> package;
        for (auto&& [portionId, chunkInfo] : columns1Remove) {
            package.emplace_back(chunkInfo);
            if (package.size() == 100) {
                std::vector<TPatchItemRemoveV1> local;
                local.swap(package);
                tasks.emplace_back(std::make_shared<TTrivialNormalizerTask>(std::make_shared<TChangesRemoveV1>(std::move(local))));
            }
        }

        if (package.size() > 0) {
            tasks.emplace_back(std::make_shared<TTrivialNormalizerTask>(std::make_shared<TChangesRemoveV1>(std::move(package))));
        }
    }

    {
        std::vector<TPatchItemAddV1> package;
        for (auto&& [portionId, portionInfo] : portions0) {
            auto it = columns0.find(portionId);
            AFL_VERIFY(it != columns0.end());
            package.emplace_back(portionInfo, std::move(it->second));
            columns0.erase(it);
            if (package.size() == 100) {
                std::vector<TPatchItemAddV1> local;
                local.swap(package);
                tasks.emplace_back(std::make_shared<TTrivialNormalizerTask>(std::make_shared<TChangesAddV1>(std::move(local))));
            }
        }

        if (package.size() > 0) {
            tasks.emplace_back(std::make_shared<TTrivialNormalizerTask>(std::make_shared<TChangesAddV1>(std::move(package))));
        }
    }

    return tasks;
}

}   // namespace NKikimr::NOlap::NRestoreV1Chunks
