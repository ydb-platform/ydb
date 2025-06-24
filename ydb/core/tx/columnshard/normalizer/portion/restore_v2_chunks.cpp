#include "normalizer.h"
#include "restore_v2_chunks.h"

#include <ydb/core/formats/arrow/size_calcer.h>
#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/tables_manager.h>

namespace NKikimr::NOlap::NRestoreV2Chunks {

class TV2BuildTask {
private:
    TPortionAddress PortionAddress;
    std::vector<TColumnChunkLoadContextV1> Chunks;

public:
    const TPortionAddress& GetPortionAddress() const {
        return PortionAddress;
    }

    TInternalPathId GetPathId() const {
        return PortionAddress.GetPathId();
    }

    ui64 GetPortionId() const {
        return PortionAddress.GetPortionId();
    }

    void AddChunk(const TColumnChunkLoadContextV1& chunk) {
        Chunks.emplace_back(chunk);
    }

    NKikimrTxColumnShard::TIndexPortionAccessor BuildProto() const {
        const auto pred = [](const TColumnChunkLoadContextV1& l, const TColumnChunkLoadContextV1& r) {
            return l.GetAddress() < r.GetAddress();
        };
        auto chunks = Chunks;
        std::sort(chunks.begin(), chunks.end(), pred);
        NKikimrTxColumnShard::TIndexPortionAccessor result;
        for (auto&& c : chunks) {
            *result.AddChunks() = c.SerializeToDBProto();
        }
        return result;
    }

    TV2BuildTask(const TPortionAddress& address)
        : PortionAddress(address) {
    }
};

class TChangesAddV2: public INormalizerChanges {
private:
    std::vector<TV2BuildTask> Patches;

public:
    TChangesAddV2(std::vector<TV2BuildTask>&& patches)
        : Patches(std::move(patches)) {
    }
    virtual bool ApplyOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const TNormalizationController&) const override {
        using namespace NColumnShard;
        NIceDb::TNiceDb db(txc.DB);
        using IndexColumnsV2 = NColumnShard::Schema::IndexColumnsV2;
        for (auto&& i : Patches) {
            auto metaProto = i.BuildProto();
            db.Table<IndexColumnsV2>()
                .Key(i.GetPathId().GetRawValue(), i.GetPortionId())
                .Update(NIceDb::TUpdate<IndexColumnsV2::Metadata>(metaProto.SerializeAsString()));
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
    ready = ready & Schema::Precharge<Schema::IndexColumnsV1>(db, txc.DB.GetScheme());
    ready = ready & Schema::Precharge<Schema::IndexColumnsV2>(db, txc.DB.GetScheme());
    if (!ready) {
        return TConclusionStatus::Fail("Not ready");
    }
    THashSet<TPortionAddress> readyPortions;
    THashMap<TPortionAddress, TV2BuildTask> buildPortions;
    {
        auto rowset = db.Table<Schema::IndexColumnsV2>().Select();
        if (!rowset.IsReady()) {
            return TConclusionStatus::Fail("Not ready");
        }

        while (!rowset.EndOfSet()) {
            AFL_VERIFY(readyPortions.emplace(TPortionAddress(TInternalPathId::FromRawValue(rowset.template GetValue<NColumnShard::Schema::IndexColumnsV2::PathId>()),
                rowset.template GetValue<NColumnShard::Schema::IndexColumnsV2::PortionId>())).second);
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
            if (!readyPortions.contains(chunk.GetPortionAddress())) {
                auto it = buildPortions.find(chunk.GetPortionAddress());
                if (it == buildPortions.end()) {
                    it = buildPortions.emplace(chunk.GetPortionAddress(), TV2BuildTask(chunk.GetPortionAddress())).first;
                }
                it->second.AddChunk(chunk);
            }

            if (!rowset.Next()) {
                return TConclusionStatus::Fail("Not ready");
            }
        }
    }

    std::vector<INormalizerTask::TPtr> tasks;
    if (buildPortions.empty()) {
        return tasks;
    }
    if (!AppDataVerified().ColumnShardConfig.GetColumnChunksV1Usage()) {
        return tasks;
    }

    {
        std::vector<TV2BuildTask> package;
        for (auto&& [portionAddress, portionInfos] : buildPortions) {
            package.emplace_back(std::move(portionInfos));
            if (package.size() == 100) {
                std::vector<TV2BuildTask> local;
                local.swap(package);
                tasks.emplace_back(std::make_shared<TTrivialNormalizerTask>(std::make_shared<TChangesAddV2>(std::move(local))));
            }
        }

        if (package.size() > 0) {
            tasks.emplace_back(std::make_shared<TTrivialNormalizerTask>(std::make_shared<TChangesAddV2>(std::move(package))));
        }
    }

    return tasks;
}

}   // namespace NKikimr::NOlap::NRestoreV1Chunks
