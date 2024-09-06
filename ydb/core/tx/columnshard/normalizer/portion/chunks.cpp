#include "chunks.h"
#include "normalizer.h"

#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/tables_manager.h>
#include <ydb/core/formats/arrow/size_calcer.h>


namespace NKikimr::NOlap {

class TChunksNormalizer::TNormalizerResult: public INormalizerChanges {
    std::vector<TChunksNormalizer::TChunkInfo> Chunks;
    std::shared_ptr<THashMap<ui64, ISnapshotSchema::TPtr>> Schemas;
public:
    TNormalizerResult(std::vector<TChunksNormalizer::TChunkInfo>&& chunks)
        : Chunks(std::move(chunks)) {
    }

    bool ApplyOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const TNormalizationController& /* normController */) const override {
        using namespace NColumnShard;
        NIceDb::TNiceDb db(txc.DB);

        for (auto&& chunkInfo : Chunks) {

            NKikimrTxColumnShard::TIndexColumnMeta metaProto = chunkInfo.GetMetaProto();
            metaProto.SetNumRows(chunkInfo.GetUpdate().GetNumRows());
            metaProto.SetRawBytes(chunkInfo.GetUpdate().GetRawBytes());

            const auto& key = chunkInfo.GetKey();

            db.Table<Schema::IndexColumns>().Key(key.GetIndex(), key.GetGranule(), key.GetColumnIdx(),
                key.GetPlanStep(), key.GetTxId(), key.GetPortion(), key.GetChunk()).Update(
                    NIceDb::TUpdate<Schema::IndexColumns::Metadata>(metaProto.SerializeAsString())
                );
        }
        return true;
    }

    ui64 GetSize() const override {
        return Chunks.size();
    }
};

class TRowsAndBytesChangesTask: public NConveyor::ITask {
public:
    using TDataContainer = std::vector<TChunksNormalizer::TChunkInfo>;
private:
    NBlobOperations::NRead::TCompositeReadBlobs Blobs;
    std::vector<TChunksNormalizer::TChunkInfo> Chunks;
    TNormalizationContext NormContext;
protected:
    virtual TConclusionStatus DoExecute(const std::shared_ptr<NConveyor::ITask>& /*taskPtr*/) override {
        for (auto&& chunkInfo : Chunks) {
            const auto& blobRange = chunkInfo.GetBlobRange();

            auto blobData = Blobs.Extract(IStoragesManager::DefaultStorageId, blobRange);

            auto columnLoader = chunkInfo.GetLoader();
            Y_ABORT_UNLESS(!!columnLoader);

            TPortionInfo::TAssembleBlobInfo assembleBlob(blobData);
            assembleBlob.SetExpectedRecordsCount(chunkInfo.GetRecordsCount());
            auto batch = assembleBlob.BuildRecordBatch(*columnLoader);
            Y_ABORT_UNLESS(!!batch);

            chunkInfo.MutableUpdate().SetNumRows(batch->GetRecordsCount());
            chunkInfo.MutableUpdate().SetRawBytes(batch->GetRawSizeVerified());
        }

        auto changes = std::make_shared<TChunksNormalizer::TNormalizerResult>(std::move(Chunks));
        TActorContext::AsActorContext().Send(NormContext.GetShardActor(), std::make_unique<NColumnShard::TEvPrivate::TEvNormalizerResult>(changes));
        return TConclusionStatus::Success();
    }

public:
    TRowsAndBytesChangesTask(NBlobOperations::NRead::TCompositeReadBlobs&& blobs, const TNormalizationContext& nCtx, std::vector<TChunksNormalizer::TChunkInfo>&& chunks, std::shared_ptr<THashMap<ui64, ISnapshotSchema::TPtr>>)
        : Blobs(std::move(blobs))
        , Chunks(std::move(chunks))
        , NormContext(nCtx)
    {}

    virtual TString GetTaskClassIdentifier() const override {
        const static TString name = "TRowsAndBytesChangesTask";
        return name;
    }

    static void FillBlobRanges(std::shared_ptr<IBlobsReadingAction> readAction, const TChunksNormalizer::TChunkInfo& chunk) {
        readAction->AddRange(chunk.GetBlobRange());
    }

    static ui64 GetMemSize(const TChunksNormalizer::TChunkInfo&) {
        return 10 * 1024 * 1024;
    }
};

void TChunksNormalizer::TChunkInfo::InitSchema(const NColumnShard::TTablesManager& tm) {
    Schema = tm.GetPrimaryIndexSafe().GetVersionedIndex().GetSchema(NOlap::TSnapshot(Key.GetPlanStep(), Key.GetTxId()));
}

TConclusion<std::vector<INormalizerTask::TPtr>> TChunksNormalizer::DoInit(const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc) {
    using namespace NColumnShard;
    NIceDb::TNiceDb db(txc.DB);

    bool ready = true;
    ready = ready & Schema::Precharge<Schema::IndexColumns>(db, txc.DB.GetScheme());
    if (!ready) {
        return TConclusionStatus::Fail("Not ready");
    }

    std::vector<TChunkInfo> chunks;
    {
        auto rowset = db.Table<Schema::IndexColumns>().Select();
        if (!rowset.IsReady()) {
            return TConclusionStatus::Fail("Not ready");
        }

        while (!rowset.EndOfSet()) {
            TKey key;
            key.Load(rowset);

            TChunkInfo chunkInfo(std::move(key), rowset, &DsGroupSelector);
            if (chunkInfo.NormalizationRequired()) {
                chunks.emplace_back(std::move(chunkInfo));
            }

            if (!rowset.Next()) {
                return TConclusionStatus::Fail("Not ready");
            }
        }
    }

    std::vector<INormalizerTask::TPtr> tasks;
    ACFL_INFO("normalizer", "TChunksNormalizer")("message", TStringBuilder() << chunks.size() << " chunks found");
    if (chunks.empty()) {
        return tasks;
    }

    TTablesManager tablesManager(controller.GetStoragesManager(), 0);
    if (!tablesManager.InitFromDB(db)) {
        ACFL_TRACE("normalizer", "TChunksNormalizer")("error", "can't initialize tables manager");
        return TConclusionStatus::Fail("Can't load index");
    }

    std::vector<TChunkInfo> package;
    package.reserve(100);

    for (auto&& chunk : chunks) {
        chunk.InitSchema(tablesManager);
        package.emplace_back(chunk);
        if (package.size() == 100) {
            std::vector<TChunkInfo> local;
            local.swap(package);
            tasks.emplace_back(std::make_shared<TPortionsNormalizerTask<TRowsAndBytesChangesTask>>(std::move(local)));
        }
    }

    if (package.size() > 0) {
        tasks.emplace_back(std::make_shared<TPortionsNormalizerTask<TRowsAndBytesChangesTask>>(std::move(package)));
    }
    return tasks;
}

}
