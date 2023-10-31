#include "normalizer.h"


#include <ydb/core/tx/conveyor/usage/abstract.h>
#include <ydb/core/tx/conveyor/usage/service.h>

#include <ydb/core/tx/columnshard/blobs_reader/task.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/tables_manager.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>

#include <ydb/core/formats/arrow/size_calcer.h>


namespace NKikimr::NOlap {

class TChunksNormalizer::TNormalizerResult : public INormalizerChanges {
    std::vector<TChunksNormalizer::TChunkInfo> Chunks;
public:
    TNormalizerResult(std::vector<TChunksNormalizer::TChunkInfo>&& chunks)
        : Chunks(std::move(chunks))
    {}

    bool Apply(NTabletFlatExecutor::TTransactionContext& txc, const TNormalizationController& /* normController */) const override {
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
};

class TNormalizationChangesTask: public NConveyor::ITask {
private:
    THashMap<NKikimr::NOlap::TBlobRange, TString> Blobs;
    std::vector<TChunksNormalizer::TChunkInfo> Chunks;
    TNormalizationContext NormContext;
protected:
    virtual bool DoExecute() override {
        for (auto&& chunkInfo : Chunks) {
            const auto& blobRange = chunkInfo.GetBlobRange();

            auto blobIt = Blobs.find(blobRange);
            Y_ABORT_UNLESS(blobIt != Blobs.end());

            auto columnLoader = chunkInfo.GetLoader();
            Y_ABORT_UNLESS(!!columnLoader);

            TPortionInfo::TAssembleBlobInfo assembleBlob(blobIt->second);
            auto batch = assembleBlob.BuildRecordBatch(*columnLoader);
            Y_ABORT_UNLESS(!!batch);

            chunkInfo.MutableUpdate().SetNumRows(batch->num_rows());
            chunkInfo.MutableUpdate().SetRawBytes(NArrow::GetBatchDataSize(batch));
        }

        auto changes = std::make_shared<TChunksNormalizer::TNormalizerResult>(std::move(Chunks));
        TActorContext::AsActorContext().Send(NormContext.GetColumnshardActor(), std::make_unique<NColumnShard::TEvPrivate::TEvNormalizerResult>(changes));
        return true;
    }

public:
    TNormalizationChangesTask(THashMap<NKikimr::NOlap::TBlobRange, TString>&& blobs, const TNormalizationContext& nCtx, std::vector<TChunksNormalizer::TChunkInfo>&& chunks)
        : Blobs(std::move(blobs))
        , Chunks(std::move(chunks))
        , NormContext(nCtx)
    {}

    virtual TString GetTaskClassIdentifier() const override {
        const static TString name = "TNormalizationChangesTask";
        return name;
    }
};

class TReadPortionsTask: public NOlap::NBlobOperations::NRead::ITask {
    private:
        using TBase = NOlap::NBlobOperations::NRead::ITask;
        std::vector<TChunksNormalizer::TChunkInfo> Chunks;
        TNormalizationContext NormContext;

    public:
        TReadPortionsTask(const TNormalizationContext& nCtx, const std::vector<std::shared_ptr<IBlobsReadingAction>>& actions, std::vector<TChunksNormalizer::TChunkInfo>&& chunks)
            : TBase(actions, "CS::NORMALIZER")
            , Chunks(std::move(chunks))
            , NormContext(nCtx)
        {
        }

    protected:
        virtual void DoOnDataReady(const std::shared_ptr<NOlap::NResourceBroker::NSubscribe::TResourcesGuard>& resourcesGuard) override {
            Y_UNUSED(resourcesGuard);
            std::shared_ptr<NConveyor::ITask> task = std::make_shared<TNormalizationChangesTask>(std::move(ExtractBlobsData()), NormContext, std::move(Chunks));
            NConveyor::TCompServiceOperator::SendTaskToExecute(task);
        }

        virtual bool DoOnError(const TBlobRange& range, const IBlobsReadingAction::TErrorStatus& status) override {
            Y_UNUSED(status, range);
            return false;
        }

    public:
        using TBase::TBase;
};

class TChunksNormalizerTask : public INormalizerTask {
    std::vector<TChunksNormalizer::TChunkInfo> Package;
public:
    TChunksNormalizerTask(std::vector<TChunksNormalizer::TChunkInfo>&& package)
        : Package(std::move(package))
    {}

    void Start(const TNormalizationController& controller, const TNormalizationContext& nCtx) override {
        controller.GetCounters().CountObjects(Package.size());
        auto readingAction = controller.GetStoragesManager()->GetInsertOperator()->StartReadingAction("CS::NORMALIZER");
        for (auto&& chunk : Package) {
            readingAction->AddRange(chunk.GetBlobRange());
        }
        std::vector<std::shared_ptr<IBlobsReadingAction>> actions = {readingAction};
        ui64 memSize = 0;
        NOlap::NResourceBroker::NSubscribe::ITask::StartResourceSubscription(
            nCtx.GetResourceSubscribeActor(),std::make_shared<NOlap::NBlobOperations::NRead::ITask::TReadSubscriber>(
                    std::make_shared<TReadPortionsTask>( nCtx, actions, std::move(Package) ), 1, memSize, "CS::NORMALIZER", controller.GetTaskSubscription()));
    }
};

void TChunksNormalizer::TChunkInfo::InitSchema(const NColumnShard::TTablesManager& tm) {
    Schema = tm.GetPrimaryIndexSafe().GetVersionedIndex().GetSchema(NOlap::TSnapshot(Key.GetPlanStep(), Key.GetTxId()));
}

TConclusion<std::vector<INormalizerTask::TPtr>> TChunksNormalizer::Init(const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc) {
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
        ACFL_ERROR("step", "TChunksNormalizer.StartNormalizer")("error", "can't initialize tables manager");
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
            tasks.emplace_back(std::make_shared<TChunksNormalizerTask>(std::move(local)));
        }
    }

    if (package.size() > 0) {
        tasks.emplace_back(std::make_shared<TChunksNormalizerTask>(std::move(package)));
    }
    AtomicSet(ActiveTasksCount, tasks.size());
    return tasks;
}

}
