#include "builder.h"

#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/defs.h>
#include <ydb/core/tx/columnshard/engines/portions/write_with_blobs.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/engines/writer/buffer/events.h>
#include <ydb/core/tx/columnshard/engines/writer/indexed_blob_constructor.h>

namespace NKikimr::NOlap {

std::optional<std::vector<NKikimr::NArrow::TSerializedBatch>> TBuildSlicesTask::BuildSlices() {
    if (!OriginalBatch->num_rows()) {
        return std::vector<NKikimr::NArrow::TSerializedBatch>();
    }
    NArrow::TBatchSplitttingContext context(NColumnShard::TLimits::GetMaxBlobSize());
    context.SetFieldsForSpecialKeys(WriteData.GetPrimaryKeySchema());
    auto splitResult = NArrow::SplitByBlobSize(OriginalBatch, context);
    if (splitResult.IsFail()) {
        AFL_INFO(NKikimrServices::TX_COLUMNSHARD_WRITE)(
            "event", TStringBuilder() << "cannot split batch in according to limits: " + splitResult.GetErrorMessage());
        return {};
    }
    auto result = splitResult.DetachResult();
    if (result.size() > 1) {
        for (auto&& i : result) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_WRITE)("event", "strange_blobs_splitting")("blob", i.DebugString())(
                "original_size", WriteData.GetSize());
        }
    }
    return result;
}

void TBuildSlicesTask::ReplyError(const TString& message, const NColumnShard::TEvPrivate::TEvWriteBlobsResult::EErrorClass errorClass) {
    auto writeDataPtr = std::make_shared<NEvWrite::TWriteData>(std::move(WriteData));
    TWritingBuffer buffer(writeDataPtr->GetBlobsAction(), { std::make_shared<TWriteAggregation>(*writeDataPtr) });
    auto result =
        NColumnShard::TEvPrivate::TEvWriteBlobsResult::Error(NKikimrProto::EReplyStatus::CORRUPTED, std::move(buffer), message, errorClass);
    TActorContext::AsActorContext().Send(Context.GetTabletActorId(), result.release());
}

class TPortionWriteController: public NColumnShard::IWriteController,
                               public NColumnShard::TMonitoringObjectsCounter<TIndexedWriteController, true> {
public:
    class TInsertPortion {
    private:
        TWritePortionInfoWithBlobsResult Portion;
        std::shared_ptr<arrow::RecordBatch> PKBatch;

    public:
        TWritePortionInfoWithBlobsResult& MutablePortion() {
            return Portion;
        }
        const TWritePortionInfoWithBlobsResult& GetPortion() const {
            return Portion;
        }
        TWritePortionInfoWithBlobsResult&& ExtractPortion() {
            return std::move(Portion);
        }
        const std::shared_ptr<arrow::RecordBatch>& GetPKBatch() const {
            return PKBatch;
        }
        TInsertPortion(TWritePortionInfoWithBlobsResult&& portion, const std::shared_ptr<arrow::RecordBatch> pkBatch)
            : Portion(std::move(portion))
            , PKBatch(pkBatch) {
            AFL_VERIFY(PKBatch);
        }
    };

private:
    const std::shared_ptr<IBlobsWritingAction> Action;
    std::vector<TInsertPortion> Portions;
    NEvWrite::TWriteMeta WriteMeta;
    TActorId DstActor;
    const ui64 DataSize;
    void DoOnReadyResult(const NActors::TActorContext& ctx, const NColumnShard::TBlobPutResult::TPtr& putResult) override {
        std::vector<NColumnShard::TInsertedPortion> portions;
        std::vector<NColumnShard::TNoDataWrite> noDataWrites;
        for (auto&& i : Portions) {
            portions.emplace_back(i.ExtractPortion(), i.GetPKBatch());
        }
        NColumnShard::TInsertedPortions pack(std::move(WriteMeta), std::move(portions), DataSize);
        std::vector<NColumnShard::TInsertedPortions> packs = { pack };
        auto result = std::make_unique<NColumnShard::NPrivateEvents::NWrite::TEvWritePortionResult>(
            putResult->GetPutStatus(), Action, std::move(packs), std::move(noDataWrites));
        ctx.Send(DstActor, result.release());
    }
    virtual void DoOnStartSending() override {
    }

public:
    TPortionWriteController(const TActorId& dstActor, const std::shared_ptr<IBlobsWritingAction>& action, const NEvWrite::TWriteMeta& writeMeta,
        std::vector<TInsertPortion>&& portions, const ui64 dataSize)
        : Action(action)
        , Portions(std::move(portions))
        , WriteMeta(writeMeta)
        , DstActor(dstActor)
        , DataSize(dataSize)
    {
        for (auto&& p : Portions) {
            for (auto&& b : p.MutablePortion().MutableBlobs()) {
                auto& task = AddWriteTask(TBlobWriteInfo::BuildWriteTask(b.GetResultBlob(), action));
                b.RegisterBlobId(p.MutablePortion(), task.GetBlobId());
            }
        }
    }
};

TConclusionStatus TBuildSlicesTask::DoExecute(const std::shared_ptr<ITask>& /*taskPtr*/) {
    const NActors::TLogContextGuard g = NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD_WRITE)("tablet_id", TabletId)("parent_id",
        Context.GetTabletActorId())("write_id", WriteData.GetWriteMeta().GetWriteId())("table_id", WriteData.GetWriteMeta().GetTableId());
    if (!Context.IsActive()) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD_WRITE)("event", "abort_execution");
        ReplyError("execution aborted", NColumnShard::TEvPrivate::TEvWriteBlobsResult::EErrorClass::Internal);
        return TConclusionStatus::Fail("execution aborted");
    }
    if (!OriginalBatch) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD_WRITE)("event", "ev_write_bad_data");
        ReplyError("no data in batch", NColumnShard::TEvPrivate::TEvWriteBlobsResult::EErrorClass::Internal);
        return TConclusionStatus::Fail("no data in batch");
    }
    if (WriteData.GetWritePortions()) {
        if (OriginalBatch->num_rows() == 0) {
            std::vector<NColumnShard::TInsertedPortions> portions;
            std::vector<NColumnShard::TNoDataWrite> noDataWrites = { NColumnShard::TNoDataWrite(WriteData.GetWriteMeta(), WriteData.GetSize()) };
            auto result = std::make_unique<NColumnShard::NPrivateEvents::NWrite::TEvWritePortionResult>(
                NKikimrProto::EReplyStatus::OK, nullptr, std::move(portions), std::move(noDataWrites));
            NActors::TActivationContext::AsActorContext().Send(Context.GetTabletActorId(), result.release());
        } else {
            auto batches = NArrow::NMerger::TRWSortableBatchPosition::SplitByBordersInIntervalPositions(OriginalBatch,
                Context.GetActualSchema()->GetIndexInfo().GetPrimaryKey()->field_names(), WriteData.GetData()->GetSeparationPoints());
            std::vector<TPortionWriteController::TInsertPortion> portions;
            for (auto&& batch : batches) {
                if (!batch) {
                    continue;
                }
                auto portionConclusion =
                    Context.GetActualSchema()->PrepareForWrite(Context.GetActualSchema(), WriteData.GetWriteMeta().GetTableId(), batch,
                        WriteData.GetWriteMeta().GetModificationType(), Context.GetStoragesManager(), Context.GetSplitterCounters());
                if (portionConclusion.IsFail()) {
                    ReplyError(portionConclusion.GetErrorMessage(), NColumnShard::TEvPrivate::TEvWriteBlobsResult::EErrorClass::Request);
                    return portionConclusion;
                }
                std::shared_ptr<arrow::RecordBatch> pkBatch =
                    NArrow::TColumnOperator().Extract(batch, Context.GetActualSchema()->GetIndexInfo().GetPrimaryKey()->fields());
                portions.emplace_back(portionConclusion.DetachResult(), pkBatch);
            }
            auto writeController = std::make_shared<NOlap::TPortionWriteController>(
                Context.GetTabletActorId(), WriteData.GetBlobsAction(), WriteData.GetWriteMeta(), std::move(portions), WriteData.GetSize());
            if (WriteData.GetBlobsAction()->NeedDraftTransaction()) {
                TActorContext::AsActorContext().Send(
                    Context.GetTabletActorId(), std::make_unique<NColumnShard::TEvPrivate::TEvWriteDraft>(writeController));
            } else {
                TActorContext::AsActorContext().Register(NColumnShard::CreateWriteActor(TabletId, writeController, TInstant::Max()));
            }
        }
    } else {
        const auto& indexSchema = Context.GetActualSchema()->GetIndexInfo().ArrowSchema();
        auto subsetConclusion = NArrow::TColumnOperator().IgnoreOnDifferentFieldTypes().BuildSequentialSubset(OriginalBatch, indexSchema);
        if (subsetConclusion.IsFail()) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_WRITE)("event", "unadaptable schemas")("index", indexSchema.ToString())(
                "problem", subsetConclusion.GetErrorMessage());
            ReplyError("unadaptable schema: " + subsetConclusion.GetErrorMessage(),
                NColumnShard::TEvPrivate::TEvWriteBlobsResult::EErrorClass::Internal);
            return TConclusionStatus::Fail("cannot reorder schema: " + subsetConclusion.GetErrorMessage());
        }
        NArrow::TSchemaSubset subset = subsetConclusion.DetachResult();

        if (OriginalBatch->num_columns() != indexSchema.num_fields()) {
            AFL_VERIFY(OriginalBatch->num_columns() < indexSchema.num_fields())("original", OriginalBatch->num_columns())(
                                                          "index", indexSchema.num_fields());
            if (HasAppData() && !AppDataVerified().FeatureFlags.GetEnableOptionalColumnsInColumnShard() &&
                WriteData.GetWriteMeta().GetModificationType() != NEvWrite::EModificationType::Delete) {
                subset = NArrow::TSchemaSubset::AllFieldsAccepted();
                const auto columnIdsVector = Context.GetActualSchema()->GetIndexInfo().GetColumnIds(false);
                const std::set<ui32> columnIdsSet(columnIdsVector.begin(), columnIdsVector.end());
                auto normalized =
                    Context.GetActualSchema()
                        ->NormalizeBatch(*Context.GetActualSchema(), std::make_shared<NArrow::TGeneralContainer>(OriginalBatch), columnIdsSet)
                        .DetachResult();
                OriginalBatch = NArrow::ToBatch(normalized->BuildTableVerified(), true);
            }
        }
        WriteData.MutableWriteMeta().SetWriteMiddle2StartInstant(TMonotonic::Now());
        auto batches = BuildSlices();
        WriteData.MutableWriteMeta().SetWriteMiddle3StartInstant(TMonotonic::Now());
        if (batches) {
            auto writeDataPtr = std::make_shared<NEvWrite::TWriteData>(std::move(WriteData));
            writeDataPtr->SetSchemaSubset(std::move(subset));
            std::shared_ptr<arrow::RecordBatch> pkBatch =
                NArrow::TColumnOperator().Extract(OriginalBatch, Context.GetActualSchema()->GetIndexInfo().GetPrimaryKey()->fields());
            auto result = std::make_unique<NColumnShard::NWriting::TEvAddInsertedDataToBuffer>(writeDataPtr, std::move(*batches), pkBatch);
            TActorContext::AsActorContext().Send(BufferActorId, result.release());
        } else {
            ReplyError("Cannot slice input to batches", NColumnShard::TEvPrivate::TEvWriteBlobsResult::EErrorClass::Internal);
            return TConclusionStatus::Fail("Cannot slice input to batches");
        }
    }
    return TConclusionStatus::Success();
}
}   // namespace NKikimr::NOlap
