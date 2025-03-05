#include "builder.h"

#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/defs.h>
#include <ydb/core/tx/columnshard/engines/portions/write_with_blobs.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/engines/writer/buffer/events.h>
#include <ydb/core/tx/columnshard/engines/writer/indexed_blob_constructor.h>

namespace NKikimr::NOlap {

std::optional<std::vector<NArrow::TSerializedBatch>> TBuildSlicesTask::BuildSlices() {
    if (!OriginalBatch->num_rows()) {
        return std::vector<NKikimr::NArrow::TSerializedBatch>();
    }
    const auto splitSettings = NYDBTest::TControllers::GetColumnShardController()->GetBlobSplitSettings();
    NArrow::TBatchSplitttingContext context(splitSettings.GetMaxBlobSize());
    context.SetFieldsForSpecialKeys(WriteData.GetPrimaryKeySchema());
    auto splitResult = NArrow::SplitByBlobSize(OriginalBatch, context);
    if (splitResult.IsFail()) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD_WRITE)(
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
        TInsertPortion(TWritePortionInfoWithBlobsResult&& portion)
            : Portion(std::move(portion)) {
        }
    };

private:
    const std::shared_ptr<IBlobsWritingAction> Action;
    std::vector<TInsertPortion> Portions;
    NColumnShard::TWriteResult WriteResult;
    TActorId DstActor;
    void DoOnReadyResult(const NActors::TActorContext& ctx, const NColumnShard::TBlobPutResult::TPtr& putResult) override {
        std::vector<NColumnShard::TInsertedPortion> portions;
        for (auto&& i : Portions) {
            portions.emplace_back(i.ExtractPortion());
        }
        NColumnShard::TInsertedPortions pack({ WriteResult }, std::move(portions));
        auto result = std::make_unique<NColumnShard::NPrivateEvents::NWrite::TEvWritePortionResult>(
            putResult->GetPutStatus(), Action, std::move(pack));
        ctx.Send(DstActor, result.release());
    }
    virtual void DoOnStartSending() override {
    }

public:
    TPortionWriteController(const TActorId& dstActor, const std::shared_ptr<IBlobsWritingAction>& action, const NColumnShard::TWriteResult& writeResult,
        std::vector<TInsertPortion>&& portions)
        : Action(action)
        , Portions(std::move(portions))
        , WriteResult(writeResult)
        , DstActor(dstActor)
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
    const NActors::TLogContextGuard g = NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD_WRITE)("tablet_id", TabletId)(
        "parent_id", Context.GetTabletActorId())("write_id", WriteData.GetWriteMeta().GetWriteId())(
        "table_id", WriteData.GetWriteMeta().GetTableId());
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
            NColumnShard::TWriteResult wResult(WriteData.GetWriteMetaPtr(), WriteData.GetSize(), nullptr, true, 0);
            NColumnShard::TInsertedPortions pack({ wResult }, {});
            auto result = std::make_unique<NColumnShard::NPrivateEvents::NWrite::TEvWritePortionResult>(
                NKikimrProto::EReplyStatus::OK, nullptr, std::move(pack));
            NActors::TActivationContext::AsActorContext().Send(Context.GetTabletActorId(), result.release());
        } else {
            std::shared_ptr<arrow::RecordBatch> pkBatch =
                NArrow::TColumnOperator().Extract(OriginalBatch, Context.GetActualSchema()->GetIndexInfo().GetPrimaryKey()->fields());
            auto batches = NArrow::NMerger::TRWSortableBatchPosition::SplitByBordersInIntervalPositions(OriginalBatch,
                Context.GetActualSchema()->GetIndexInfo().GetPrimaryKey()->field_names(), WriteData.GetData()->GetSeparationPoints());
            NColumnShard::TWriteResult wResult(WriteData.GetWriteMetaPtr(), WriteData.GetSize(), pkBatch, false, OriginalBatch->num_rows());
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
                portions.emplace_back(portionConclusion.DetachResult());
            }
            auto writeController = std::make_shared<NOlap::TPortionWriteController>(
                Context.GetTabletActorId(), WriteData.GetBlobsAction(), wResult, std::move(portions));
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
                OriginalBatch = NArrow::ToBatch(normalized->BuildTableVerified());
            }
        }
        auto batches = BuildSlices();
        if (batches) {
            auto writeDataPtr = std::make_shared<NEvWrite::TWriteData>(std::move(WriteData));
            writeDataPtr->SetSchemaSubset(std::move(subset));
            std::shared_ptr<arrow::RecordBatch> pkBatch =
                NArrow::TColumnOperator().Extract(OriginalBatch, Context.GetActualSchema()->GetIndexInfo().GetPrimaryKey()->fields());
            auto result = std::make_unique<NColumnShard::NWriting::TEvAddInsertedDataToBuffer>(writeDataPtr, std::move(*batches), pkBatch);
            TActorContext::AsActorContext().Send(Context.GetBufferizationInsertionActorId(), result.release());
        } else {
            ReplyError("Cannot slice input to batches", NColumnShard::TEvPrivate::TEvWriteBlobsResult::EErrorClass::Internal);
            return TConclusionStatus::Fail("Cannot slice input to batches");
        }
    }
    return TConclusionStatus::Success();
}
}   // namespace NKikimr::NOlap
