#include "pack_builder.h"

#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/defs.h>
#include <ydb/core/tx/columnshard/engines/portions/write_with_blobs.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/engines/writer/buffer/events.h>
#include <ydb/core/tx/columnshard/engines/writer/indexed_blob_constructor.h>

namespace NKikimr::NOlap {

std::optional<std::vector<NKikimr::NArrow::TSerializedBatch>> TBuildPackSlicesTask::BuildSlices() {
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

void TBuildPackSlicesTask::ReplyError(const TString& message, const NColumnShard::TEvPrivate::TEvWriteBlobsResult::EErrorClass errorClass) {
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
            : Portion(std::move(portion)){
        }
    };

private:
    const std::shared_ptr<IBlobsWritingAction> Action;
    std::vector<TInsertPortion> Portions;
    std::vector<NColumnShard::TWriteResult> WriteResults;
    TActorId DstActor;
    const ui64 DataSize;
    void DoOnReadyResult(const NActors::TActorContext& ctx, const NColumnShard::TBlobPutResult::TPtr& putResult) override {
        std::vector<NColumnShard::TInsertedPortion> portions;
        for (auto&& i : Portions) {
            portions.emplace_back(i.ExtractPortion());
        }
        NColumnShard::TInsertedPortions pack(std::move(WriteResults), std::move(portions));
        auto result = std::make_unique<NColumnShard::NPrivateEvents::NWrite::TEvWritePortionResult>(
            putResult->GetPutStatus(), Action, std::move(pack));
        ctx.Send(DstActor, result.release());
    }
    virtual void DoOnStartSending() override {
    }

public:
    TPortionWriteController(const TActorId& dstActor, const std::shared_ptr<IBlobsWritingAction>& action,
        std::vector<NColumnShard::TWriteResult>&& writeResults, std::vector<TInsertPortion>&& portions)
        : Action(action)
        , Portions(std::move(portions))
        , WriteResults(std::move(writeResults))
        , DstActor(dstActor) {
        for (auto&& p : Portions) {
            for (auto&& b : p.MutablePortion().MutableBlobs()) {
                auto& task = AddWriteTask(TBlobWriteInfo::BuildWriteTask(b.GetResultBlob(), action));
                b.RegisterBlobId(p.MutablePortion(), task.GetBlobId());
            }
        }
    }
};

class TSliceToMerge {
private:
    YDB_READONLY_DEF(std::vector<std::shared_ptr<arrow::RecordBatch>>, Batches);
    std::vector<ui64> SequentialWriteId;
    std::vector<std::shared_ptr<arrow::Schema>> Schemas;
    const ui64 PathId;
    const NEvWrite::EModificationType ModificationType;

public:
    TSliceToMerge(const ui64 pathId, const NEvWrite::EModificationType modificationType)
        : PathId(pathId)
        , ModificationType(modificationType) {
    }

    void Add(const std::shared_ptr<arrow::RecordBatch>& rb, const std::shared_ptr<TWriteData>& data) {
        if (!rb) {
            return;
        }
        Batches.emplace_back(rb);
        SequentialWriteId.emplace_back(data->GetWriteMeta().GetWriteId());
        Schemas.emplace_back(rb->schema());
    }

    void Finalize(const TWritingContext& context, std::vector<TPortionWriteController::TInsertPortion>& result) {
        if (Batches.size() == 0) {
            return;
        }
        if (Batches.size() == 1) {
            auto portionConclusion = Context.GetActualSchema()->PrepareForWrite(
                Context.GetActualSchema(), PathId, Batches.front(), ModificationType, Context->GetStoragesManager(), Context->GetSplitterCounters());
            result.emplace_back(portionConclusion.DetachResult());
        } else {
            ui32 idx = 0;
            std::vector<std::shared_ptr<NArrow::TGeneralContainer>> containers;
            ui32 recordsCountSum = 0;
            const std::shared_ptr<arrow::Schema> dataSchema = Batches.front()->schema();
            for (auto&& i : Batches) {
                auto gContainer = std::make_shared<TGeneralContainer>(i);
                recordsCountSum += i->num_rows();
                gContainer->AddField(IIndexInfo::GetWriteIdField(),
                    NArrow::TStatusValidator::GetValid(arrow::MakeArrayFromScalar(arrow::UInt64Scalar(SequentialWriteId[idx]), i->num_rows())));
                ++idx;
                containers.emplace_back(gContainer);
            }
            NArrow::NMerger::TMergePartialStream stream(context.GetActualSchema()->GetIndexInfo().GetReplaceKey(), dataSchema,
                false, { IIndexInfo::GetWriteIdField()->name() });
            for (auto&& i : containers) {
                stream.AddSource(i, nullptr);
            }
            NArrow::NMerger::TRecordBatchBuilder rbBuilder(dataSchema->fields(), recordsCountSum);
            stream.DrainAll(rbBuilder);
            auto portionConclusion = Context.GetActualSchema()->PrepareForWrite(Context.GetActualSchema(), PathId, rbBuilder.Finalize(),
                ModificationType, Context->GetStoragesManager(), Context->GetSplitterCounters());
            result.emplace_back(portionConclusion.DetachResult());
        }
    }
};

TConclusionStatus TBuildPackSlicesTask::DoExecute(const std::shared_ptr<ITask>& /*taskPtr*/) {
    const NActors::TLogContextGuard g =
        NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD_WRITE)("tablet_id", TabletId)("parent_id",
            Context.GetTabletActorId())("write_id", WriteData.GetWriteMeta().GetWriteId())("table_id", WriteData.GetWriteMeta().GetTableId());
    if (!Context.IsActive()) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD_WRITE)("event", "abort_execution");
        ReplyError("execution aborted", NColumnShard::TEvPrivate::TEvWriteBlobsResult::EErrorClass::Internal);
        return TConclusionStatus::Fail("execution aborted");
    }
    NColumnShard::TInsertedPortions portions;
    NArrow::NMerger::TIntervalPositions splitPositions;
    for (auto&& unit : WriteUnits) {
        splitPositions = splitPositions.Merge(WriteData.GetData()->GetSeparationPoints());
    }
    std::vector<TSliceToMerge> slicesToMerge;
    slicesToMerge.resize(splitPositions.GetPointsCount() + 1, TSliceToMerge(PathId, ModificationType));
    std::vector<NColumnShard::TWriteResult> writeResults;
    for (auto&& unit : WriteUnits) {
        const auto& originalBatch = unit.GetBatch();
        if (originalBatch->num_rows() == 0) {
            writeResults.emplace_back(i.GetData()->GetWriteMeta(), i.GetData()->GetDataSize(), nullptr, true);
            continue;
        }
        auto batches = NArrow::NMerger::TRWSortableBatchPosition::SplitByBordersInIntervalPositions(
            originalBatch, Context.GetActualSchema()->GetIndexInfo().GetPrimaryKey()->field_names(), splitPositions);
        std::shared_ptr<arrow::RecordBatch> pkBatch =
            NArrow::TColumnOperator().Extract(batch, Context.GetActualSchema()->GetIndexInfo().GetPrimaryKey()->fields());
        writeResults.emplace_back(i.GetData()->GetWriteMeta(), i.GetData()->GetDataSize(), pkBatch, false);
        ui32 idx = 0;
        for (auto&& batch : batches) {
            if (!!batch) {
                slicesToMerge[idx].Add(batch, unit.GetData());
            }
            ++idx;
        }
    }
    std::vector<TPortionWriteController::TInsertPortion> portionsToWrite;
    for (auto&& i : slicesToMerge) {
        i.Finalize(Context, portionsToWrite);
    }
    auto actions = WriteUnits.front().GetData()->GetBlobsAction();
    auto writeController = std::make_shared<NOlap::TPortionWriteController>(
        Context.GetTabletActorId(), actions, std::move(writeResults), std::move(portionsToWrite));
    if (actions->NeedDraftTransaction()) {
        TActorContext::AsActorContext().Send(
            Context.GetTabletActorId(), std::make_unique<NColumnShard::TEvPrivate::TEvWriteDraft>(writeController));
    } else {
        TActorContext::AsActorContext().Register(NColumnShard::CreateWriteActor(TabletId, writeController, TInstant::Max()));
    }
    return TConclusionStatus::Success();
}
}   // namespace NKikimr::NOlap
