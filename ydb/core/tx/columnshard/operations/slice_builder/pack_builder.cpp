#include "pack_builder.h"

#include <ydb/core/formats/arrow/reader/merger.h>
#include <ydb/core/formats/arrow/reader/result_builder.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/defs.h>
#include <ydb/core/tx/columnshard/engines/portions/write_with_blobs.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/engines/writer/buffer/events.h>
#include <ydb/core/tx/columnshard/engines/writer/indexed_blob_constructor.h>

namespace NKikimr::NOlap::NWritingPortions {

class TPortionWriteController: public NColumnShard::IWriteController,
                               public NColumnShard::TMonitoringObjectsCounter<TPortionWriteController, true> {
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
    std::vector<NColumnShard::TWriteResult> WriteResults;
    TActorId DstActor;
    void DoOnReadyResult(const NActors::TActorContext& ctx, const NColumnShard::TBlobPutResult::TPtr& putResult) override {
        std::vector<NColumnShard::TInsertedPortion> portions;
        for (auto&& i : Portions) {
            portions.emplace_back(i.ExtractPortion());
        }
        if (putResult->GetPutStatus() != NKikimrProto::OK) {
            for (auto&& i : WriteResults) {
                i.SetErrorMessage("cannot put blobs: " + ::ToString(putResult->GetPutStatus()), true);
            }
        }
        NColumnShard::TInsertedPortions pack(std::move(WriteResults), std::move(portions));
        auto result =
            std::make_unique<NColumnShard::NPrivateEvents::NWrite::TEvWritePortionResult>(putResult->GetPutStatus(), Action, std::move(pack));
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
    YDB_READONLY_DEF(std::vector<NArrow::TContainerWithIndexes<arrow::RecordBatch>>, Batches);
    std::vector<ui64> SequentialWriteId;
    const TInternalPathId PathId;
    const NEvWrite::EModificationType ModificationType;

public:
    TSliceToMerge(const TInternalPathId pathId, const NEvWrite::EModificationType modificationType)
        : PathId(pathId)
        , ModificationType(modificationType) {
    }

    void Add(const NArrow::TContainerWithIndexes<arrow::RecordBatch>& rb, const std::shared_ptr<NEvWrite::TWriteData>& data) {
        if (!rb) {
            return;
        }
        Batches.emplace_back(rb);
        SequentialWriteId.emplace_back(data->GetWriteMeta().GetWriteId());
    }

    [[nodiscard]] TConclusionStatus Finalize(
        const NOlap::TWritingContext& context, std::vector<TPortionWriteController::TInsertPortion>& result) {
        if (Batches.size() == 0) {
            return TConclusionStatus::Success();
        }
        if (Batches.size() == 1) {
            auto portionConclusion = context.GetActualSchema()->PrepareForWrite(context.GetActualSchema(), PathId,
                Batches.front().GetContainer(), ModificationType, context.GetStoragesManager(), context.GetSplitterCounters(), std::nullopt);
            if (portionConclusion.IsFail()) {
                AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "cannot prepare for write")("reason", portionConclusion.GetErrorMessage());
                return portionConclusion;
            }
            result.emplace_back(portionConclusion.DetachResult());
        } else {
            ui32 idx = 0;
            std::vector<std::shared_ptr<NArrow::TGeneralContainer>> containers;
            ui32 recordsCountSum = 0;
            auto indexes = NArrow::TOrderedColumnIndexesImpl::MergeColumnIdxs(Batches);
            std::shared_ptr<arrow::Schema> dataSchema;
            const auto& indexInfo = context.GetActualSchema()->GetIndexInfo();
            for (auto&& i : Batches) {
                std::shared_ptr<NArrow::TGeneralContainer> gContainer;
                if (i.GetColumnIndexes().size() == indexes.size()) {
                    if (!dataSchema) {
                        dataSchema = i->schema();
                    }
                    gContainer = std::make_shared<NArrow::TGeneralContainer>(i.GetContainer());
                } else {
                    gContainer = std::make_shared<NArrow::TGeneralContainer>(i->num_rows());
                    auto itBatchIndexes = i.GetColumnIndexes().begin();
                    AFL_VERIFY(i.GetColumnIndexes().size() < indexes.size());
                    for (auto itAllIndexes = indexes.begin(); itAllIndexes != indexes.end(); ++itAllIndexes) {
                        if (itBatchIndexes == i.GetColumnIndexes().end() || *itAllIndexes < *itBatchIndexes) {
                            auto defaultColumn = indexInfo.BuildDefaultColumn(*itAllIndexes, i->num_rows(), false);
                            if (defaultColumn.IsFail()) {
                                AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "cannot build default column")(
                                    "reason", defaultColumn.GetErrorMessage());
                                return defaultColumn;
                            }
                            gContainer->AddField(context.GetActualSchema()->GetFieldByIndexVerified(*itAllIndexes), defaultColumn.DetachResult())
                                .Validate();
                        } else {
                            AFL_VERIFY(*itAllIndexes == *itBatchIndexes);
                            gContainer
                                ->AddField(context.GetActualSchema()->GetFieldByIndexVerified(*itAllIndexes),
                                    i->column(itBatchIndexes - i.GetColumnIndexes().begin()))
                                .Validate();
                            ++itBatchIndexes;
                        }
                    }
                }
                //                AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_WRITE)("data", NArrow::DebugJson(i, 5, 5))("write_id", SequentialWriteId[idx]);
                recordsCountSum += i->num_rows();
                gContainer
                    ->AddField(IIndexInfo::GetWriteIdField(), NArrow::TStatusValidator::GetValid(arrow::MakeArrayFromScalar(
                                                                  arrow::UInt64Scalar(SequentialWriteId[idx]), i->num_rows())))
                    .Validate();
                ++idx;
                containers.emplace_back(gContainer);
            }
            if (!dataSchema) {
                dataSchema = indexInfo.GetColumnsSchemaByOrderedIndexes(indexes);
            }
            NArrow::NMerger::TMergePartialStream stream(context.GetActualSchema()->GetIndexInfo().GetReplaceKey(), dataSchema, false,
                { IIndexInfo::GetWriteIdField()->name() }, std::nullopt);
            for (auto&& i : containers) {
                stream.AddSource(i, nullptr, NArrow::NMerger::TIterationOrder::Forward(0));
            }
            NArrow::NMerger::TRecordBatchBuilder rbBuilder(dataSchema->fields(), recordsCountSum);
            stream.DrainAll(rbBuilder);
            auto portionConclusion = context.GetActualSchema()->PrepareForWrite(context.GetActualSchema(), PathId, rbBuilder.Finalize(),
                ModificationType, context.GetStoragesManager(), context.GetSplitterCounters(), std::nullopt);
            if (portionConclusion.IsFail()) {
                AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "cannot prepare for write")("reason", portionConclusion.GetErrorMessage());
                return portionConclusion;
            }
            result.emplace_back(portionConclusion.DetachResult());
        }
        return TConclusionStatus::Success();
    }
};

void TBuildPackSlicesTask::DoExecute(const std::shared_ptr<ITask>& /*taskPtr*/) {
    const NActors::TLogContextGuard g = NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD_WRITE)("tablet_id", TabletId)(
        "parent_id", Context.GetTabletActorId())("path_id", PathId);
    NArrow::NMerger::TIntervalPositions splitPositions;
    for (auto&& unit : WriteUnits) {
        splitPositions.Merge(unit.GetData()->GetData()->GetSeparationPoints());
    }
    std::vector<TSliceToMerge> slicesToMerge;
    slicesToMerge.resize(splitPositions.GetPointsCount() + 1, TSliceToMerge(PathId, ModificationType));
    std::vector<NColumnShard::TWriteResult> writeResults;
    for (auto&& unit : WriteUnits) {
        const auto& originalBatch = unit.GetBatch();
        if (originalBatch->num_rows() == 0) {
            unit.GetData()->GetWriteMetaPtr()->OnStage(NEvWrite::EWriteStage::PackSlicesReady);
            writeResults.emplace_back(unit.GetData()->GetWriteMetaPtr(), unit.GetData()->GetSize(), nullptr, true, 0);
            continue;
        }
        auto batches = NArrow::NMerger::TRWSortableBatchPosition::SplitByBordersInIntervalPositions(
            originalBatch.GetContainer(), Context.GetActualSchema()->GetIndexInfo().GetPrimaryKey()->field_names(), splitPositions);
        std::shared_ptr<arrow::RecordBatch> pkBatch =
            NArrow::TColumnOperator().Extract(originalBatch.GetContainer(), Context.GetActualSchema()->GetIndexInfo().GetPrimaryKey()->fields());
        writeResults.emplace_back(unit.GetData()->GetWriteMetaPtr(), unit.GetData()->GetSize(), pkBatch, false, originalBatch->num_rows());
        ui32 idx = 0;
        for (auto&& batch : batches) {
            if (!!batch) {
                slicesToMerge[idx].Add(originalBatch.BuildWithAnotherContainer(batch), unit.GetData());
            }
            ++idx;
        }
    }
    std::vector<TPortionWriteController::TInsertPortion> portionsToWrite;
    TString cancelWritingReason;
    if (!Context.IsActive()) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD_WRITE)("event", "abort_execution");
        cancelWritingReason = "execution aborted";
    } else {
        for (auto&& i : slicesToMerge) {
            auto conclusion = i.Finalize(Context, portionsToWrite);
            if (conclusion.IsFail()) {
                AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "cannot build slice")("reason", conclusion.GetErrorMessage());
                cancelWritingReason = conclusion.GetErrorMessage();
                break;
            }
        }
    }
    if (!cancelWritingReason) {
        for (auto&& unit : WriteUnits) {
            unit.GetData()->GetWriteMetaPtr()->OnStage(NEvWrite::EWriteStage::PackSlicesReady);
        }

        auto actions = WriteUnits.front().GetData()->GetBlobsAction();
        auto writeController =
            std::make_shared<TPortionWriteController>(Context.GetTabletActorId(), actions, std::move(writeResults), std::move(portionsToWrite));
        if (actions->NeedDraftTransaction()) {
            TActorContext::AsActorContext().Send(
                Context.GetTabletActorId(), std::make_unique<NColumnShard::TEvPrivate::TEvWriteDraft>(writeController));
        } else {
            TActorContext::AsActorContext().Register(NColumnShard::CreateWriteActor(TabletId, writeController, TInstant::Max()));
        }
    } else {
        for (auto&& unit : WriteUnits) {
            unit.GetData()->GetWriteMetaPtr()->OnStage(NEvWrite::EWriteStage::PackSlicesError);
        }
        for (auto&& i : writeResults) {
            i.SetErrorMessage(cancelWritingReason, false);
        }
        NColumnShard::TInsertedPortions pack(std::move(writeResults), std::vector<NColumnShard::TInsertedPortion>());
        auto result =
            std::make_unique<NColumnShard::NPrivateEvents::NWrite::TEvWritePortionResult>(NKikimrProto::EReplyStatus::ERROR, nullptr, std::move(pack));
        TActorContext::AsActorContext().Send(Context.GetTabletActorId(), result.release());

    }
}
}   // namespace NKikimr::NOlap::NWritingPortions
