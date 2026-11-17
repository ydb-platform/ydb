#include "builder.h"
#include "merger.h"
#include "restore.h"

#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/engines/writer/buffer/events.h>
#include <ydb/core/tx/columnshard/engines/writer/indexed_blob_constructor.h>
#include <ydb/core/tx/columnshard/operations/slice_builder/builder.h>
#include <ydb/core/tx/conveyor/usage/service.h>
#include <ydb/core/tx/conveyor_composite/usage/service.h>
#include <ydb/core/tx/data_events/common/modification_type.h>

#include <ydb/library/actors/struct_log/log_stack.h>

namespace NKikimr::NOlap {

namespace {

bool ReadsExistingRows(const NEvWrite::EModificationType modificationType, const NArrow::TContainerWithIndexes<arrow::RecordBatch>& batch,
    const TWritingContext& context) {
    // a snapshot-isolated blind write reads the rows committed after its snapshot to find conflicts
    const bool readsConflicts = !context.GetNoTxWrite() && context.IsSnapshotIsolated();
    switch (modificationType) {
        case NEvWrite::EModificationType::Upsert:
            return !context.GetActualSchema()->GetAbsentFields(batch.GetContainer()->schema()).empty() || readsConflicts;
        case NEvWrite::EModificationType::Insert:
        case NEvWrite::EModificationType::UpsertIncrement:
        case NEvWrite::EModificationType::Increment:
        case NEvWrite::EModificationType::Update:
            return true;
        case NEvWrite::EModificationType::Replace:
        case NEvWrite::EModificationType::Delete:
            return readsConflicts;
    }
}

}   // namespace

void TBuildBatchesTask::ReplyError(const TString& message, const NColumnShard::TEvPrivate::TEvWriteBlobsResult::EErrorClass errorClass) {
    YDB_LOG_ERROR_COMP(NKikimrServices::TX_COLUMNSHARD, "",
        {"problem", "cannot build batch for insert"},
        {"reason", message},
        {"data", WriteData.GetWriteMeta().GetLongTxIdOptional()});
    auto writeDataPtr = std::make_shared<NEvWrite::TWriteData>(std::move(WriteData));
    TWritingBuffer buffer(writeDataPtr->GetBlobsAction(), { std::make_shared<TWriteAggregation>(*writeDataPtr) });
    auto result =
        NColumnShard::TEvPrivate::TEvWriteBlobsResult::Error(NKikimrProto::EReplyStatus::CORRUPTED, std::move(buffer), message, errorClass);
    TActorContext::AsActorContext().Send(Context.GetTabletActorId(), result.release());
}

void TBuildBatchesTask::DoExecute(const std::shared_ptr<ITask>& /*taskPtr*/) {
    YDB_LOG_CREATE_CONTEXT(
        {"scope", "TBuildBatchesTask::DoExecute"});
    if (!Context.IsActive()) {
        YDB_LOG_WARN_COMP(NKikimrServices::TX_COLUMNSHARD_WRITE, "",
            {"event", "abort_external"});
        ReplyError(TStringBuilder{} << "writing aborted, reason: " << Context.GetErrorMessage(),
            NColumnShard::TEvPrivate::TEvWriteBlobsResult::EErrorClass::Internal);
        return;
    }
    TConclusion<std::shared_ptr<arrow::RecordBatch>> batchConclusion = WriteData.GetData()->ExtractBatch();
    if (batchConclusion.IsFail()) {
        YDB_LOG_WARN_COMP(NKikimrServices::TX_COLUMNSHARD_WRITE, "",
            {"event", "abort_on_extract"},
            {"reason", batchConclusion.GetErrorMessage()});
        ReplyError("cannot extract incoming batch: " + batchConclusion.GetErrorMessage(),
            NColumnShard::TEvPrivate::TEvWriteBlobsResult::EErrorClass::Internal);
        return;
    }
    Context.GetWritingCounters()->OnIncomingData(NArrow::GetBatchDataSize(*batchConclusion));

    auto preparedConclusion =
        Context.GetActualSchema()->PrepareForModification(batchConclusion.DetachResult(), WriteData.GetWriteMeta().GetModificationType());
    if (preparedConclusion.IsFail()) {
        YDB_LOG_WARN_COMP(NKikimrServices::TX_COLUMNSHARD_WRITE, "",
            {"event", "abort_on_prepare"},
            {"reason", preparedConclusion.GetErrorMessage()});
        ReplyError("cannot prepare incoming batch: " + preparedConclusion.GetErrorMessage(),
            NColumnShard::TEvPrivate::TEvWriteBlobsResult::EErrorClass::Request);
        return;
    }
    auto batch = preparedConclusion.DetachResult();
    if (Context.GetPassWriteGate()) {
        NArrow::TSimpleRow minKey(
            NArrow::TColumnOperator().VerifyIfAbsent().Extract(batch.GetContainer(), Context.GetActualSchema()->GetPKColumnNames()), 0);
        const bool readsExistingRows = ReadsExistingRows(WriteData.GetWriteMeta().GetModificationType(), batch, Context);
        TActorContext::AsActorContext().Send(Context.GetTabletActorId(),
            new TEvPassWriteGate(std::move(WriteData), std::move(batch), Context, std::move(minKey), readsExistingRows));
        return;
    }
    StartWritingPreparedBatch(std::move(WriteData), batch, Context);
}

void StartWritingPreparedBatch(
    NEvWrite::TWriteData&& writeData, const NArrow::TContainerWithIndexes<arrow::RecordBatch>& batch, const TWritingContext& context) {
    const NEvWrite::EModificationType modificationType = writeData.GetWriteMeta().GetModificationType();
    if (!ReadsExistingRows(modificationType, batch, context)) {
        if (context.GetNoTxWrite()) {
            NActors::TActivationContext::ActorSystem()->Send(context.GetBufferizationPortionsActorId(),
                new NWritingPortions::TEvAddInsertedDataToBuffer(
                    std::make_shared<NEvWrite::TWriteData>(writeData), batch, std::make_shared<TWritingContext>(context)));
        } else {
            std::shared_ptr<NConveyor::ITask> task =
                std::make_shared<NOlap::TBuildSlicesTask>(std::move(writeData), batch.GetContainer(), context);
            NConveyorComposite::TInsertServiceOperator::SendTaskToExecute(task);
        }
        return;
    }
    std::shared_ptr<IMerger> merger;
    bool readOnlyConflicts = false;
    switch (modificationType) {
        case NEvWrite::EModificationType::Upsert: {
            const std::vector<std::shared_ptr<arrow::Field>> defaultFields =
                context.GetActualSchema()->GetAbsentFields(batch.GetContainer()->schema());
            if (defaultFields.empty()) {
                merger = std::make_shared<TInsertMerger>(batch, context.GetActualSchema());
                readOnlyConflicts = true;
            } else {
                auto insertionConclusion = context.GetActualSchema()->CheckColumnsDefault(defaultFields);
                auto conclusion = context.GetActualSchema()->BuildDefaultBatch(context.GetActualSchema()->GetIndexInfo().ArrowSchema(), 1, true);
                AFL_VERIFY(!conclusion.IsFail())("error", conclusion.GetErrorMessage());
                auto batchDefault = conclusion.DetachResult();
                NArrow::NMerger::TSortableBatchPosition pos(
                    batchDefault, 0, batchDefault->schema()->field_names(), batchDefault->schema()->field_names(), false);
                merger = std::make_shared<TUpdateMerger>(
                    batch, context.GetActualSchema(), insertionConclusion.IsSuccess() ? "" : insertionConclusion.GetErrorMessage(), pos);
            }
            break;
        }
        case NEvWrite::EModificationType::Insert: {
            merger = std::make_shared<TInsertMerger>(batch, context.GetActualSchema());
            break;
        }
        case NEvWrite::EModificationType::UpsertIncrement:
        case NEvWrite::EModificationType::Increment:
        case NEvWrite::EModificationType::Update: {
            merger = std::make_shared<TUpdateMerger>(batch, context.GetActualSchema(), "");
            break;
        }
        case NEvWrite::EModificationType::Replace:
        case NEvWrite::EModificationType::Delete: {
            merger = std::make_shared<TInsertMerger>(batch, context.GetActualSchema());
            readOnlyConflicts = true;
            break;
        }
    }
    std::shared_ptr<NDataReader::IRestoreTask> task =
        std::make_shared<NOlap::TModificationRestoreTask>(std::move(writeData), merger, batch, context, readOnlyConflicts);
    NActors::TActivationContext::AsActorContext().Register(new NDataReader::TActor(task));
}

}   // namespace NKikimr::NOlap
