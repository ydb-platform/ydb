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

namespace NKikimr::NOlap {

void TBuildBatchesTask::ReplyError(const TString& message, const NColumnShard::TEvPrivate::TEvWriteBlobsResult::EErrorClass errorClass) {
    AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("problem", "cannot build batch for insert")("reason", message)(
        "data", WriteData.GetWriteMeta().GetLongTxIdOptional());
    auto writeDataPtr = std::make_shared<NEvWrite::TWriteData>(std::move(WriteData));
    TWritingBuffer buffer(writeDataPtr->GetBlobsAction(), { std::make_shared<TWriteAggregation>(*writeDataPtr) });
    auto result =
        NColumnShard::TEvPrivate::TEvWriteBlobsResult::Error(NKikimrProto::EReplyStatus::CORRUPTED, std::move(buffer), message, errorClass);
    TActorContext::AsActorContext().Send(Context.GetTabletActorId(), result.release());
}

void TBuildBatchesTask::DoExecute(const std::shared_ptr<ITask>& /*taskPtr*/) {
    const NActors::TLogContextGuard lGuard = NActors::TLogContextBuilder::Build()("scope", "TBuildBatchesTask::DoExecute");
    if (!Context.IsActive()) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD_WRITE)("event", "abort_external");
        ReplyError("writing aborted", NColumnShard::TEvPrivate::TEvWriteBlobsResult::EErrorClass::Internal);
        return;
    }
    TConclusion<std::shared_ptr<arrow::RecordBatch>> batchConclusion = WriteData.GetData()->ExtractBatch();
    if (batchConclusion.IsFail()) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD_WRITE)("event", "abort_on_extract")("reason", batchConclusion.GetErrorMessage());
        ReplyError("cannot extract incoming batch: " + batchConclusion.GetErrorMessage(),
            NColumnShard::TEvPrivate::TEvWriteBlobsResult::EErrorClass::Internal);
        return;
    }
    Context.GetWritingCounters()->OnIncomingData(NArrow::GetBatchDataSize(*batchConclusion));

    auto preparedConclusion =
        Context.GetActualSchema()->PrepareForModification(batchConclusion.DetachResult(), WriteData.GetWriteMeta().GetModificationType());
    if (preparedConclusion.IsFail()) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD_WRITE)("event", "abort_on_prepare")("reason", preparedConclusion.GetErrorMessage());
        ReplyError("cannot prepare incoming batch: " + preparedConclusion.GetErrorMessage(),
            NColumnShard::TEvPrivate::TEvWriteBlobsResult::EErrorClass::Request);
        return;
    }
    auto batch = preparedConclusion.DetachResult();
    std::shared_ptr<IMerger> merger;
    switch (WriteData.GetWriteMeta().GetModificationType()) {
        case NEvWrite::EModificationType::Upsert: {
            const std::vector<std::shared_ptr<arrow::Field>> defaultFields =
                Context.GetActualSchema()->GetAbsentFields(batch.GetContainer()->schema());
            if (defaultFields.empty()) {
                if (!Context.GetNoTxWrite()) {
                    std::shared_ptr<NConveyor::ITask> task =
                        std::make_shared<NOlap::TBuildSlicesTask>(std::move(WriteData), batch.GetContainer(), Context, ActualSnapshot);
                    NConveyorComposite::TInsertServiceOperator::SendTaskToExecute(task);
                } else {
                    NActors::TActivationContext::ActorSystem()->Send(Context.GetBufferizationPortionsActorId(),
                        new NWritingPortions::TEvAddInsertedDataToBuffer(
                            std::make_shared<NEvWrite::TWriteData>(WriteData), batch, std::make_shared<TWritingContext>(Context)));
                }
                return;
            } else {
                auto insertionConclusion = Context.GetActualSchema()->CheckColumnsDefault(defaultFields);
                auto conclusion = Context.GetActualSchema()->BuildDefaultBatch(Context.GetActualSchema()->GetIndexInfo().ArrowSchema(), 1, true);
                AFL_VERIFY(!conclusion.IsFail())("error", conclusion.GetErrorMessage());
                auto batchDefault = conclusion.DetachResult();
                NArrow::NMerger::TSortableBatchPosition pos(
                    batchDefault, 0, batchDefault->schema()->field_names(), batchDefault->schema()->field_names(), false);
                merger = std::make_shared<TUpdateMerger>(
                    batch, Context.GetActualSchema(), insertionConclusion.IsSuccess() ? "" : insertionConclusion.GetErrorMessage(), pos);
                break;
            }
        }
        case NEvWrite::EModificationType::Insert: {
            merger = std::make_shared<TInsertMerger>(batch, Context.GetActualSchema());
            break;
        }
        case NEvWrite::EModificationType::Update: {
            merger = std::make_shared<TUpdateMerger>(batch, Context.GetActualSchema(), "");
            break;
        }
        case NEvWrite::EModificationType::Increment: {
            merger = std::make_shared<TUpdateMerger>(batch, Context.GetActualSchema(), "");
            break;
        }
        case NEvWrite::EModificationType::Replace:
        case NEvWrite::EModificationType::Delete: {
            if (!Context.GetNoTxWrite()) {
                std::shared_ptr<NConveyor::ITask> task =
                    std::make_shared<NOlap::TBuildSlicesTask>(std::move(WriteData), batch.GetContainer(), Context, ActualSnapshot);
                NConveyorComposite::TInsertServiceOperator::SendTaskToExecute(task);
            } else {
                NActors::TActivationContext::ActorSystem()->Send(Context.GetBufferizationPortionsActorId(),
                    new NWritingPortions::TEvAddInsertedDataToBuffer(
                        std::make_shared<NEvWrite::TWriteData>(WriteData), batch, std::make_shared<TWritingContext>(Context)));
            }
            return;
        }
    }
    std::shared_ptr<NDataReader::IRestoreTask> task =
        std::make_shared<NOlap::TModificationRestoreTask>(std::move(WriteData), merger, ActualSnapshot, batch, Context);
    NActors::TActivationContext::AsActorContext().Register(new NDataReader::TActor(task));
}

}   // namespace NKikimr::NOlap
