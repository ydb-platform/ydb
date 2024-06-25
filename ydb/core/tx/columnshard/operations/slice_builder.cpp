#include "slice_builder.h"
#include <ydb/library/actors/core/log.h>
#include <ydb/core/tx/columnshard/engines/writer/indexed_blob_constructor.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/data_reader/actor.h>
#include <ydb/core/tx/columnshard/engines/writer/buffer/events.h>
#include <ydb/core/formats/arrow/reader/result_builder.h>
#include <ydb/core/tx/conveyor/usage/service.h>
#include <ydb/core/formats/arrow/simple_arrays_cache.h>

namespace NKikimr::NOlap {

std::optional<std::vector<NKikimr::NArrow::TSerializedBatch>> TBuildSlicesTask::BuildSlices() {
    if (!OriginalBatch->num_rows()) {
        return std::vector<NKikimr::NArrow::TSerializedBatch>();
    }
    NArrow::TBatchSplitttingContext context(NColumnShard::TLimits::GetMaxBlobSize());
    context.SetFieldsForSpecialKeys(WriteData.GetPrimaryKeySchema());
    auto splitResult = NArrow::SplitByBlobSize(OriginalBatch, context);
    if (splitResult.IsFail()) {
        AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", TStringBuilder() << "cannot split batch in according to limits: " + splitResult.GetErrorMessage());
        return {};
    }
    auto result = splitResult.DetachResult();
    if (result.size() > 1) {
        for (auto&& i : result) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "strange_blobs_splitting")("blob", i.DebugString())("original_size", WriteData.GetSize());
        }
    }
    return result;
}

void TBuildSlicesTask::ReplyError(const TString& message) {
    auto writeDataPtr = std::make_shared<NEvWrite::TWriteData>(std::move(WriteData));
    TWritingBuffer buffer(writeDataPtr->GetBlobsAction(), { std::make_shared<TWriteAggregation>(writeDataPtr) });
    auto result = NColumnShard::TEvPrivate::TEvWriteBlobsResult::Error(
        NKikimrProto::EReplyStatus::CORRUPTED, std::move(buffer), message);
    TActorContext::AsActorContext().Send(ParentActorId, result.release());
}

bool TBuildSlicesTask::DoExecute() {
    NActors::TLogContextGuard g(NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tablet_id", TabletId)("parent_id", ParentActorId));
    if (!OriginalBatch) {
        AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "ev_write_bad_data")("write_id", WriteData.GetWriteMeta().GetWriteId())("table_id", WriteData.GetWriteMeta().GetTableId());
        ReplyError("no data in batch");
        return true;
    }
    const auto& indexSchema = ActualSchema->GetIndexInfo().ArrowSchema();
    {
        const auto batchSchema = OriginalBatch->schema();
        OriginalBatch = NArrow::ExtractColumns(OriginalBatch, indexSchema);
        if (!OriginalBatch) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "incompatible schemas")("batch", batchSchema->ToString())
                ("index", indexSchema->ToString());
            ReplyError("incompatible schemas");
            return true;
        }
    }
    if (!OriginalBatch->schema()->Equals(indexSchema)) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "unequal schemas")("batch", OriginalBatch->schema()->ToString())
            ("index", indexSchema->ToString());
        ReplyError("unequal schemas");
        return true;
    }

    WriteData.MutableWriteMeta().SetWriteMiddle2StartInstant(TMonotonic::Now());
    auto batches = BuildSlices();
    WriteData.MutableWriteMeta().SetWriteMiddle3StartInstant(TMonotonic::Now());
    if (batches) {
        auto writeDataPtr = std::make_shared<NEvWrite::TWriteData>(std::move(WriteData));
        auto result = std::make_unique<NColumnShard::NWriting::TEvAddInsertedDataToBuffer>(writeDataPtr, std::move(*batches));
        TActorContext::AsActorContext().Send(BufferActorId, result.release());
    } else {
        ReplyError("Cannot slice input to batches");
    }

    return true;
}

class IMerger {
private:
    NArrow::NMerger::TRWSortableBatchPosition IncomingPosition;

    virtual TConclusionStatus OnEqualKeys(const NArrow::NMerger::TSortableBatchPosition& exists, const NArrow::NMerger::TSortableBatchPosition& incoming) = 0;
    virtual TConclusionStatus OnIncomingOnly(const NArrow::NMerger::TSortableBatchPosition& incoming) = 0;
protected:
    std::shared_ptr<ISnapshotSchema> Schema;
    std::shared_ptr<arrow::RecordBatch> IncomingData;
    bool IncomingFinished = false;
public:
    IMerger(const std::shared_ptr<arrow::RecordBatch>& incoming, const std::shared_ptr<ISnapshotSchema>& actualSchema)
        : IncomingPosition(incoming, 0, actualSchema->GetPKColumnNames(), incoming->schema()->field_names(), false)
        , Schema(actualSchema)
        , IncomingData(incoming)
    {
        IncomingFinished = !IncomingPosition.InitPosition(0);
    }

    virtual ~IMerger() = default;

    virtual std::shared_ptr<arrow::RecordBatch> BuildResultBatch() = 0;

    TConclusionStatus Finish() {
        while (!IncomingFinished) {
            auto result = OnIncomingOnly(IncomingPosition);
            if (result.IsFail()) {
                return result;
            }
            IncomingFinished = !IncomingPosition.NextPosition(1);
        }
        return TConclusionStatus::Success();
    }

    TConclusionStatus AddExistsDataOrdered(const std::shared_ptr<arrow::Table>& data) {
        AFL_VERIFY(data);
        NArrow::NMerger::TRWSortableBatchPosition existsPosition(data, 0, Schema->GetPKColumnNames(),
            Schema->GetIndexInfo().GetColumnSTLNames(Schema->GetIndexInfo().GetColumnIds(false)), false);
        bool exsistFinished = !existsPosition.InitPosition(0);
        while (!IncomingFinished && !exsistFinished) {
            auto cmpResult = IncomingPosition.Compare(existsPosition);
            if (cmpResult == std::partial_ordering::equivalent) {
                auto result = OnEqualKeys(existsPosition, IncomingPosition);
                if (result.IsFail()) {
                    return result;
                }
                exsistFinished = !existsPosition.NextPosition(1);
                IncomingFinished = !IncomingPosition.NextPosition(1);
            } else if (cmpResult == std::partial_ordering::less) {
                auto result = OnIncomingOnly(IncomingPosition);
                if (result.IsFail()) {
                    return result;
                }
                IncomingFinished = !IncomingPosition.NextPosition(1);
            } else {
                AFL_VERIFY(false);
            }
        }
        AFL_VERIFY(exsistFinished);
        return TConclusionStatus::Success();
    }
};

class TInsertMerger: public IMerger {
private:
    using TBase = IMerger;
    virtual TConclusionStatus OnEqualKeys(const NArrow::NMerger::TSortableBatchPosition& exists, const NArrow::NMerger::TSortableBatchPosition& /*incoming*/) override {
        return TConclusionStatus::Fail("Conflict with existing key. " + exists.GetSorting()->DebugJson(exists.GetPosition()).GetStringRobust());
    }
    virtual TConclusionStatus OnIncomingOnly(const NArrow::NMerger::TSortableBatchPosition& /*incoming*/) override {
        return TConclusionStatus::Success();
    }
public:
    using TBase::TBase;
    virtual std::shared_ptr<arrow::RecordBatch> BuildResultBatch() override {
        return IncomingData;
    }
};

class TReplaceMerger: public IMerger {
private:
    using TBase = IMerger;
    NArrow::TColumnFilter Filter = NArrow::TColumnFilter::BuildDenyFilter();
    virtual TConclusionStatus OnEqualKeys(const NArrow::NMerger::TSortableBatchPosition& /*exists*/, const NArrow::NMerger::TSortableBatchPosition& /*incoming*/) override {
        Filter.Add(true);
        return TConclusionStatus::Success();
    }
    virtual TConclusionStatus OnIncomingOnly(const NArrow::NMerger::TSortableBatchPosition& /*incoming*/) override {
        Filter.Add(false);
        return TConclusionStatus::Success();
    }
public:
    using TBase::TBase;

    virtual std::shared_ptr<arrow::RecordBatch> BuildResultBatch() override {
        auto result = IncomingData;
        AFL_VERIFY(Filter.Apply(result));
        return result;
    }
};

class TUpdateMerger: public IMerger {
private:
    using TBase = IMerger;
    NArrow::NMerger::TRecordBatchBuilder Builder;
    std::vector<std::optional<ui32>> IncomingColumnRemap;
    std::vector<std::shared_ptr<arrow::BooleanArray>> HasIncomingDataFlags;
    const std::optional<NArrow::NMerger::TSortableBatchPosition> DefaultExists;
    virtual TConclusionStatus OnEqualKeys(const NArrow::NMerger::TSortableBatchPosition& exists, const NArrow::NMerger::TSortableBatchPosition& incoming) override {
        auto rGuard = Builder.StartRecord();
        AFL_VERIFY(Schema->GetIndexInfo().GetColumnIds(false).size() == exists.GetData().GetColumns().size())
            ("index", Schema->GetIndexInfo().GetColumnIds(false).size())("exists", exists.GetData().GetColumns().size());
        for (i32 columnIdx = 0; columnIdx < Schema->GetIndexInfo().ArrowSchema()->num_fields(); ++columnIdx) {
            const std::optional<ui32>& incomingColumnIdx = IncomingColumnRemap[columnIdx];
            if (incomingColumnIdx && HasIncomingDataFlags[*incomingColumnIdx]->GetView(incoming.GetPosition())) {
                const ui32 idxChunk = incoming.GetData().GetPositionInChunk(*incomingColumnIdx, incoming.GetPosition());
                rGuard.Add(*incoming.GetData().GetPositionAddress(*incomingColumnIdx).GetArray(), idxChunk);
            } else {
                const ui32 idxChunk = exists.GetData().GetPositionInChunk(columnIdx, exists.GetPosition());
                rGuard.Add(*exists.GetData().GetPositionAddress(columnIdx).GetArray(), idxChunk);
            }
        }
        return TConclusionStatus::Success();
    }
    virtual TConclusionStatus OnIncomingOnly(const NArrow::NMerger::TSortableBatchPosition& incoming) override {
        if (!DefaultExists) {
            return TConclusionStatus::Success();
        } else {
            return OnEqualKeys(*DefaultExists, incoming);
        }
    }
public:
    virtual std::shared_ptr<arrow::RecordBatch> BuildResultBatch() override {
        return Builder.Finalize();
    }

    TUpdateMerger(const std::shared_ptr<arrow::RecordBatch>& incoming, const std::shared_ptr<ISnapshotSchema>& actualSchema, 
        const std::optional<NArrow::NMerger::TSortableBatchPosition>& defaultExists = {})
        : TBase(incoming, actualSchema)
        , Builder(actualSchema->GetIndexInfo().ArrowSchema()->fields())
        , DefaultExists(defaultExists)
    {
        for (auto&& i : actualSchema->GetIndexInfo().ArrowSchema()->field_names()) {
            auto fIdx = IncomingData->schema()->GetFieldIndex(i);
            if (fIdx == -1) {
                IncomingColumnRemap.emplace_back();
            } else {
                auto fExistsIdx = IncomingData->schema()->GetFieldIndex("$$EXISTS::" + i);
                std::shared_ptr<arrow::Array> flagsArray;
                if (fExistsIdx != -1) {
                    AFL_VERIFY(IncomingData->column(fExistsIdx)->type_id() == arrow::Type::BOOL);
                    flagsArray = IncomingData->column(fExistsIdx);
                } else {
                    flagsArray = NArrow::TThreadSimpleArraysCache::GetConst(arrow::TypeTraits<arrow::BooleanType>::type_singleton(),
                        std::make_shared<arrow::BooleanScalar>(true), IncomingData->num_rows());
                }
                HasIncomingDataFlags.emplace_back(static_pointer_cast<arrow::BooleanArray>(flagsArray));
                IncomingColumnRemap.emplace_back(fIdx);
            }
        }
    }
};

class TModificationRestoreTask: public NDataReader::IRestoreTask {
private:
    using TBase = NDataReader::IRestoreTask;
    NEvWrite::TWriteData WriteData;
    const ui64 TabletId;
    const NActors::TActorId ParentActorId;
    const NActors::TActorId BufferActorId;
    std::shared_ptr<IMerger> Merger;
    const std::shared_ptr<ISnapshotSchema> ActualSchema;
    const ui64 LocalPathId;
    const TSnapshot Snapshot;
    std::shared_ptr<arrow::RecordBatch> IncomingData;
    virtual std::unique_ptr<TEvColumnShard::TEvInternalScan> DoBuildRequestInitiator() const override {
        auto request = std::make_unique<TEvColumnShard::TEvInternalScan>(LocalPathId);
        request->ReadToSnapshot = Snapshot;
        request->RangesFilter = std::make_shared<TPKRangesFilter>(false);
        auto pkData = NArrow::ExtractColumns(IncomingData, ActualSchema->GetPKColumnNames());
        for (ui32 i = 0; i < pkData->num_rows(); ++i) {
            auto batch = pkData->Slice(i, 1);
            auto pFrom = std::make_shared<NOlap::TPredicate>(NKernels::EOperation::GreaterEqual, batch);
            auto pTo = std::make_shared<NOlap::TPredicate>(NKernels::EOperation::LessEqual, batch);
            AFL_VERIFY(request->RangesFilter->Add(pFrom, pTo, &ActualSchema->GetIndexInfo()));
        }
        for (auto&& i : ActualSchema->GetIndexInfo().GetColumnIds(false)) {
            request->AddColumn(i, ActualSchema->GetIndexInfo().GetColumnName(i));
        }
        return request;
    }

    virtual TConclusionStatus DoOnDataChunk(const std::shared_ptr<arrow::Table>& data) override {
        auto result = Merger->AddExistsDataOrdered(data);
        if (result.IsFail()) {
            auto writeDataPtr = std::make_shared<NEvWrite::TWriteData>(std::move(WriteData));
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "restore_data_problems")
                ("write_id", WriteData.GetWriteMeta().GetWriteId())("tablet_id", TabletId)("message", result.GetErrorMessage());
            TWritingBuffer buffer(writeDataPtr->GetBlobsAction(), { std::make_shared<TWriteAggregation>(writeDataPtr) });
            auto evResult = NColumnShard::TEvPrivate::TEvWriteBlobsResult::Error(NKikimrProto::EReplyStatus::CORRUPTED, 
                std::move(buffer), result.GetErrorMessage());
            TActorContext::AsActorContext().Send(ParentActorId, evResult.release());
        }
        return result;
    }
    virtual TConclusionStatus DoOnFinished() override {
        {
            auto result = Merger->Finish();
            if (result.IsFail()) {
                return result;
            }
        }

        auto batchResult = Merger->BuildResultBatch();
        std::shared_ptr<NConveyor::ITask> task = std::make_shared<NOlap::TBuildSlicesTask>(
            TabletId, ParentActorId, BufferActorId, std::move(WriteData), batchResult, ActualSchema);
        NConveyor::TInsertServiceOperator::AsyncTaskToExecute(task);
        return TConclusionStatus::Success();
    }
public:
    TModificationRestoreTask(const ui64 tabletId, const NActors::TActorId parentActorId,
        const NActors::TActorId bufferActorId, NEvWrite::TWriteData&& writeData, const std::shared_ptr<IMerger>& merger, 
        const std::shared_ptr<ISnapshotSchema>& actualSchema, const TSnapshot actualSnapshot, const std::shared_ptr<arrow::RecordBatch>& incomingData)
        : TBase(tabletId, parentActorId)
        , WriteData(std::move(writeData))
        , TabletId(tabletId)
        , ParentActorId(parentActorId)
        , BufferActorId(bufferActorId)
        , Merger(merger)
        , ActualSchema(actualSchema)
        , LocalPathId(WriteData.GetWriteMeta().GetTableId())
        , Snapshot(actualSnapshot)
        , IncomingData(incomingData)
    {

    }
};

void TBuildBatchesTask::ReplyError(const TString& message) {
    auto writeDataPtr = std::make_shared<NEvWrite::TWriteData>(std::move(WriteData));
    TWritingBuffer buffer(writeDataPtr->GetBlobsAction(), { std::make_shared<TWriteAggregation>(writeDataPtr) });
    auto result = NColumnShard::TEvPrivate::TEvWriteBlobsResult::Error(
        NKikimrProto::EReplyStatus::CORRUPTED, std::move(buffer), message);
    TActorContext::AsActorContext().Send(ParentActorId, result.release());
}

bool TBuildBatchesTask::DoExecute() {
    std::shared_ptr<arrow::RecordBatch> batch = WriteData.GetData()->ExtractBatch();
    if (!batch) {
        ReplyError("cannot extract incoming batch");
        return true;
    }
    {
        auto validationResult = batch->ValidateFull();
        if (!validationResult.ok()) {
            ReplyError("incorrect incoming batch: " + validationResult.ToString());
            return true;
        }
    }
    const std::vector<std::shared_ptr<arrow::Field>> defaultFields = ActualSchema->GetAbsentFields(batch->schema());
    for (auto&& i : batch->schema()->fields()) {
        if (i->nullable() && !ActualSchema->GetIndexInfo().IsNullableVerified(i->name())) {
            ReplyError("nullable data for not null column " + i->name());
            return true;
        }
    }
    for (auto&& i : defaultFields) {
        if (!ActualSchema->GetIndexInfo().IsNullableVerified(i->name())) {
            ReplyError("not initialized not null column " + i->name());
            return true;
        }
    }
    std::shared_ptr<IMerger> merger;
    switch (WriteData.GetWriteMeta().GetModificationType()) {
        case NEvWrite::EModificationType::Replace: {
            batch = ActualSchema->AddDefault(batch);
            std::shared_ptr<NConveyor::ITask> task = std::make_shared<NOlap::TBuildSlicesTask>(
                TabletId, ParentActorId, BufferActorId, std::move(WriteData), batch, ActualSchema);
            NConveyor::TInsertServiceOperator::AsyncTaskToExecute(task);
            return true;
        }

        case NEvWrite::EModificationType::Upsert: {
            if (defaultFields.empty()) {
                std::shared_ptr<NConveyor::ITask> task = std::make_shared<NOlap::TBuildSlicesTask>(
                    TabletId, ParentActorId, BufferActorId, std::move(WriteData), batch, ActualSchema);
                NConveyor::TInsertServiceOperator::AsyncTaskToExecute(task);
                return true;
            } else {
                auto batchDefault = ActualSchema->BuildDefaultBatch(ActualSchema->GetIndexInfo().ArrowSchema()->fields(), 1);
                NArrow::NMerger::TSortableBatchPosition pos(batchDefault, 0, batchDefault->schema()->field_names(), batchDefault->schema()->field_names(), false);
                merger = std::make_shared<TUpdateMerger>(batch, ActualSchema, pos);
                break;
            }
        }
        case NEvWrite::EModificationType::Insert: {
            merger = std::make_shared<TInsertMerger>(batch, ActualSchema);
            break;
        }
        case NEvWrite::EModificationType::Update: {
            merger = std::make_shared<TUpdateMerger>(batch, ActualSchema);
            break;
        }
        case NEvWrite::EModificationType::Delete: {
            batch = ActualSchema->AddDefault(batch);
            std::shared_ptr<NConveyor::ITask> task = std::make_shared<NOlap::TBuildSlicesTask>(
                TabletId, ParentActorId, BufferActorId, std::move(WriteData), batch, ActualSchema);
            NConveyor::TInsertServiceOperator::AsyncTaskToExecute(task);
            return true;
        }

    }
    std::shared_ptr<NDataReader::IRestoreTask> task = std::make_shared<NOlap::TModificationRestoreTask>(
        TabletId, ParentActorId, BufferActorId, std::move(WriteData), merger, ActualSchema, ActualSnapshot, batch);
    NActors::TActivationContext::AsActorContext().Register(new NDataReader::TActor(task));

    return true;
}

}
