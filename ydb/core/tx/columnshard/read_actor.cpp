#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/blob_cache.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>
#include <ydb/core/tx/columnshard/blobs_reader/events.h>
#include <ydb/core/tx/conveyor/usage/events.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>

namespace NKikimr::NColumnShard {
namespace {

class TReadActor : public TActorBootstrapped<TReadActor> {
private:
    void BuildResult(const TActorContext& ctx) {
        auto ready = IndexedData->ExtractReadyResults(Max<i64>());
        LOG_S_TRACE("Ready results with " << ready.size() << " batches at tablet " << TabletId << " (read)");
        if (ready.empty()) {
            if (IndexedData->IsFinished()) {
                SendResult(ctx, nullptr, true);
            }
        } else {
            size_t next = 1;
            for (auto it = ready.begin(); it != ready.end(); ++it, ++next) {
                const bool lastOne = IndexedData->IsFinished() && (next == ready.size());
                SendResult(ctx, it->GetResultBatchPtrVerified(), lastOne);
            }
        }
    }
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TX_COLUMNSHARD_READ_ACTOR;
    }

    TReadActor(ui64 tabletId, const NActors::TActorId readBlobsActor,
               const std::shared_ptr<NOlap::IStoragesManager>& storages,
               const TActorId& dstActor,
               std::unique_ptr<TEvColumnShard::TEvReadResult>&& event,
               NOlap::TReadMetadata::TConstPtr readMetadata,
               const TInstant& deadline,
               const TActorId& columnShardActorId,
               ui64 requestCookie, const TConcreteScanCounters& counters)
        : TabletId(tabletId)
        , ReadBlobsActor(readBlobsActor)
        , Storages(storages)
        , DstActor(dstActor)
        , BlobCacheActorId(NBlobCache::MakeBlobCacheServiceId())
        , Result(std::move(event))
        , ReadMetadata(readMetadata)
        , Deadline(deadline)
        , ColumnShardActorId(columnShardActorId)
        , RequestCookie(requestCookie)
        , ReturnedBatchNo(0)
        , Counters(counters)
    {}

    void Handle(NConveyor::TEvExecution::TEvTaskProcessedResult::TPtr& ev, const TActorContext& ctx) {
        if (ev->Get()->GetErrorMessage()) {
            ACFL_DEBUG("event", "TEvTaskProcessedResult")("error", ev->Get()->GetErrorMessage());
            SendErrorResult(ctx, NKikimrTxColumnShard::EResultStatus::ERROR);
            return DieFinished(ctx);
        } else {
            ACFL_DEBUG("event", "TEvTaskProcessedResult");
            auto t = static_pointer_cast<IDataTasksProcessor::ITask>(ev->Get()->GetResult());
            Y_VERIFY_DEBUG(dynamic_pointer_cast<IDataTasksProcessor::ITask>(ev->Get()->GetResult()));
            if (!IndexedData->IsFinished()) {
                Y_VERIFY(t->Apply(*IndexedData));
            }
            BuildResult(ctx);
            if (IndexedData->IsFinished()) {
                DieFinished(ctx);
            }
        }
    }

    void Handle(TEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ev);
        LOG_S_INFO("TEvWakeup: read timeout at tablet " << TabletId << " (read)");

        SendTimeouts(ctx);
        DieFinished(ctx);
    }

    void SendErrorResult(const TActorContext& ctx, NKikimrTxColumnShard::EResultStatus status) {
        Y_VERIFY(status != NKikimrTxColumnShard::EResultStatus::SUCCESS);
        SendResult(ctx, {}, true, status);
        IndexedData->Abort();
    }

    void SendResult(const TActorContext& ctx, const std::shared_ptr<arrow::RecordBatch>& batch, bool finished = false,
                    NKikimrTxColumnShard::EResultStatus status = NKikimrTxColumnShard::EResultStatus::SUCCESS) {
        auto chunkEvent = std::make_unique<TEvColumnShard::TEvReadResult>(*Result);
        auto& proto = Proto(chunkEvent.get());

        TString data;
        if (batch && batch->num_rows()) {
            data = NArrow::SerializeBatchNoCompression(batch);

            auto metadata = proto.MutableMeta();
            metadata->SetFormat(NKikimrTxColumnShard::FORMAT_ARROW);
            metadata->SetSchema(GetSerializedSchema(batch));
            if (status == NKikimrTxColumnShard::EResultStatus::SUCCESS) {
                Y_VERIFY(!data.empty());
            }
        }

        proto.SetBatch(ReturnedBatchNo);
        proto.SetData(data);
        proto.SetStatus(status);
        proto.SetFinished(finished);
        ++ReturnedBatchNo;

        if (finished) {
            auto stats = ReadMetadata->ReadStats;
            auto* protoStats = proto.MutableMeta()->MutableReadStats();
            protoStats->SetBeginTimestamp(stats->BeginTimestamp.MicroSeconds());
            protoStats->SetDurationUsec(stats->Duration().MicroSeconds());
            protoStats->SetSelectedIndex(stats->SelectedIndex);
            protoStats->SetIndexGranules(stats->IndexGranules);
            protoStats->SetIndexPortions(stats->IndexPortions);
            protoStats->SetIndexBatches(stats->IndexBatches);
            protoStats->SetNotIndexedBatches(stats->CommittedBatches);
            protoStats->SetSchemaColumns(stats->SchemaColumns);
            protoStats->SetFilterColumns(stats->FilterColumns);
            protoStats->SetAdditionalColumns(stats->AdditionalColumns);
            protoStats->SetDataFilterBytes(stats->DataFilterBytes);
            protoStats->SetDataAdditionalBytes(stats->DataAdditionalBytes);
            protoStats->SetPortionsBytes(stats->PortionsBytes);
            protoStats->SetSelectedRows(stats->SelectedRows);
        }

        if (Deadline != TInstant::Max()) {
            TInstant now = TAppData::TimeProvider->Now();
            if (Deadline <= now) {
                proto.SetStatus(NKikimrTxColumnShard::EResultStatus::TIMEOUT);
            }
        }

        ctx.Send(DstActor, chunkEvent.release());
    }

    void DieFinished(const TActorContext& ctx) {
        if (IndexedData->IsFinished()) {
            LOG_S_DEBUG("Finished read (with " << ReturnedBatchNo << " batches sent) at tablet " << TabletId);
            Send(ColumnShardActorId, new TEvPrivate::TEvReadFinished(RequestCookie));
            Die(ctx);
        }
    }

    void Bootstrap(const TActorContext& ctx) {
        IndexedData = ReadMetadata->BuildReader(NOlap::TReadContext(Storages, Counters, true), ReadMetadata);
        LOG_S_DEBUG("Starting read (" << IndexedData->DebugString() << ") at tablet " << TabletId);

        bool earlyExit = false;
        if (Deadline != TInstant::Max()) {
            TInstant now = TAppData::TimeProvider->Now();
            if (Deadline <= now) {
                earlyExit = true;
            } else {
                const TDuration timeout = Deadline - now;
                ctx.Schedule(timeout, new TEvents::TEvWakeup());
            }
        }

        if (earlyExit) {
            SendTimeouts(ctx);
            ctx.Send(SelfId(), new TEvents::TEvPoisonPill());
        } else {
            while (auto task = IndexedData->ExtractNextReadTask(false)) {
                Send(ReadBlobsActor, std::make_unique<NOlap::NBlobOperations::NRead::TEvStartReadTask>(task));
            }
            BuildResult(ctx);
        }

        Become(&TThis::StateWait);
    }

    void SendTimeouts(const TActorContext& ctx) {
        SendErrorResult(ctx, NKikimrTxColumnShard::EResultStatus::TIMEOUT);
    }

    STFUNC(StateWait) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NConveyor::TEvExecution::TEvTaskProcessedResult, Handle);
            HFunc(TEvents::TEvWakeup, Handle);
            default:
                break;
        }
    }

private:
    ui64 TabletId;
    TActorId ReadBlobsActor;
    std::shared_ptr<NOlap::IStoragesManager> Storages;
    TActorId DstActor;
    TActorId BlobCacheActorId;
    std::unique_ptr<TEvColumnShard::TEvReadResult> Result;
    NOlap::TReadMetadata::TConstPtr ReadMetadata;
    std::shared_ptr<NOlap::IDataReader> IndexedData;
    TInstant Deadline;
    TActorId ColumnShardActorId;
    const ui64 RequestCookie;
    ui32 ReturnedBatchNo;
    const TConcreteScanCounters Counters;
    mutable TString SerializedSchema;

    TString GetSerializedSchema(const std::shared_ptr<arrow::RecordBatch>& batch) const {
        auto resultSchema = ReadMetadata->GetResultSchema();
        Y_VERIFY(resultSchema);

        // TODO: make real ResultSchema with SSA effects
        if (resultSchema->Equals(batch->schema())) {
            if (!SerializedSchema.empty()) {
                return SerializedSchema;
            }
            SerializedSchema = NArrow::SerializeSchema(*resultSchema);
            return SerializedSchema;
        }

        return NArrow::SerializeSchema(*batch->schema());
    }
};

} // namespace

IActor* CreateReadActor(ui64 tabletId,
    const NActors::TActorId readBlobsActor, const TActorId& dstActor, const std::shared_ptr<NOlap::IStoragesManager>& storages,
                        std::unique_ptr<TEvColumnShard::TEvReadResult>&& event,
                        NOlap::TReadMetadata::TConstPtr readMetadata,
                        const TInstant& deadline,
                        const TActorId& columnShardActorId,
                        ui64 requestCookie, const TConcreteScanCounters& counters)
{
    return new TReadActor(tabletId, readBlobsActor, storages, dstActor, std::move(event), readMetadata,
                          deadline, columnShardActorId, requestCookie, counters);
}

}
