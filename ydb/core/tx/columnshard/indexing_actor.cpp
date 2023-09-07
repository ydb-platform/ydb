#include "blob_cache.h"
#include "columnshard_impl.h"
#include "engines/changes/indexation.h"
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/conveyor/usage/events.h>
#include <ydb/core/tx/conveyor/usage/service.h>

namespace NKikimr::NColumnShard {
namespace {

class TIndexingActor : public TActorBootstrapped<TIndexingActor> {
private:
    const TIndexationCounters Counters;
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TX_COLUMNSHARD_INDEXING_ACTOR;
    }

    TIndexingActor(ui64 tabletId, const TActorId& parent, const TIndexationCounters& counters)
        : Counters(counters)
        , TabletId(tabletId)
        , Parent(parent)
        , BlobCacheActorId(NBlobCache::MakeBlobCacheServiceId())
    {}

    void Handle(TEvPrivate::TEvIndexing::TPtr& ev) {
        LOG_S_DEBUG("TEvIndexing at tablet " << TabletId << " (index)");

        LastActivationTime = TAppData::TimeProvider->Now();
        auto& event = *ev->Get();
        TxEvent = std::move(event.TxEvent);
        Y_VERIFY(TxEvent);
        auto indexChanges = dynamic_pointer_cast<NOlap::TInsertColumnEngineChanges>(TxEvent->IndexChanges);
        Y_VERIFY(indexChanges);

        for (auto& [blobId, ranges] : event.GroupedBlobRanges) {
            Y_VERIFY(ranges.size() == 1);
            Y_VERIFY(BlobsToRead.emplace(ranges.front().BlobId).second);
            SendReadRequest(ranges.front());
            Counters.ReadBytes->Add(blobId.BlobSize());
        }

        if (BlobsToRead.empty()) {
            Index();
        }
    }

    void Handle(NBlobCache::TEvBlobCache::TEvReadBlobRangeResult::TPtr& ev) {
        LOG_S_TRACE("TEvReadBlobRangeResult (waiting " << BlobsToRead.size()
            << ") at tablet " << TabletId << " (index)");

        auto& event = *ev->Get();
        const TUnifiedBlobId& blobId = event.BlobRange.BlobId;
        if (event.Status != NKikimrProto::EReplyStatus::OK) {
            LOG_S_ERROR("TEvReadBlobRangeResult cannot get blob " << blobId
                << " status " << NKikimrProto::EReplyStatus_Name(event.Status)
                << " at tablet " << TabletId << " (index)");

            BlobsToRead.erase(blobId);
            if (event.Status == NKikimrProto::UNKNOWN) {
                TxEvent->SetPutStatus(NKikimrProto::ERROR);
            } else {
                TxEvent->SetPutStatus(event.Status);
            }
            return;
        }

        TString blobData = event.Data;
        Y_VERIFY(blobData.size() == blobId.BlobSize());

        if (!BlobsToRead.contains(blobId)) {
            return;
        }

        BlobsToRead.erase(blobId);

        Y_VERIFY(TxEvent);
        auto& indexChanges = TxEvent->IndexChanges;
        Y_VERIFY(indexChanges);
        indexChanges->Blobs[event.BlobRange] = blobData;

        if (BlobsToRead.empty()) {
            Index();
        }
    }

    void Bootstrap() {
        Become(&TThis::StateWait);
    }

    STFUNC(StateWait) {
        TLogContextGuard gLogging(NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tablet_id", TabletId));
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvIndexing, Handle);
            hFunc(NBlobCache::TEvBlobCache::TEvReadBlobRangeResult, Handle);
            hFunc(NConveyor::TEvExecution::TEvTaskProcessedResult, Handle)
            default:
                break;
        }
    }

private:
    ui64 TabletId;
    TActorId Parent;
    TActorId BlobCacheActorId;
    std::unique_ptr<TEvPrivate::TEvWriteIndex> TxEvent;
    THashSet<TUnifiedBlobId> BlobsToRead;
    TInstant LastActivationTime;

    void SendReadRequest(const NBlobCache::TBlobRange& blobRange) {
        Y_VERIFY(blobRange.Size);
        NBlobCache::TReadBlobRangeOptions readOpts {
            .CacheAfterRead = false,
            .ForceFallback = false,
            .IsBackgroud = true,
            .WithDeadline = false
        };
        Send(BlobCacheActorId, new NBlobCache::TEvBlobCache::TEvReadBlobRange(blobRange, std::move(readOpts)));
    }

    class TConveyorTask: public NConveyor::ITask {
    private:
        std::unique_ptr<TEvPrivate::TEvWriteIndex> TxEvent;
        const TIndexationCounters Counters;
    protected:
        virtual bool DoExecute() override {
            auto guard = TxEvent->PutResult->StartCpuGuard();

            NOlap::TConstructionContext context(TxEvent->IndexInfo, Counters);
            Y_VERIFY(TxEvent->IndexChanges->ConstructBlobs(context).Ok());
            return true;
        }
    public:
        virtual TString GetTaskClassIdentifier() const override {
            return "Changes::ConstructBlobs::" + TxEvent->IndexChanges->TypeString();
        }

        std::unique_ptr<TEvPrivate::TEvWriteIndex> ExtractEvent() {
            Y_VERIFY(TxEvent);
            return std::move(TxEvent);
        }

        TConveyorTask(std::unique_ptr<TEvPrivate::TEvWriteIndex>&& txEvent, const TIndexationCounters& counters)
            : TxEvent(std::move(txEvent))
            , Counters(counters)
        {
            Y_VERIFY(TxEvent);
        }
    };

    void Index() {
        Y_VERIFY(TxEvent);
        if (TxEvent->GetPutStatus() == NKikimrProto::UNKNOWN) {
            LOG_S_DEBUG("Indexing started at tablet " << TabletId);
            std::shared_ptr<TConveyorTask> task = std::make_shared<TConveyorTask>(std::move(TxEvent), Counters);
            NConveyor::TCompServiceOperator::SendTaskToExecute(task);
        } else {
            LOG_S_ERROR("Indexing failed at tablet " << TabletId);
            TxEvent->Duration = TAppData::TimeProvider->Now() - LastActivationTime;
            Send(Parent, TxEvent.release());
        }
    }

    void Handle(NConveyor::TEvExecution::TEvTaskProcessedResult::TPtr& ev) {
        auto t = static_pointer_cast<TConveyorTask>(ev->Get()->GetResult());
        Y_VERIFY_DEBUG(dynamic_pointer_cast<TConveyorTask>(ev->Get()->GetResult()));
        auto txEvent = t->ExtractEvent();
        if (t->HasError()) {
            ACFL_ERROR("event", "task_error")("message", t->GetErrorMessage());
        } else {
            LOG_S_DEBUG("Indexing finished at tablet " << TabletId);
        }
        txEvent->Duration = TAppData::TimeProvider->Now() - LastActivationTime;
        Send(Parent, txEvent.release());
    }
};

} // namespace

IActor* CreateIndexingActor(ui64 tabletId, const TActorId& parent, const TIndexationCounters& counters) {
    return new TIndexingActor(tabletId, parent, counters);
}

}
