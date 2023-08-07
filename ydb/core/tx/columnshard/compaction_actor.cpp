#include "columnshard_impl.h"
#include "blob_cache.h"

#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/conveyor/usage/events.h>
#include <ydb/core/tx/conveyor/usage/service.h>

using NKikimr::NOlap::TBlobRange;

namespace NKikimr::NColumnShard {
namespace {

class TCompactionActor: public TActorBootstrapped<TCompactionActor> {
private:
    const TIndexationCounters InternalCounters = TIndexationCounters("InternalCompaction");
    const TIndexationCounters SplitCounters = TIndexationCounters("SplitCompaction");
public:
    TCompactionActor(ui64 tabletId, const TActorId& parent)
        : TabletId(tabletId)
        , Parent(parent)
        , BlobCacheActorId(NBlobCache::MakeBlobCacheServiceId()) {
    }

    void Handle(TEvPrivate::TEvCompaction::TPtr& ev) {
        Y_VERIFY(!TxEvent);
        Y_VERIFY(Blobs.empty() && !NumRead);
        LastActivationTime = TAppData::TimeProvider->Now();
        auto& event = *ev->Get();
        TxEvent = std::move(event.TxEvent);
        auto compactChanges = dynamic_pointer_cast<NOlap::TCompactColumnEngineChanges>(TxEvent->IndexChanges);
        Y_VERIFY(compactChanges);
        IsSplitCurrently = compactChanges->IsSplit();

        auto& indexChanges = TxEvent->IndexChanges;
        Y_VERIFY(indexChanges);
        LOG_S_DEBUG("Granules compaction: " << *indexChanges << " at tablet " << TabletId);

        for (auto& [blobId, ranges] : event.GroupedBlobRanges) {
            Y_VERIFY(ranges.size());

            for (const auto& blobRange : ranges) {
                Y_VERIFY(blobId == blobRange.BlobId);
                Blobs[blobRange] = {};
                GetCurrentCounters().ReadBytes->Add(blobRange.Size);
            }
            NBlobCache::TReadBlobRangeOptions readOpts{ .CacheAfterRead = false, .ForceFallback = event.Externals.contains(blobId), .IsBackgroud = true };
            Send(BlobCacheActorId, new NBlobCache::TEvBlobCache::TEvReadBlobRangeBatch(std::move(ranges), std::move(readOpts)));
        }
    }

    void Handle(NBlobCache::TEvBlobCache::TEvReadBlobRangeResult::TPtr& ev) {
        LOG_S_TRACE("TEvReadBlobRangeResult (got " << NumRead << " of " << Blobs.size() << ") at tablet " << TabletId
                                                   << " (compaction)");

        auto& event = *ev->Get();
        const TBlobRange& blobId = event.BlobRange;
        Y_VERIFY(Blobs.contains(blobId));
        if (!Blobs[blobId].empty()) {
            return;
        }

        if (event.Status == NKikimrProto::EReplyStatus::OK) {
            Y_VERIFY(event.Data.size());

            TString blobData = event.Data;
            Y_VERIFY(blobData.size() == blobId.Size, "%u vs %u", (ui32)blobData.size(), blobId.Size);
            Blobs[blobId] = blobData;
        } else if (event.Status == NKikimrProto::EReplyStatus::NODATA) {
            Y_ASSERT(false);
            LOG_S_WARN("TEvReadBlobRangeResult cannot get blob "
                << blobId.ToString() << " status " << NKikimrProto::EReplyStatus_Name(event.Status) << " at tablet "
                << TabletId << " (compaction)");
            TxEvent->SetPutStatus(NKikimrProto::ERROR);
        } else {
            LOG_S_ERROR("TEvReadBlobRangeResult cannot get blob "
                        << blobId.ToString() << " status " << NKikimrProto::EReplyStatus_Name(event.Status) << " at tablet "
                        << TabletId << " (compaction)");
            if (event.Status == NKikimrProto::UNKNOWN) {
                TxEvent->SetPutStatus(NKikimrProto::ERROR);
            } else {
                TxEvent->SetPutStatus(event.Status);
            }
        }

        if (++NumRead == Blobs.size()) {
            CompactGranules();
            Clear();
        }
    }

    void Bootstrap() {
        Become(&TThis::StateWait);
    }

    STFUNC(StateWait) {
        TLogContextGuard gLogging(NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tablet_id", TabletId));
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvCompaction, Handle);
            hFunc(NBlobCache::TEvBlobCache::TEvReadBlobRangeResult, Handle);
            hFunc(NConveyor::TEvExecution::TEvTaskProcessedResult, Handle);
            default:
                break;
        }
    }

private:
    bool IsSplitCurrently = false;
    ui64 TabletId;
    TActorId Parent;
    TActorId BlobCacheActorId;
    std::unique_ptr<TEvPrivate::TEvWriteIndex> TxEvent;
    THashMap<TBlobRange, TString> Blobs;
    ui32 NumRead{0};
    TInstant LastActivationTime;

    void Clear() {
        TxEvent.reset();
        Blobs.clear();
        NumRead = 0;
    }

    const TIndexationCounters& GetCurrentCounters() const {
        if (IsSplitCurrently) {
            return SplitCounters;
        } else {
            return InternalCounters;
        }
    }

    class TConveyorTask: public NConveyor::ITask {
    private:
        std::unique_ptr<TEvPrivate::TEvWriteIndex> TxEvent;
        const TIndexationCounters Counters;
    protected:
        virtual bool DoExecute() override {
            auto guard = TxEvent->PutResult->StartCpuGuard();

            NOlap::TConstructionContext context(TxEvent->IndexInfo, Counters);
            TxEvent->Blobs = std::move(TxEvent->IndexChanges->ConstructBlobs(context).DetachResult());
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

    void CompactGranules() {
        Y_VERIFY(TxEvent);
        if (TxEvent->GetPutStatus() != NKikimrProto::EReplyStatus::UNKNOWN) {
            LOG_S_INFO("Granules compaction not started at tablet " << TabletId);
            Send(Parent, TxEvent.release());
            return;
        }
        LOG_S_DEBUG("Granules compaction started at tablet " << TabletId);
        TxEvent->IndexChanges->SetBlobs(std::move(Blobs));
        {
            std::shared_ptr<TConveyorTask> task = std::make_shared<TConveyorTask>(std::move(TxEvent), GetCurrentCounters());
            NConveyor::TCompServiceOperator::SendTaskToExecute(task);
        }
    }

    void Handle(NConveyor::TEvExecution::TEvTaskProcessedResult::TPtr& ev) {
        auto t = static_pointer_cast<TConveyorTask>(ev->Get()->GetResult());
        Y_VERIFY_DEBUG(dynamic_pointer_cast<TConveyorTask>(ev->Get()->GetResult()));
        auto txEvent = t->ExtractEvent();
        if (t->HasError()) {
            ACFL_ERROR("event", "task_error")("message", ev->Get()->GetErrorMessage());
            txEvent->SetPutStatus(NKikimrProto::ERROR);
            Send(Parent, txEvent.release());
        } else {
            ACFL_DEBUG("event", "task_finished")("new_blobs", txEvent->Blobs.size());
            if (txEvent->Blobs.empty()) {
                txEvent->SetPutStatus(NKikimrProto::OK); // nothing to write, commit
            }
            txEvent->Duration = TAppData::TimeProvider->Now() - LastActivationTime;
            Send(Parent, txEvent.release());
        }
    }
};

class TCompactionGroupActor: public TActorBootstrapped<TCompactionGroupActor> {
public:
    TCompactionGroupActor(ui64 tabletId, const TActorId& parent, const ui64 size)
        : TabletId(tabletId)
        , Parent(parent)
        , Idle(size == 0 ? 1 : size)
    {
    }

    void Bootstrap() {
        Become(&TThis::StateWait);

        for (auto& worker : Idle) {
            worker = Register(new TCompactionActor(TabletId, SelfId()));
        }
    }

    STFUNC(StateWait) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvents::TEvPoisonPill, Handle);
            hFunc(TEvPrivate::TEvWriteIndex, Handle);
            hFunc(TEvPrivate::TEvCompaction, Handle);
            default:
                break;
        }
    }

private:
    void Handle(TEvents::TEvPoisonPill::TPtr&) {
        for (const auto& worker : Active) {
            Send(worker, new TEvents::TEvPoisonPill);
        }
        for (const auto& worker : Idle) {
            Send(worker, new TEvents::TEvPoisonPill);
        }

        PassAway();
    }

    void Handle(TEvPrivate::TEvWriteIndex::TPtr& ev) {
        if (auto ai = Active.find(ev->Sender); ai != Active.end()) {
            Send(ev->Forward(Parent));
            Idle.push_back(*ai);
            Active.erase(ai);
        } else {
            Y_VERIFY(false);
        }
    }

    void Handle(TEvPrivate::TEvCompaction::TPtr& ev) {
        Y_VERIFY(!Idle.empty());

        Send(ev->Forward(Idle.back()));

        Active.insert(Idle.back());
        Idle.pop_back();
    }

private:
    const ui64 TabletId;
    const TActorId Parent;
    /// Set of workers perform compactions.
    std::unordered_set<TActorId> Active;
    /// List of idle workers.
    std::vector<TActorId> Idle;
};

} // namespace

IActor* CreateCompactionActor(ui64 tabletId, const TActorId& parent, const ui64 workers) {
    return new TCompactionGroupActor(tabletId, parent, workers);
}

} // namespace NKikimr::NColumnShard
