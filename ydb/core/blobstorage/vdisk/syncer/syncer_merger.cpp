#include "syncer_merger.h"
#include "syncer_job_task.h"
#include "blobstorage_syncer_localwriter.h"
#include "blobstorage_syncer_data.h"

#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_generic_it.h>
#include <ydb/core/blobstorage/vdisk/hulldb/generic/blobstorage_hullwriteindexsst.h>
#include <ydb/core/blobstorage/vdisk/hulldb/fresh/fresh_appendix.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclogmsgreader.h>
#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NSyncer {

template <class TKey, class TMemRec>
struct TMergeBuffer {
    using TRecords = TFreshAppendix<TKey, TMemRec>::TVec;

    const TVDiskID VDiskId;
    std::queue<TRecords> Queue;
    bool Finished = false;

    explicit TMergeBuffer(const TVDiskID& vdiskId)
        : VDiskId(vdiskId)
    {}

    bool Empty() const {
        return Queue.empty() && Finished;
    }
};

template <class TKey, class TMemRec>
class TMergeBufferIterator {
public:
    using TContType = TMergeBuffer<TKey, TMemRec>;

    TContType* Buffer;
    size_t RecordIndex = 0;

public:
    TMergeBufferIterator(const THullCtxPtr& /*hullCtx*/, TContType* buffer)
        : Buffer(buffer)
    {}

    bool Exhausted() const {
        return Buffer->Queue.empty();
    }

    bool Valid() const {
        return !Exhausted() || !Buffer->Finished;
    }

    template <class TRecordMerger>
    void PutToMerger(TRecordMerger* merger) {
        Y_ABORT_UNLESS(Valid() && !Exhausted());
        auto& record = Buffer->Queue.front()[RecordIndex];
        merger->AddFromSegment(record.GetMemRec(), nullptr, record.GetKey(), 0, nullptr);
    }

    TKey GetCurKey() const {
        Y_ABORT_UNLESS(Valid() && !Exhausted());
        return Buffer->Queue.front()[RecordIndex].GetKey();
    }

    void Next() {
        Y_ABORT_UNLESS(Valid() && !Exhausted());
        ++RecordIndex;
        if (RecordIndex >= Buffer->Queue.front().size()) {
            Buffer->Queue.pop();
            RecordIndex = 0;
        }
    }

    TVDiskID GetVDiskId() const {
        return Buffer->VDiskId;
    }
};

template <class TKey, class TMemRec>
class TMergeIterator : public TGenericNWayForwardIterator<TKey, TMergeBufferIterator<TKey, TMemRec>> {
    using TBase = TGenericNWayForwardIterator<TKey, TMergeBufferIterator<TKey, TMemRec>>;
    using TIterContType = TBase::TIterContType;
    using TIterPtr = TBase::TIterPtr;
    using TBase::PQueue;
    using TBase::Iters;

    std::unordered_map<TVDiskID, std::shared_ptr<TIterContType>> Buffers;
    std::unordered_map<TVDiskID, TIterPtr> ExhaustedIterators;

public:
    TMergeIterator(const THullCtxPtr& hullCtx, const TVector<TIterContType*>& elements)
        : TBase(hullCtx, elements)
    {
        for (const auto& element : elements) {
            Buffers[element->VDiskId].reset(element);
        }
        for (const auto& it : Iters) {
            ExhaustedIterators.emplace(it->GetVDiskId(), it);
        }
    }

    void Next() {
        TIterPtr it = PQueue.top();
        PQueue.pop();
        Y_ABORT_UNLESS(it->Valid());
        TKey curKey = it->GetCurKey();
        it->Next();
        if (it->Valid()) {
            if (it->Exhausted()) {
                ExhaustedIterators.emplace(it->GetVDiskId(), it);
            } else {
                PQueue.push(it);
            }
        }

        while (!PQueue.empty() && curKey == ((it = PQueue.top())->GetCurKey())) {
            PQueue.pop();
            it->Next();
            if (it->Valid()) {
                if (it->Exhausted()) {
                    ExhaustedIterators.emplace(it->GetVDiskId(), it);
                } else {
                    PQueue.push(it);
                }
            }
        }
    }

    bool HasExhausted() const {
        return !ExhaustedIterators.empty();
    }

    void IterateExhausted(const std::function<void(const TVDiskID& vDiskId)>& callback) {
        for (const auto& [vDiskId, _] : ExhaustedIterators) {
            callback(vDiskId);
        }
    }

    void PushData(
            const TVDiskID& vDiskId,
            TMergeBuffer<TKey, TMemRec>::TRecords&& data)
    {
        Y_ABORT_UNLESS(Buffers.contains(vDiskId));
        Buffers[vDiskId]->Queue.push(std::move(data));

        Y_ABORT_UNLESS(ExhaustedIterators.contains(vDiskId));
        PQueue.push(ExhaustedIterators[vDiskId]);
        ExhaustedIterators.erase(vDiskId);
    }

    void EndOfStream(const TVDiskID& vDiskId) {
        Y_ABORT_UNLESS(Buffers.contains(vDiskId));
        Buffers[vDiskId]->Finished = true;
        ExhaustedIterators.erase(vDiskId);
    }
};

template <class TKey, class TMemRec>
class TIndexMerger {
    using TEvAddFullSyncSsts = TEvAddFullSyncSsts<TKey, TMemRec>;

    TIndexRecordMerger<TKey, TMemRec> RecordMerger;
    TIndexSstWriter<TKey, TMemRec> SstWriter;

    std::vector<std::shared_ptr<TMergeBuffer<TKey, TMemRec>>> Buffers;

    using TIterator = TMergeIterator<TKey, TMemRec>;
    std::unique_ptr<TIterator> Iterator;

    bool Finished = false;

public:
    enum class EState : ui64 {
        WaitData,
        WaitChunkReserve,
        Finished,
    };

public:
    TIndexMerger(
            TVDiskContextPtr vCtx,
            TPDiskCtxPtr pDiskCtx,
            const std::unordered_map<TVDiskID, TPeerSyncState>& syncStates,
            TIntrusivePtr<TLevelIndex<TKey, TMemRec>> levelIndex,
            TQueue<std::unique_ptr<NPDisk::TEvChunkWrite>>& msgQueue)
        : RecordMerger(vCtx->Top->GType)
        , SstWriter(vCtx, pDiskCtx, std::move(levelIndex), msgQueue)
    {
        TVector<TMergeBuffer<TKey, TMemRec>*> buffers;
        buffers.reserve(syncStates.size());
        for (const auto& [vDiskId, _] : syncStates) {
            auto buffer = std::make_shared<TMergeBuffer<TKey, TMemRec>>(vDiskId);
            Buffers.push_back(buffer);
            buffers.push_back(buffer.get());
        }
        Iterator = std::make_unique<TIterator>(nullptr /*hullCtx*/, buffers);
    }

    void PushData(
            const TVDiskID& vDiskId,
            TMergeBuffer<TKey, TMemRec>::TRecords&& records)
    {
        Iterator->PushData(vDiskId, std::move(records));
    }

    void EndOfStream(const TVDiskID& vDiskId) {
        Iterator->EndOfStream(vDiskId);
    }

    bool IsFinished() const {
        return Finished;
    }

    EState Progress() {
        if (Finished) {
            return EState::Finished;
        }
        if (Iterator->HasExhausted()) {
            return EState::WaitData;
        }
        while (Iterator->Valid()) {
            RecordMerger.Clear();
            Iterator->PutToMerger(&RecordMerger);
            RecordMerger.Finish();

            bool pushed = SstWriter.PushRecord(Iterator->GetCurKey(), RecordMerger.GetMemRec());
            Iterator->Next();

            if (!pushed) {
                return EState::WaitChunkReserve;
            }
            if (Iterator->HasExhausted()) {
                return EState::WaitData;
            }
        }
        SstWriter.Finish();
        Finished = true;
        return EState::Finished;
    }

    void IterateExhausted(const std::function<void(const TVDiskID& vDiskId)>& callback) {
        Iterator->IterateExhausted(callback);
    }

    void OnChunkReserved(ui32 chunkIdx) {
        SstWriter.OnChunkReserved(chunkIdx);
    }

    std::unique_ptr<TEvAddFullSyncSsts> GenerateCommitMessage(const TActorId sstWriterId) {
        return SstWriter.GenerateCommitMessage(sstWriterId);
    }

    TActorId GetLevelIndexActorId() const {
        return SstWriter.GetLevelIndexActorId();
    }
};

class TIndexMergerActor : public TActor<TIndexMergerActor> {
    using TSyncStatusVal = NKikimrVDiskData::TSyncerVDiskEntry;
    using ESyncStatus = TSyncStatusVal::ESyncStatus;

    TSyncerContextPtr SyncerCtx;
    TVDiskContextPtr VCtx;
    TPDiskCtxPtr PDiskCtx;
    TActorId SchedulerActorId;
    TIntrusivePtr<TBlobStorageGroupInfo> GInfo;
    TVDiskID SelfVDiskId;

    struct TSync {
        TActorId ActorId;
        NSyncer::TPeerSyncState Current;
        std::unique_ptr<TSyncerJobTask::TFullRecoverInfo> FullRecoverInfo;
        bool EndOfStream = false;
    };
    std::unordered_map<TVDiskID, TSync> Syncs;
    std::unordered_set<TVDiskID> VSyncFullInFlight;

    ui64 BytesReceived = 0;

    TIndexMerger<TKeyLogoBlob, TMemRecLogoBlob> LogoBlobMerger;
    TIndexMerger<TKeyBlock, TMemRecBlock> BlockMerger;
    TIndexMerger<TKeyBarrier, TMemRecBarrier> BarrierMerger;

    enum class EWriterType : ui64 {
        LOGOBLOBS,
        BLOCKS,
        BARRIERS,
    };

    TQueue<std::unique_ptr<NPDisk::TEvChunkWrite>> MsgQueue;

    bool InCommit = false;
    ui32 CommitsInFlight = 0;
    ui32 ReservesInFlight = 0;
    ui32 WritesInFlight = 0;
    static constexpr ui32 MaxWritesInFlight = 5;

    void ProcessWrites() {
        while (!MsgQueue.empty() && WritesInFlight < MaxWritesInFlight) {
            std::unique_ptr<NPDisk::TEvChunkWrite> msg = std::move(MsgQueue.front());
            STLOG(PRI_DEBUG, BS_SYNCER, BSFS12, VDISKP(VCtx->VDiskLogPrefix,
                "TIndexMergerActor: Send TEvChunkWrite"),
                (Msg, msg->ToString()));
            MsgQueue.pop();

            Send(PDiskCtx->PDiskId, msg.release());
            ++WritesInFlight;
        }

        STLOG(PRI_DEBUG, BS_SYNCER, BSFS01, VDISKP(VCtx->VDiskLogPrefix,
            "TIndexMergerActor: ProcessWrites"),
            (WritesInFlight, WritesInFlight), (ReservesInFlight, ReservesInFlight));

        if (WritesInFlight == 0 && ReservesInFlight == 0 && !InCommit) {
            if (LogoBlobMerger.IsFinished() && BlockMerger.IsFinished() && BarrierMerger.IsFinished()) {
                Commit();
            }
        }
    }

    void ReserveChunk(EWriterType type) {
        STLOG(PRI_DEBUG, BS_SYNCER, BSFS03, VDISKP(VCtx->VDiskLogPrefix,
            "TIndexMergerActor: Send ReserveChunk"),
            (Type, (ui64)type));

        auto msg = std::make_unique<NPDisk::TEvChunkReserve>(
            PDiskCtx->Dsk->Owner,
            PDiskCtx->Dsk->OwnerRound,
            1);

        Send(PDiskCtx->PDiskId, msg.release(), 0, (ui64)type);
        ++ReservesInFlight;
    }

    void SendVSyncFull(const TVDiskID& vDiskId) {
        auto& sync = Syncs[vDiskId];

        auto msg = std::make_unique<TEvBlobStorage::TEvVSyncFull>(
            sync.Current.SyncState,
            SelfVDiskId,
            vDiskId,
            sync.FullRecoverInfo->VSyncFullMsgsReceived,
            sync.FullRecoverInfo->Stage,
            sync.FullRecoverInfo->LogoBlobFrom.LogoBlobID(),
            ReadUnaligned<ui64>(&sync.FullRecoverInfo->BlockTabletFrom.TabletId),
            sync.FullRecoverInfo->BarrierFrom,
            sync.FullRecoverInfo->Protocol);

        VSyncFullInFlight.insert(vDiskId);

        SyncerCtx->MonGroup.SyncerVSyncFullBytesSent() += msg->GetCachedByteSize();
        ++SyncerCtx->MonGroup.SyncerVSyncFullMessagesSent();

        Send(sync.ActorId, msg.release());
    }

    void Commit() {
        InCommit = true;

        auto commit = [this]<class TMerger>(TMerger& merger) {
            auto msg = merger.GenerateCommitMessage(SelfId());
            if (msg) {
                STLOG(PRI_DEBUG, BS_SYNCER, BSFS05, VDISKP(VCtx->VDiskLogPrefix,
                    "TIndexMergerActor: Send commit"));

                Send(merger.GetLevelIndexActorId(), msg.release());
                ++CommitsInFlight;
            }
        };

        commit(LogoBlobMerger);
        commit(BlockMerger);
        commit(BarrierMerger);

        STLOG(PRI_DEBUG, BS_SYNCER, BSFS05, VDISKP(VCtx->VDiskLogPrefix,
            "TIndexMergerActor: Commit"),
            (CommitsInFlight, CommitsInFlight));

        if (CommitsInFlight == 0) {
            NotifySchedulerAndDie();
        }
    }

    void Progress() {
        auto progress = [this]<class TMerger>(TMerger& merger, EWriterType type) {
            if (merger.IsFinished()) {
                return false;
            }
            switch (merger.Progress()) {
            case TMerger::EState::WaitData:
                merger.IterateExhausted([this](const TVDiskID& vDiskId) {
                    if (!VSyncFullInFlight.contains(vDiskId)) {
                        SendVSyncFull(vDiskId);
                    }
                });
                return true;
            case TMerger::EState::WaitChunkReserve:
                ReserveChunk(type);
                return true;
            case TMerger::EState::Finished:
                return false;
            }
        };

        if (progress(LogoBlobMerger, EWriterType::LOGOBLOBS)) {
            ProcessWrites();
            return;
        }
        if (progress(BlockMerger, EWriterType::BLOCKS)) {
            ProcessWrites();
            return;
        }
        progress(BarrierMerger, EWriterType::BARRIERS);
        ProcessWrites();
    }

    void SyncError(const TVDiskID& vdiskId, TSyncerJobTask::ESyncStatus status) {
        Y_VERIFY_S(status != TSyncStatusVal::Running, VCtx->VDiskLogPrefix);
        auto& sync = Syncs[vdiskId];

        auto now = TAppData::TimeProvider->Now();
        sync.Current.LastSyncStatus = status;
        sync.Current.LastTry = now;
        if (NSyncer::TPeerSyncState::Good(status)) {
            sync.Current.LastGood = now;
        }

        STLOG(PRI_DEBUG, BS_SYNCER, BSFS27, VDISKP(VCtx->VDiskLogPrefix,
            "TIndexMergerActor: SyncError"),
            (VDiskId, vdiskId), (Status, status));

        // TODO: invalidate iterator for this vdisk
    }

    void NotifySchedulerAndDie() {
        auto msg = std::make_unique<TEvSyncerFullSyncFinished>();
        for (const auto& [vDiskId, sync] : Syncs) {
            msg->PeerSyncStates[vDiskId] = sync.Current;
        }
        Send(SchedulerActorId, msg.release());
        PassAway();
    }

    bool CheckFragmentFormat(const TString& data) {
        NSyncLog::TFragmentReader fragment(data);
        TString errorString;
        bool good = fragment.Check(errorString);
        if (!good) {
            STLOG(PRI_ERROR, BS_SYNCER, BSFS25, VDISKP(VCtx->VDiskLogPrefix,
                "TIndexMergerActor: CheckFragmentFormat"),
                (ErrorString, errorString));
        }
        return good;
    }

    void HandleOK(TEvBlobStorage::TEvVSyncFullResult::TPtr& ev) {
        const NKikimrBlobStorage::TEvVSyncFullResult& record = ev->Get()->Record;

        TVDiskID vDiskId = VDiskIDFromVDiskID(record.GetVDiskID());
        Y_VERIFY_S(Syncs.contains(vDiskId), VCtx->VDiskLogPrefix);
        auto& sync = Syncs[vDiskId];

        const TString& data = record.GetData();
        if (!CheckFragmentFormat(data)) {
            SyncError(vDiskId, TSyncStatusVal::ProtocolError);
            return;
        }

        TSyncState syncState = SyncStateFromSyncState(record.GetSyncState());

        sync.EndOfStream = record.GetFinished();
        sync.FullRecoverInfo->Stage = record.GetStage();
        sync.FullRecoverInfo->LogoBlobFrom = LogoBlobIDFromLogoBlobID(record.GetLogoBlobFrom());
        sync.FullRecoverInfo->BlockTabletFrom = record.GetBlockTabletFrom();
        sync.FullRecoverInfo->BarrierFrom = TKeyBarrier(record.GetBarrierFrom());

        if (record.HasProtocol()) {
            sync.FullRecoverInfo->Protocol = record.GetProtocol();
        } else {
            sync.FullRecoverInfo->Protocol = NKikimrBlobStorage::EFullSyncProtocol::Legacy;
        }

        if (sync.FullRecoverInfo->VSyncFullMsgsReceived == 1) {
            sync.Current.SyncState = syncState;
        } else {
            Y_VERIFY_S(sync.FullRecoverInfo->VSyncFullMsgsReceived > 1, VCtx->VDiskLogPrefix);
            Y_VERIFY_S(sync.Current.SyncState == syncState, VCtx->VDiskLogPrefix);
        }

        if (!data.empty()) {
            auto msg = std::make_unique<TEvLocalSyncData>(vDiskId, syncState, data);
            msg->UnpackData(VCtx);

            if (msg->Extracted.LogoBlobs && !msg->Extracted.LogoBlobs->Empty()) {
                auto data = std::move(msg->Extracted.LogoBlobs->Extract());
                LogoBlobMerger.PushData(vDiskId, std::move(data));
            }

            if (msg->Extracted.Blocks && !msg->Extracted.Blocks->Empty()) {
                auto data = std::move(msg->Extracted.Blocks->Extract());
                BlockMerger.PushData(vDiskId, std::move(data));
                LogoBlobMerger.EndOfStream(vDiskId);
            }

            if (msg->Extracted.Barriers && !msg->Extracted.Barriers->Empty()) {
                auto data = std::move(msg->Extracted.Barriers->Extract());
                BarrierMerger.PushData(vDiskId, std::move(data));
                LogoBlobMerger.EndOfStream(vDiskId);
                BlockMerger.EndOfStream(vDiskId);
            }

            if (sync.EndOfStream) {
                LogoBlobMerger.EndOfStream(vDiskId);
                BlockMerger.EndOfStream(vDiskId);
                BarrierMerger.EndOfStream(vDiskId);
            }
        } else {
            if (!sync.EndOfStream) {
                SyncError(vDiskId, TSyncStatusVal::ProtocolError);
            } else {
                LogoBlobMerger.EndOfStream(vDiskId);
                BlockMerger.EndOfStream(vDiskId);
                BarrierMerger.EndOfStream(vDiskId);
            }
        }

        Progress();
    }

    void Handle(TEvBlobStorage::TEvVSyncFullResult::TPtr& ev) {
        STLOG(PRI_DEBUG, BS_SYNCER, BSFS13, VDISKP(VCtx->VDiskLogPrefix,
            "TIndexMergerActor: Handle TEvVSyncFullResult"),
            (Msg, ev->Get()->ToString()));

        size_t bytesReceived = ev->Get()->GetCachedByteSize();
        SyncerCtx->MonGroup.SyncerVSyncFullBytesReceived() += bytesReceived;
        BytesReceived += bytesReceived;

        const NKikimrBlobStorage::TEvVSyncFullResult& record = ev->Get()->Record;
        TVDiskID vDiskId = VDiskIDFromVDiskID(record.GetVDiskID());
        Y_VERIFY_S(Syncs.contains(vDiskId), VCtx->VDiskLogPrefix);
        auto& sync = Syncs[vDiskId];

        VSyncFullInFlight.erase(vDiskId);

        Y_VERIFY_S(record.GetCookie() == sync.FullRecoverInfo->VSyncFullMsgsReceived,
            VCtx->VDiskLogPrefix);

        sync.FullRecoverInfo->VSyncFullMsgsReceived++;
        if (!SelfVDiskId.SameGroupAndGeneration(vDiskId)) {
            SyncError(vDiskId, TSyncStatusVal::Race);
            return;
        }

        NKikimrProto::EReplyStatus status = record.GetStatus();
        switch (status) {
            case NKikimrProto::OK:
                HandleOK(ev);
                return;
            case NKikimrProto::NODATA:
                SyncError(vDiskId, TSyncStatusVal::Race); // TODO: handle this case
                return;
            case NKikimrProto::RACE:
                SyncError(vDiskId, TSyncStatusVal::Race);
                return;
            case NKikimrProto::ERROR:
                SyncError(vDiskId, TSyncStatusVal::Error);
                return;
            case NKikimrProto::NOTREADY:
                SyncError(vDiskId, TSyncStatusVal::NotReady);
                return;
            case NKikimrProto::BLOCKED:
                SyncError(vDiskId, TSyncStatusVal::Locked);
                return;
            default: {
                SyncError(vDiskId, TSyncStatusVal::ProtocolError);
                return;
            }
        }
    }

    void Handle(NPDisk::TEvChunkWriteResult::TPtr& ev) {
        CHECK_PDISK_RESPONSE(VCtx, ev, TActivationContext::AsActorContext());

        STLOG(PRI_DEBUG, BS_SYNCER, BSFS09, VDISKP(VCtx->VDiskLogPrefix,
            "TIndexMergerActor: Handle TEvChunkWriteResult"));

        Y_VERIFY_S(WritesInFlight, VCtx->VDiskLogPrefix);
        --WritesInFlight;

        ProcessWrites();
    }

    void Handle(NPDisk::TEvChunkReserveResult::TPtr& ev) {
        CHECK_PDISK_RESPONSE(VCtx, ev, TActivationContext::AsActorContext());

        Y_VERIFY_S(ReservesInFlight, VCtx->VDiskLogPrefix);
        --ReservesInFlight;

        auto msg = ev->Get();
        Y_VERIFY_S(msg->ChunkIds.size() == 1, VCtx->VDiskLogPrefix);
        auto chunkId = msg->ChunkIds.front();
        auto type = (EWriterType)ev->Cookie;

        STLOG(PRI_DEBUG, BS_SYNCER, BSFS10, VDISKP(VCtx->VDiskLogPrefix,
            "TIndexMergerActor: Handle TEvChunkReserveResult"),
            (ChunkId, chunkId), (Type, (ui64)type));

        switch (type) {
            case EWriterType::LOGOBLOBS:
                LogoBlobMerger.OnChunkReserved(chunkId);
                break;
            case EWriterType::BLOCKS:
                BlockMerger.OnChunkReserved(chunkId);
                break;
            case EWriterType::BARRIERS:
                BarrierMerger.OnChunkReserved(chunkId);
                break;
        }

        Progress();
    }

    void Handle(TEvAddFullSyncSstsResult::TPtr&) {
        STLOG(PRI_DEBUG, BS_SYNCER, BSFS11, VDISKP(VCtx->VDiskLogPrefix,
            "TIndexMergerActor: Handle TEvAddFullSyncSstsResult"),
            (CommitsInFlight, CommitsInFlight));

        Y_VERIFY_S(CommitsInFlight, VCtx->VDiskLogPrefix);
        if (--CommitsInFlight == 0) {
            NotifySchedulerAndDie();
        }
    }

    void HandlePoison(TEvents::TEvPoisonPill::TPtr& , const TActorContext &ctx) {
        Die(ctx);
    }

    STRICT_STFUNC(MainFunc,
        hFunc(TEvBlobStorage::TEvVSyncFullResult, Handle)
        hFunc(NPDisk::TEvChunkReserveResult, Handle)
        hFunc(NPDisk::TEvChunkWriteResult, Handle)
        hFunc(TEvAddFullSyncSstsResult, Handle)
        HFunc(TEvents::TEvPoisonPill, HandlePoison)
    )

    PDISK_TERMINATE_STATE_FUNC_DEF;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::BS_SYNCER_MERGER;
    }

    TIndexMergerActor(
            TSyncerContextPtr syncerCtx,
            const TActorId& schedulerActorId,
            const std::unordered_map<TVDiskID, TPeerSyncState>& syncStates,
            const TIntrusivePtr<TBlobStorageGroupInfo>& info)
        : TActor(&TThis::MainFunc)
        , SyncerCtx(std::move(syncerCtx))
        , VCtx(SyncerCtx->VCtx)
        , PDiskCtx(SyncerCtx->PDiskCtx)
        , SchedulerActorId(schedulerActorId)
        , GInfo(info)
        , SelfVDiskId(GInfo->GetVDiskId(VCtx->ShortSelfVDisk))
        , LogoBlobMerger(VCtx, PDiskCtx, syncStates, SyncerCtx->LevelIndexLogoBlob, MsgQueue)
        , BlockMerger(VCtx, PDiskCtx, syncStates, SyncerCtx->LevelIndexBlock, MsgQueue)
        , BarrierMerger(VCtx, PDiskCtx, syncStates, SyncerCtx->LevelIndexBarrier, MsgQueue)
    {
        for (const auto& [vdiskId, syncState] : syncStates) {
            auto& sync = Syncs[vdiskId];
            sync.ActorId = GInfo->GetActorId(vdiskId);
            sync.Current = syncState;
            sync.Current.LastSyncStatus = TSyncStatusVal::FullRecover;
            sync.FullRecoverInfo = std::make_unique<TSyncerJobTask::TFullRecoverInfo>(
                NKikimrBlobStorage::EFullSyncProtocol::UnorderedData);
            sync.EndOfStream = false;
        }
    }
};

IActor* CreateIndexMergerActor(
        TSyncerContextPtr syncerCtx,
        const TActorId& schedulerActorId,
        const std::unordered_map<TVDiskID, TPeerSyncState>& syncStates,
        const TIntrusivePtr<TBlobStorageGroupInfo>& info)
{
    return new TIndexMergerActor(
        std::move(syncerCtx),
        schedulerActorId,
        syncStates,
        info);
}

} // namespace NKikimr::NSyncer
