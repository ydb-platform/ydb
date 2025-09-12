#include "helpers.h"

#include <util/generic/set.h>
#include <util/random/shuffle.h>

#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_private_events.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclog.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclogreader.h>
#include <ydb/core/blobstorage/backpressure/queue_backpressure_client.h>

using namespace NKikimr;

static inline TString LimitData(const TString &data) {
    return data.size() <= 16 ? data : (data.substr(0, 16) + "...");
}


////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// PDisk Gen status handlers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////
template <class TMsg>
bool PDiskGenStatusHandlerDefault(const TActorContext &ctx,
                                  const TActorId &notifyId,
                                  typename TMsg::TPtr &ev) {
    Y_UNUSED(notifyId);
    Y_ABORT_UNLESS(ev->Get()->Record.GetStatus() == NKikimrProto::OK, "Status=%d (%s)", ev->Get()->Record.GetStatus(),
            NKikimrProto::EReplyStatus_Name(ev->Get()->Record.GetStatus()).c_str());
    LOG_DEBUG(ctx, NActorsServices::TEST, "  TEvVPutResult succeded");
    return false;
}

template <class TMsg>
bool PDiskGenStatusHandlerErrorAware(const TActorContext &ctx,
                                     const TActorId &notifyId,
                                     typename TMsg::TPtr &ev) {
    auto status = ev->Get()->Record.GetStatus();
    if (status != NKikimrProto::OK) {
        // Finished
        ctx.Send(notifyId, new TEvents::TEvCompleted(0, status));
        return true;
    }
    return false;
}

template <class TMsg>
bool PDiskGenStatusHandleYellowMoveZone(const TActorContext &ctx,
                                    const TActorId &notifyId,
                                    typename TMsg::TPtr &ev) {
    auto status = ev->Get()->Record.GetStatus();
    auto flags = ev->Get()->Record.GetStatusFlags();

    Y_ABORT_UNLESS(status == NKikimrProto::OK, "Status=%d (%s)", status, NKikimrProto::EReplyStatus_Name(status).c_str());
    if (flags & NKikimrBlobStorage::EStatusFlags::StatusDiskSpaceLightYellowMove) {
        // Finished
        ctx.Send(notifyId, new TEvents::TEvCompleted(0, 0x28733642)); // signature
        return true;
    }
    return false;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// PDisk Put status handlers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////
bool PDiskPutStatusHandlerDefault(const TActorContext &ctx,
                                  const TActorId &notifyId,
                                  TEvBlobStorage::TEvVPutResult::TPtr &ev) {
    return PDiskGenStatusHandlerDefault<TEvBlobStorage::TEvVPutResult>(ctx, notifyId, ev);
}

bool PDiskPutStatusHandlerErrorAware(const TActorContext &ctx,
                                     const TActorId &notifyId,
                                     TEvBlobStorage::TEvVPutResult::TPtr &ev) {
    return PDiskGenStatusHandlerErrorAware<TEvBlobStorage::TEvVPutResult>(ctx, notifyId, ev);
}

bool PDiskPutStatusHandlerYellowMoveZone(const NActors::TActorContext &ctx,
                                     const NActors::TActorId &notifyId,
                                     NKikimr::TEvBlobStorage::TEvVPutResult::TPtr &ev) {
    return PDiskGenStatusHandleYellowMoveZone<TEvBlobStorage::TEvVPutResult>(ctx, notifyId, ev);
}


////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// PDisk Get status handlers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////
bool PDiskGetStatusHandlerDefault(const TActorContext &ctx,
                                  const TActorId &notifyId,
                                  TEvBlobStorage::TEvVGetResult::TPtr &ev) {
    return PDiskGenStatusHandlerDefault<TEvBlobStorage::TEvVGetResult>(ctx, notifyId, ev);}

bool PDiskGetStatusHandlerErrorAware(const TActorContext &ctx,
                                     const TActorId &notifyId,
                                     TEvBlobStorage::TEvVGetResult::TPtr &ev) {
    return PDiskGenStatusHandlerErrorAware<TEvBlobStorage::TEvVGetResult>(ctx, notifyId, ev);
}




///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TRangeGet : public TActorBootstrapped<TRangeGet> {
    const TActorId NotifyID;
    const TAllVDisks::TVDiskInstance VDiskInfo;
    const TLogoBlobID ReadFrom;
    const TLogoBlobID ReadTo;
    const ui32 MaxResults;

    friend class TActorBootstrapped<TRangeGet>;

    void Bootstrap(const TActorContext &ctx) {
        Become(&TThis::StateFunc);

        LOG_NOTICE(ctx, NActorsServices::TEST,
                   "  RANGE READ: from=%s to=%s\n", ReadFrom.ToString().data(), ReadTo.ToString().data());

        auto req = TEvBlobStorage::TEvVGet::CreateRangeIndexQuery(VDiskInfo.VDiskID,
                                                                  TInstant::Max(),
                                                                  NKikimrBlobStorage::EGetHandleClass::FastRead,
                                                                  TEvBlobStorage::TEvVGet::EFlags::None,
                                                                  {},
                                                                  ReadFrom,
                                                                  ReadTo,
                                                                  MaxResults);

        ctx.Send(VDiskInfo.ActorID, req.release());
    }

    void CheckOrder(const NKikimrBlobStorage::TEvVGetResult &rec,
                    bool (*cmp)(const TLogoBlobID &, const TLogoBlobID &)) {
        ui32 size = rec.GetResult().size();
        if (size > 0) {
            const NKikimrBlobStorage::TQueryResult &q = rec.GetResult(0);
            TLogoBlobID id = LogoBlobIDFromLogoBlobID(q.GetBlobID());
            for (ui32 i = 1; i < size; i++) {
                const NKikimrBlobStorage::TQueryResult &qq = rec.GetResult(i);
                const TLogoBlobID nextId = LogoBlobIDFromLogoBlobID(qq.GetBlobID());
                Y_ABORT_UNLESS(cmp(id, nextId));
                id = nextId;
            }
        }
    }

    void Handle(TEvBlobStorage::TEvVGetResult::TPtr &ev, const TActorContext &ctx) {
        Y_ABORT_UNLESS(ev->Get()->Record.GetStatus() == NKikimrProto::OK);
        LOG_NOTICE(ctx, NActorsServices::TEST, "  TEvVGetResult succeded");

        // check result order
        const NKikimrBlobStorage::TEvVGetResult &rec = ev->Get()->Record;
        if (ReadFrom < ReadTo) {
            // forward order
            auto lbless = [](const TLogoBlobID &x, const TLogoBlobID &y) {return x < y; };
            CheckOrder(rec, lbless);
        } else {
            // backward order
            auto lbgreater = [](const TLogoBlobID &x, const TLogoBlobID &y) {return x > y; };
            CheckOrder(rec, lbgreater);
        }

        ctx.Send(NotifyID, new TEvents::TEvCompleted());
        Die(ctx);
    }

    STRICT_STFUNC(StateFunc,
        HFunc(TEvBlobStorage::TEvVGetResult, Handle);
        IgnoreFunc(TEvBlobStorage::TEvVWindowChange);
    )

public:
    TRangeGet(const TActorId &notifyID, const TAllVDisks::TVDiskInstance &vdiskInfo,
               const TLogoBlobID &readFrom, const TLogoBlobID &readTo, bool /*indexOnly*/, ui32 maxResults)
        : TActorBootstrapped<TRangeGet>()
        , NotifyID(notifyID)
        , VDiskInfo(vdiskInfo)
        , ReadFrom(readFrom)
        , ReadTo(readTo)
        , MaxResults(maxResults)
    {}
};

IActor *CreateRangeGet(const TActorId &notifyID, const TAllVDisks::TVDiskInstance &vdiskInfo,
                       const TLogoBlobID &readFrom, const TLogoBlobID &readTo, bool indexOnly, ui32 maxResults) {
    Y_ABORT_UNLESS(indexOnly);
    return new TRangeGet(notifyID, vdiskInfo, readFrom, readTo, indexOnly, maxResults);
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TManyPuts : public TActorBootstrapped<TManyPuts> {
    struct TPut {
        ui64 Step;
        TString Data;
    };

    TConfiguration *Conf;
    TActorId NotifyID;
    const TAllVDisks::TVDiskInstance VDiskInfo;

    std::shared_ptr<TVector<TMsgPackInfo>> MsgPacks;
    ui32 MsgNum;

    const ui64 TabletId;
    const ui32 Channel;
    const ui32 Gen;
    std::shared_ptr<IPutHandleClassGenerator> HandleClassGen;
    std::shared_ptr<TSet<ui32>> BadSteps;
    const TDuration RequestTimeout;
    TVector<TPut> Puts;
    ui32 PutIdx;
    TActorId QueueActorId;
    bool Started = false;
    // how many deadline statuses we got
    ui64 RequestDeadlines = 0;

    friend class TActorBootstrapped<TManyPuts>;

    void Bootstrap(const TActorContext &ctx) {
        Become(&TThis::StateWriteFunc);

        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters = new ::NMonitoring::TDynamicCounters;
        TIntrusivePtr<NBackpressure::TFlowRecord> flowRecord(new NBackpressure::TFlowRecord);
        QueueActorId = ctx.Register(CreateVDiskBackpressureClient(Conf->GroupInfo, VDiskInfo.VDiskID,
            NKikimrBlobStorage::PutTabletLog, counters, new TBSProxyContext{counters}, {}, "PutTabletLog", 0, false,
            TDuration::Minutes(10), flowRecord, NMonitoring::TCountableBase::EVisibility::Public));
    }

    void Handle(TEvProxyQueueState::TPtr& ev, const TActorContext& ctx) {
        if (ev->Get()->IsConnected && !Started) {
            // put logo blob
            SendPut(ctx);
            Started = true;
        }
    }

    void Finish(const TActorContext &ctx) {
        // Finished
        const bool noTimeout = RequestTimeout == TDuration::Seconds(0);
        if (!noTimeout) {
            // got some deadlines
            Y_ABORT_UNLESS(RequestDeadlines);
        }
        ui32 msgsSent = MsgNum - BadSteps->size();
        LOG_NOTICE(ctx, NActorsServices::TEST, "TOTALLY SENT %u messages", msgsSent);
        ctx.Send(NotifyID, new TEvents::TEvCompleted());
        Die(ctx);
    }

    bool IsEnd() {
        return PutIdx == MsgNum;
    }

    void SendPut(const TActorContext &ctx) {
        // put logo blob
        while (!IsEnd()) {
            if (PutIdx % 100 == 0)
                LOG_NOTICE(ctx, NActorsServices::TEST, "PUT PutIdx=%u", PutIdx);

            const TPut &put = Puts[PutIdx];

            TLogoBlobID logoBlobID(TabletId, Gen, put.Step, Channel, put.Data.size(), 0, 1);
            TVDiskIdShort mainVDiskId = TIngress::GetMainReplica(&Conf->GroupInfo->GetTopology(), logoBlobID);
            if (mainVDiskId == VDiskInfo.VDiskID) {
                const bool noTimeout = RequestTimeout == TDuration::Seconds(0);
                const TInstant deadline = noTimeout ? TInstant::Max() : TInstant::Now() + RequestTimeout;
                ctx.Send(QueueActorId,
                         new TEvBlobStorage::TEvVPut(logoBlobID, TRope(put.Data), VDiskInfo.VDiskID, false,
                                                     nullptr, deadline, HandleClassGen->GetHandleClass()));
                return;
            } else {
                BadSteps->insert(put.Step);
                PutIdx++;
            }
        }

        Finish(ctx);
    }

    void Handle(TEvBlobStorage::TEvVPutResult::TPtr &ev, const TActorContext &ctx) {
        const bool noTimeout = RequestTimeout == TDuration::Seconds(0);
        auto status = ev->Get()->Record.GetStatus();
        if (noTimeout) {
            Y_ABORT_UNLESS(status == NKikimrProto::OK, "Event# %s", ev->Get()->ToString().data());
        } else {
            Y_ABORT_UNLESS(status == NKikimrProto::OK || status == NKikimrProto::DEADLINE,
                "Event# %s", ev->Get()->ToString().data());
            if (status == NKikimrProto::DEADLINE) {
                ++RequestDeadlines;
            }
        }

        PutIdx++;
        if (IsEnd()) {
            Finish(ctx);
        } else {
            SendPut(ctx);
        }
    }

    STRICT_STFUNC(StateWriteFunc,
        HFunc(TEvBlobStorage::TEvVPutResult, Handle);
        HFunc(TEvProxyQueueState, Handle);
    )

    void Init() {
        MsgNum = 0;
        for (auto & el: *MsgPacks) {
            MsgNum += el.Count;
        }
        Puts.reserve(MsgNum);
        ui64 msgIdx = 0;
        for (ui32 packIdx = 0; packIdx < MsgPacks->size(); ++packIdx) {
            for (ui32 i = 0; i < MsgPacks->at(packIdx).Count; ++i) {
                Puts.push_back({msgIdx, MsgPacks->at(packIdx).MsgData});
                msgIdx++;
            }
        }
        Shuffle(Puts.begin(), Puts.end());
        Y_ABORT_UNLESS(Puts.size() == MsgNum);
    }

public:
    TManyPuts(TConfiguration *conf, const TActorId &notifyID, const TAllVDisks::TVDiskInstance &vdiskInfo,
              ui32 msgDataSize, ui32 msgNum, ui64 tabletId, ui32 channel, ui32 gen,
              std::shared_ptr<IPutHandleClassGenerator> cls, std::shared_ptr<TSet<ui32>> badSteps,
              TDuration requestTimeout)
        : TActorBootstrapped<TManyPuts>()
        , Conf(conf)
        , NotifyID(notifyID)
        , VDiskInfo(vdiskInfo)
        , MsgPacks(new TVector<TMsgPackInfo>{TMsgPackInfo(msgDataSize, msgNum)})
        , TabletId(tabletId)
        , Channel(channel)
        , Gen(gen)
        , HandleClassGen(cls)
        , BadSteps(badSteps)
        , RequestTimeout(requestTimeout)
        , PutIdx(0)
    {
        Init();
    }

    TManyPuts(TConfiguration *conf, const TActorId &notifyID, const TAllVDisks::TVDiskInstance &vdiskInfo,
              std::shared_ptr<TVector<TMsgPackInfo>> msgPacks, ui64 tabletId, ui32 channel, ui32 gen,
              std::shared_ptr<IPutHandleClassGenerator> cls, std::shared_ptr<TSet<ui32>> badSteps,
              TDuration requestTimeout)
        : TActorBootstrapped<TManyPuts>()
        , Conf(conf)
        , NotifyID(notifyID)
        , VDiskInfo(vdiskInfo)
        , MsgPacks(msgPacks)
        , TabletId(tabletId)
        , Channel(channel)
        , Gen(gen)
        , HandleClassGen(cls)
        , BadSteps(badSteps)
        , RequestTimeout(requestTimeout)
        , PutIdx(0)
    {
        Init();
    }
};

IActor *CreateManyPuts(TConfiguration *conf, const TActorId &notifyID, const TAllVDisks::TVDiskInstance &vdiskInfo,
                       ui32 msgDataSize, ui32 msgNum, ui64 tabletId, ui32 channel, ui32 gen,
                       std::shared_ptr<IPutHandleClassGenerator> cls, std::shared_ptr<TSet<ui32>> badSteps,
                       TDuration requestTimeout) {
    return new TManyPuts(conf, notifyID, vdiskInfo, msgDataSize, msgNum, tabletId, channel, gen, cls, badSteps,
        requestTimeout);
}

IActor *CreateManyPuts(TConfiguration *conf, const TActorId &notifyID, const TAllVDisks::TVDiskInstance &vdiskInfo,
                       std::shared_ptr<TVector<TMsgPackInfo>> msgPacks, ui64 tabletId, ui32 channel, ui32 gen,
                       std::shared_ptr<IPutHandleClassGenerator> cls, std::shared_ptr<TSet<ui32>> badSteps,
                       TDuration requestTimeout) {
    return new TManyPuts(conf, notifyID, vdiskInfo, msgPacks, tabletId, channel, gen, cls, badSteps, requestTimeout);
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TManyMultiPuts : public TActorBootstrapped<TManyMultiPuts> {
    TConfiguration *Conf;
    TActorId NotifyID;
    const TAllVDisks::TVDiskInstance VDiskInfo;
    const ui32 MsgDataSize;
    const ui32 MsgNum;
    const ui32 BatchSize;
    const ui64 TabletId;
    const ui32 Channel;
    const ui32 Gen;
    std::shared_ptr<IPutHandleClassGenerator> HandleClassGen;
    std::shared_ptr<TSet<ui32>> BadSteps;
    const TDuration RequestTimeout;
    TVector<ui32> Steps;
    ui32 Step;
    TString MsgData;
    TActorId QueueActorId;
    bool Started = false;
    // how many deadline statuses we got
    ui64 RequestDeadlines = 0;
    ui32 MinREALHugeBlobInBytes = 0;

    ui64 LastBatchSize = 0;

    friend class TActorBootstrapped<TManyMultiPuts>;

    void Bootstrap(const TActorContext &ctx) {
        Become(&TThis::StateWriteFunc);

        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters = new ::NMonitoring::TDynamicCounters;
        TIntrusivePtr<NBackpressure::TFlowRecord> flowRecord(new NBackpressure::TFlowRecord);
        QueueActorId = ctx.Register(CreateVDiskBackpressureClient(Conf->GroupInfo, VDiskInfo.VDiskID,
            NKikimrBlobStorage::PutTabletLog, counters, new TBSProxyContext{counters}, {}, "PutTabletLog", 0, false,
            TDuration::Minutes(10), flowRecord, NMonitoring::TCountableBase::EVisibility::Public));
    }

    void Handle(TEvProxyQueueState::TPtr& ev, const TActorContext& ctx) {
        if (ev->Get()->IsConnected && !Started) {
            // put logo blob
            MinREALHugeBlobInBytes = ev->Get()->CostModel->MinREALHugeBlobInBytes;
            Y_ABORT_UNLESS(MinREALHugeBlobInBytes);
            SendPut(ctx);
            Started = true;
        }
    }

    void Finish(const TActorContext &ctx) {
        // Finished
        const bool noTimeout = RequestTimeout == TDuration::Seconds(0);
        if (!noTimeout) {
            // got some deadlines
            Y_ABORT_UNLESS(RequestDeadlines);
        }
        ui32 msgsSent = MsgNum - BadSteps->size();
        LOG_NOTICE(ctx, NActorsServices::TEST, "TOTALLY SENT %u messages", msgsSent);
        ctx.Send(NotifyID, new TEvents::TEvCompleted());
        Die(ctx);
    }

    void SendPut(const TActorContext &ctx) {
        // put logo blob
        Y_ABORT_UNLESS(Step <= MsgNum);
        ui32 putCount = 0;
        const bool noTimeout = RequestTimeout == TDuration::Seconds(0);
        const TInstant deadline = noTimeout ? TInstant::Max() : TInstant::Now() + RequestTimeout;

        std::unique_ptr<TEvBlobStorage::TEvVMultiPut> vMultiPut(
            new TEvBlobStorage::TEvVMultiPut(VDiskInfo.VDiskID, deadline, HandleClassGen->GetHandleClass(),
                                             false, nullptr));
        while (Step < MsgNum) {
            if (Step % 100 == 0)
                LOG_NOTICE(ctx, NActorsServices::TEST, "PUT Step=%u", Step);

            TLogoBlobID logoBlobID(TabletId, Gen, Steps[Step], Channel, MsgData.size(), 0, 1);
            TVDiskIdShort mainVDiskId = TIngress::GetMainReplica(&Conf->GroupInfo->GetTopology(), logoBlobID);
            if (mainVDiskId == VDiskInfo.VDiskID) {
                ui64 cookieValue = Step;
                vMultiPut->AddVPut(logoBlobID, TRcBuf(MsgData), &cookieValue, nullptr, NWilson::TTraceId());
                putCount++;

                Step++;
                if (putCount == BatchSize) {
                    // next increment in Handle
                    break;
                }
            } else {
                BadSteps->insert(Steps[Step]);
                Step++;
            }
        }

        if (putCount) {
            ctx.Send(QueueActorId, vMultiPut.release());
            LastBatchSize = putCount;
            return;
        }

        if (Step == MsgNum) {
            Finish(ctx);
        }
    }

    void Handle(TEvBlobStorage::TEvVMultiPutResult::TPtr &ev, const TActorContext &ctx) {
        NKikimrBlobStorage::TEvVMultiPutResult &record = ev->Get()->Record;
        //Cerr << "Handle\n";
        Y_DEBUG_ABORT_UNLESS(LastBatchSize && LastBatchSize == record.ItemsSize());
        const bool noTimeout = RequestTimeout == TDuration::Seconds(0);
        auto status = record.GetStatus();
        Y_ABORT_UNLESS(status == NKikimrProto::OK || noTimeout && status == NKikimrProto::DEADLINE,
            "Event# %s", ev->Get()->ToString().data());

        Y_ABORT_UNLESS(MinREALHugeBlobInBytes);

        switch (status) {
        case NKikimrProto::OK:
            for (auto &item : record.GetItems()) {
                Y_ABORT_UNLESS(item.GetStatus() == (MsgData.size() < MinREALHugeBlobInBytes ? NKikimrProto::OK : NKikimrProto::ERROR));
            }
            break;
        case NKikimrProto::DEADLINE:
            RequestDeadlines++;
            break;
        default:
            // IMPOSSIBLE
            Y_ABORT();
        }

        if (Step == MsgNum) {
            Finish(ctx);
        } else {
            SendPut(ctx);
        }
    }

    STFUNC(StateWriteFunc) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvBlobStorage::TEvVMultiPutResult, Handle);
            HFunc(TEvProxyQueueState, Handle);
        }
    }

public:
    TManyMultiPuts(TConfiguration *conf, const TActorId &notifyID, const TAllVDisks::TVDiskInstance &vdiskInfo,
              ui32 msgDataSize, ui32 msgNum, ui32 batchSize, ui64 tabletId, ui32 channel, ui32 gen,
              std::shared_ptr<IPutHandleClassGenerator> cls, std::shared_ptr<TSet<ui32>> badSteps,
              TDuration requestTimeout)
        : TActorBootstrapped<TManyMultiPuts>()
        , Conf(conf)
        , NotifyID(notifyID)
        , VDiskInfo(vdiskInfo)
        , MsgDataSize(msgDataSize)
        , MsgNum(msgNum)
        , BatchSize(batchSize)
        , TabletId(tabletId)
        , Channel(channel)
        , Gen(gen)
        , HandleClassGen(cls)
        , BadSteps(badSteps)
        , RequestTimeout(requestTimeout)
        , Step(0)
    {
        MsgData.reserve(MsgDataSize);
        for (ui32 i = 0; i < MsgDataSize; i++) {
            MsgData.append('a' + char(i % 26));
        }

        for (ui32 i = 0; i < MsgNum; i++) {
            Steps.push_back(i);
        }
        Shuffle(Steps.begin(), Steps.end());
    }
};

IActor *CreateManyMultiPuts(TConfiguration *conf, const TActorId &notifyID, const TAllVDisks::TVDiskInstance &vdiskInfo,
                       ui32 msgDataSize, ui32 msgNum, ui32 batchSize, ui64 tabletId, ui32 channel, ui32 gen,
                       std::shared_ptr<IPutHandleClassGenerator> cls, std::shared_ptr<TSet<ui32>> badSteps,
                       TDuration requestTimeout) {
    return new TManyMultiPuts(conf, notifyID, vdiskInfo, msgDataSize, msgNum, batchSize, tabletId, channel, gen, cls,
                         badSteps, requestTimeout);
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TManyGets : public TActorBootstrapped<TManyGets> {
    TActorId NotifyID;
    const TAllVDisks::TVDiskInstance VDiskInfo;
    const ui32 MsgDataSize;
    const ui32 MsgNum;
    const ui64 TabletId;
    const ui32 Channel;
    const ui32 Gen;
    std::shared_ptr<TSet<ui32>> BadSteps;
    ui32 Step;
    TString MsgData;

    friend class TActorBootstrapped<TManyGets>;

    void Bootstrap(const TActorContext &ctx) {
        Become(&TThis::StateReadFunc);

        // get logo blob
        SendGet(ctx);
    }

    void SendGet(const TActorContext &ctx) {
        // get logo blob
        if (Step % 100 == 0)
            LOG_NOTICE(ctx, NActorsServices::TEST, "GET Step=%u", Step);
        TLogoBlobID logoBlobID(TabletId, Gen, Step, Channel, MsgData.size(), 0, 1);
        auto req = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(VDiskInfo.VDiskID,
                                                                   TInstant::Max(),
                                                                   NKikimrBlobStorage::EGetHandleClass::AsyncRead,
                                                                   TEvBlobStorage::TEvVGet::EFlags::None,
                                                                   {},
                                                                   {logoBlobID});
        ctx.Send(VDiskInfo.ActorID, req.release());
    }

    void Check(const TActorContext &ctx, const NKikimrBlobStorage::TEvVGetResult &rec, const TEvBlobStorage::TEvVGetResult& ev) {
        Y_UNUSED(ctx);
        int size = rec.GetResult().size();
        Y_ABORT_UNLESS(size == 1, "size=%d", size);
        const NKikimrBlobStorage::TQueryResult &q = rec.GetResult(0);
        if (const auto& s = ev.GetBlobData(q).ConvertToString(); s != MsgData) {
            fprintf(stderr, "Original: %s\n", MsgData.data());
            fprintf(stderr, "Received: %s\n", s.data());
            Y_ABORT();
        }
    }

    void Handle(TEvBlobStorage::TEvVGetResult::TPtr &ev, const TActorContext &ctx) {
        if (ev->Get()->Record.GetStatus() != NKikimrProto::OK) {
            LOG_NOTICE(ctx, NActorsServices::TEST, "ERROR");
            fprintf(stderr, "ERROR\n");
        } else {
            if (BadSteps->find(Step) == BadSteps->end()) {
                Check(ctx, ev->Get()->Record, *ev->Get());
            }
        }

        ++Step;
        if (Step == MsgNum) {
            // Finished
            ctx.Send(NotifyID, new TEvents::TEvCompleted());
            Die(ctx);
        } else {
            SendGet(ctx);
        }
    }

    STRICT_STFUNC(StateReadFunc,
        HFunc(TEvBlobStorage::TEvVGetResult, Handle);
        IgnoreFunc(TEvBlobStorage::TEvVWindowChange);
    )

public:
    TManyGets(const TActorId &notifyID, const TAllVDisks::TVDiskInstance &vdiskInfo, ui32 msgDataSize, ui32 msgNum,
              ui64 tabletId, ui32 channel, ui32 gen, std::shared_ptr<TSet<ui32>> badSteps)
        : TActorBootstrapped<TManyGets>()
        , NotifyID(notifyID)
        , VDiskInfo(vdiskInfo)
        , MsgDataSize(msgDataSize)
        , MsgNum(msgNum)
        , TabletId(tabletId)
        , Channel(channel)
        , Gen(gen)
        , BadSteps(badSteps)
        , Step(0)
    {
        MsgData.reserve(MsgDataSize);
        for (ui32 i = 0; i < MsgDataSize; i++) {
            MsgData.append('a' + char(i % 26));
        }
    }
};

IActor *CreateManyGets(const TActorId &notifyID, const TAllVDisks::TVDiskInstance &vdiskInfo, ui32 msgDataSize,
                       ui32 msgNum, ui64 tabletId, ui32 channel, ui32 gen, std::shared_ptr<TSet<ui32>> badSteps) {
    return new TManyGets(notifyID, vdiskInfo, msgDataSize, msgNum, tabletId, channel, gen, badSteps);
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TGet : public TActorBootstrapped<TGet> {
    TActorId NotifyID;
    const TAllVDisks::TVDiskInstance VDiskInfo;
    ui32 MsgNum;
    std::shared_ptr<TVector<TMsgPackInfo>> MsgPacks;
    const ui64 TabletId;
    const ui32 Channel;
    const ui32 Gen;
    const ui64 Shift;
    TVector<TString> Data;

    bool WithErrorResponse;

    friend class TActorBootstrapped<TGet>;

    void Bootstrap(const TActorContext &ctx) {
        Become(&TThis::StateReadFunc);

        // get logo blob
        SendGet(ctx);
    }

    void SendGet(const TActorContext &ctx) {
        // get logo blob
        auto req = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(VDiskInfo.VDiskID,
                                                                   TInstant::Max(),
                                                                   NKikimrBlobStorage::EGetHandleClass::AsyncRead,
                                                                   TEvBlobStorage::TEvVGet::EFlags::None,
                                                                   {},
                                                                   {});
        for (ui64 i = 0; i < MsgNum; ++i) {
            const TString data = Data[i];
            TLogoBlobID logoBlobID(TabletId, Gen, i, Channel, data.size(), 0, 1);
            req->AddExtremeQuery(logoBlobID, Shift, data.size() - Shift, &i);
        }
        ctx.Send(VDiskInfo.ActorID, req.release());
    }

    void Check(const TActorContext &ctx, const NKikimrBlobStorage::TEvVGetResult &rec, const TEvBlobStorage::TEvVGetResult& ev) {
        Y_UNUSED(ctx);
        ui32 size = rec.GetResult().size();
        Y_ABORT_UNLESS(size == MsgNum, "size=%d", size);
        for (ui64 i = 0; i < MsgNum; ++i) {
            const NKikimrBlobStorage::TQueryResult &q = rec.GetResult(i);
            const TString &data = Data[i];
            if (const auto& s = ev.GetBlobData(q).ConvertToString(); s != data.substr(Shift)) {
                fprintf(stderr, "Original: %s\n", data.data());
                fprintf(stderr, "Received: %s\n", s.data());
                Y_ABORT();
            }
        }
        auto serial = rec.SerializeAsString();
        if (serial.size() > MaxProtobufSize) {
            Y_ABORT();
        }
    }

    void Handle(TEvBlobStorage::TEvVGetResult::TPtr &ev, const TActorContext &ctx) {
        NKikimrProto::EReplyStatus status = ev->Get()->Record.GetStatus();
        switch (status) {
        case NKikimrProto::OK:
            if (WithErrorResponse) {
                Y_ABORT("Expected ERROR status but given OK");
            }
            Check(ctx, ev->Get()->Record, *ev->Get());
            break;

        case NKikimrProto::ERROR:
            if (!WithErrorResponse) {
                Y_ABORT("Expected OK status but given ERROR");
            }
            break;

        default:
            LOG_NOTICE(ctx, NActorsServices::TEST, "ERROR");
            fprintf(stderr, "ERROR\n");
        }

        ctx.Send(NotifyID, new TEvents::TEvCompleted());
        Die(ctx);
    }

    STRICT_STFUNC(StateReadFunc,
        HFunc(TEvBlobStorage::TEvVGetResult, Handle);
        IgnoreFunc(TEvBlobStorage::TEvVWindowChange);
    )

    void Init() {
        MsgNum = 0;
        for (auto &el: *MsgPacks) {
            MsgNum += el.Count;
        }
        Data.reserve(MsgNum);
        for (ui32 packIdx = 0; packIdx < MsgPacks->size(); ++packIdx) {
            for (ui32 i = 0; i < MsgPacks->at(packIdx).Count; ++i) {
                Data.emplace_back(MsgPacks->at(packIdx).MsgData);
            }
        }
        Y_ABORT_UNLESS(Data.size() == MsgNum);
    }

public:
    TGet(const TActorId &notifyID, const TAllVDisks::TVDiskInstance &vdiskInfo, ui32 msgDataSize, ui32 msgNum,
              ui64 tabletId, ui32 channel, ui32 gen, ui64 shift, bool withErrorResponse)
        : TActorBootstrapped<TGet>()
        , NotifyID(notifyID)
        , VDiskInfo(vdiskInfo)
        , MsgPacks(new TVector<TMsgPackInfo>{TMsgPackInfo(msgDataSize, msgNum)})
        , TabletId(tabletId)
        , Channel(channel)
        , Gen(gen)
        , Shift(shift)
        , WithErrorResponse(withErrorResponse)
    {
        Init();
    }

    TGet(const TActorId &notifyID, const TAllVDisks::TVDiskInstance &vdiskInfo,
            std::shared_ptr<TVector<TMsgPackInfo>> msgPacks,
            ui64 tabletId, ui32 channel, ui32 gen, ui64 shift, bool withErrorResponse)
        : TActorBootstrapped<TGet>()
        , NotifyID(notifyID)
        , VDiskInfo(vdiskInfo)
        , MsgPacks(msgPacks)
        , TabletId(tabletId)
        , Channel(channel)
        , Gen(gen)
        , Shift(shift)
        , WithErrorResponse(withErrorResponse)
    {
        Init();
    }
};

IActor *CreateGet(const TActorId &notifyID, const TAllVDisks::TVDiskInstance &vdiskInfo, ui32 msgDataSize,
                       ui32 msgNum, ui64 tabletId, ui32 channel, ui32 gen, ui64 shift, bool withErrorResponse) {
    return new TGet(notifyID, vdiskInfo, msgDataSize, msgNum, tabletId, channel, gen, shift, withErrorResponse);
}

IActor *CreateGet(const TActorId &notifyID, const TAllVDisks::TVDiskInstance &vdiskInfo,
                  std::shared_ptr<TVector<TMsgPackInfo>> msgPacks, ui64 tabletId, ui32 channel, ui32 gen, ui64 shift,
                  bool withErrorResponse) {
    return new TGet(notifyID, vdiskInfo, msgPacks, tabletId, channel, gen, shift, withErrorResponse);
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TPutGC : public TActorBootstrapped<TPutGC> {
    const NActors::TActorId NotifyID;
    const TAllVDisks::TVDiskInstance VDiskInfo;
    ui64 TabletID;
    ui32 RecGen;
    ui32 RecGenCounter;
    ui32 Channel;
    ui32 Collect;
    ui32 CollectGen;
    ui32 CollectStep;
    TAutoPtr<TVector<TLogoBlobID>> Keep;
    TAutoPtr<TVector<TLogoBlobID>> DoNotKeep;

    friend class TActorBootstrapped<TPutGC>;

    void Bootstrap(const TActorContext &ctx) {
        TThis::Become(&TThis::StateFunc);
        ctx.Send(VDiskInfo.ActorID,
                 new TEvBlobStorage::TEvVCollectGarbage(TabletID, RecGen, RecGenCounter, Channel, Collect, CollectGen,
                                                        CollectStep, false, Keep.Get(), DoNotKeep.Get(), VDiskInfo.VDiskID,
                                                        TInstant::Max()));
    }

    void Handle(TEvBlobStorage::TEvVCollectGarbageResult::TPtr &ev, const TActorContext &ctx) {
        Y_ABORT_UNLESS(ev->Get()->Record.GetStatus() == NKikimrProto::OK, "Status=%d", ev->Get()->Record.GetStatus());
        LOG_NOTICE(ctx, NActorsServices::TEST, "  TEvVCollectGarbageResult succeded");
        ctx.Send(NotifyID, new TEvents::TEvCompleted());
        TThis::Die(ctx);
    }

    STRICT_STFUNC(StateFunc,
        HFunc(TEvBlobStorage::TEvVCollectGarbageResult, Handle);
        IgnoreFunc(TEvBlobStorage::TEvVWindowChange);
    )

public:
    TPutGC(const NActors::TActorId &notifyID, const TAllVDisks::TVDiskInstance &vdiskInfo,
           ui64 tabletID, ui32 recGen, ui32 recGenCounter, ui32 channel, bool collect, ui32 collectGen,
           ui32 collectStep, TAutoPtr<TVector<TLogoBlobID>> keep, TAutoPtr<TVector<TLogoBlobID>> doNotKeep)
        : TActorBootstrapped<TPutGC>()
        , NotifyID(notifyID)
        , VDiskInfo(vdiskInfo)
        , TabletID(tabletID)
        , RecGen(recGen)
        , RecGenCounter(recGenCounter)
        , Channel(channel)
        , Collect(collect)
        , CollectGen(collectGen)
        , CollectStep(collectStep)
        , Keep(keep)
        , DoNotKeep(doNotKeep)
    {}
};

NActors::IActor *CreatePutGC(const NActors::TActorId &notifyID, const TAllVDisks::TVDiskInstance &vdiskInfo,
                             ui64 tabletID, ui32 recGen, ui32 recGenCounter, ui32 channel, bool collect,
                             ui32 collectGen, ui32 collectStep, TAutoPtr<TVector<TLogoBlobID>> keep,
                             TAutoPtr<TVector<TLogoBlobID>> doNotKeep) {
    return new TPutGC(notifyID, vdiskInfo, tabletID, recGen, recGenCounter, channel, collect, collectGen, collectStep,
                      keep, doNotKeep);
}



///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TWaitForCompactionOneDisk : public TActorBootstrapped<TWaitForCompactionOneDisk> {
    TActorId NotifyID;
    const TAllVDisks::TVDiskInstance VDiskInfo;
    const bool Sync;
    friend class TActorBootstrapped<TWaitForCompactionOneDisk>;

    void Bootstrap(const TActorContext &ctx) {
        Become(&TThis::StateSchedule);
        if (Sync) {
            ctx.Send(VDiskInfo.ActorID,
                TEvCompactVDisk::Create(EHullDbType::LogoBlobs, TEvCompactVDisk::EMode::FULL, false));
        } else {
            ctx.Send(VDiskInfo.ActorID,
                new TEvBlobStorage::TEvVCompact(VDiskInfo.VDiskID, NKikimrBlobStorage::TEvVCompact::ASYNC));
        }
    }

    void Handle(TEvBlobStorage::TEvVCompactResult::TPtr &ev, const TActorContext &ctx) {
        if (ev->Get()->Record.GetStatus() == NKikimrProto::OK) {
            Become(&TThis::StateWait);
            ctx.Send(VDiskInfo.ActorID, new TEvBlobStorage::TEvVStatus(VDiskInfo.VDiskID));
        } else {
            ctx.Send(VDiskInfo.ActorID,
                new TEvBlobStorage::TEvVCompact(VDiskInfo.VDiskID, NKikimrBlobStorage::TEvVCompact::ASYNC));
        }
    }

    void Handle(TEvCompactVDiskResult::TPtr&, const TActorContext& ctx) {
        ctx.Send(NotifyID, new TEvents::TEvCompleted());
        Die(ctx);
    }

    void Handle(TEvBlobStorage::TEvVStatusResult::TPtr &ev, const TActorContext &ctx) {
        Y_ABORT_UNLESS(ev->Get()->Record.GetStatus() == NKikimrProto::OK);
        const NKikimrBlobStorage::TEvVStatusResult &record = ev->Get()->Record;

        bool logoBlobsCompacted = record.GetLogoBlobs().GetCompacted();
        bool blocksCompacted = record.GetBlocks().GetCompacted();
        bool barriersCompacted = record.GetBarriers().GetCompacted();

        if (logoBlobsCompacted && blocksCompacted && barriersCompacted) {
            // Finished
            ctx.Send(NotifyID, new TEvents::TEvCompleted());
            Die(ctx);
        } else {
            Become(&TThis::StateSchedule);
            ctx.Send(VDiskInfo.ActorID,
                new TEvBlobStorage::TEvVCompact(VDiskInfo.VDiskID, NKikimrBlobStorage::TEvVCompact::ASYNC));
        }
    }

    STRICT_STFUNC(StateSchedule,
        HFunc(TEvBlobStorage::TEvVCompactResult, Handle);
        HFunc(TEvCompactVDiskResult, Handle);
    )

    STRICT_STFUNC(StateWait,
        HFunc(TEvBlobStorage::TEvVStatusResult, Handle);
    )

public:
    TWaitForCompactionOneDisk(const TActorId &notifyID, const TAllVDisks::TVDiskInstance &vdiskInfo, bool sync)
        : TActorBootstrapped<TWaitForCompactionOneDisk>()
        , NotifyID(notifyID)
        , VDiskInfo(vdiskInfo)
        , Sync(sync)
    {}
};

NActors::IActor *CreateWaitForCompaction(const NActors::TActorId &notifyID, const TAllVDisks::TVDiskInstance &vdiskInfo, bool sync) {
    return new TWaitForCompactionOneDisk(notifyID, vdiskInfo, sync);
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TWaitForCompaction : public TActorBootstrapped<TWaitForCompaction> {
    TActorId NotifyID;
    TConfiguration *Conf;
    ui32 Counter;
    bool Sync;
    friend class TActorBootstrapped<TWaitForCompaction>;

    void Bootstrap(const TActorContext &ctx) {
        Become(&TThis::StateFunc);

        ui32 total = Conf->GroupInfo->GetTotalVDisksNum();
        for (ui32 i = 0; i < total; i++) {
            TAllVDisks::TVDiskInstance &instance = Conf->VDisks->Get(i);
            ctx.RegisterWithSameMailbox(CreateWaitForCompaction(ctx.SelfID, instance, Sync));
            Counter++;
        }
    }

    void Handle(const TActorContext &ctx) {
        Counter--;
        if (Counter == 0) {
            // Finished
            ctx.Send(NotifyID, new TEvents::TEvCompleted());
            Die(ctx);
        }
    }

    STRICT_STFUNC(StateFunc,
        CFunc(TEvents::TSystem::Completed, Handle);
    )

public:
    TWaitForCompaction(const TActorId &notifyID, TConfiguration *conf, bool sync)
        : TActorBootstrapped<TWaitForCompaction>()
        , NotifyID(notifyID)
        , Conf(conf)
        , Counter(0)
        , Sync(sync)
    {}
};

NActors::IActor *CreateWaitForCompaction(const NActors::TActorId &notifyID, TConfiguration *conf, bool sync) {
    return new TWaitForCompaction(notifyID, conf, sync);
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TWaitForDefragOneDisk : public TActorBootstrapped<TWaitForDefragOneDisk> {
    TActorId NotifyID;
    const TAllVDisks::TVDiskInstance VDiskInfo;
    const bool Full;
    std::function<void(TEvBlobStorage::TEvVDefragResult::TPtr &)> Check;

    friend class TActorBootstrapped<TWaitForDefragOneDisk>;

    void Bootstrap(const TActorContext &ctx) {
        Become(&TThis::StateFunc);
        ctx.Send(VDiskInfo.ActorID, new TEvBlobStorage::TEvVDefrag(VDiskInfo.VDiskID, Full));
    }

    void Handle(TEvBlobStorage::TEvVDefragResult::TPtr &ev, const TActorContext &ctx) {
        if (ev->Get()->Record.GetStatus() == NKikimrProto::OK) {
            Check(ev);
            // Finished
            ctx.Send(NotifyID, new TEvents::TEvCompleted());
            Die(ctx);
        } else {
            ctx.Send(VDiskInfo.ActorID, new TEvBlobStorage::TEvVDefrag(VDiskInfo.VDiskID, Full));
        }
    }

    STRICT_STFUNC(StateFunc,
        HFunc(TEvBlobStorage::TEvVDefragResult, Handle);
    )

public:
    TWaitForDefragOneDisk(
            const TActorId &notifyID,
            const TAllVDisks::TVDiskInstance &vdiskInfo,
            bool full,
            std::function<void(TEvBlobStorage::TEvVDefragResult::TPtr &)> &&check)
        : TActorBootstrapped<TWaitForDefragOneDisk>()
        , NotifyID(notifyID)
        , VDiskInfo(vdiskInfo)
        , Full(full)
        , Check(std::move(check))
    {}
};

NActors::IActor *CreateDefrag(
        const NActors::TActorId &notifyID,
        const TAllVDisks::TVDiskInstance &vdiskInfo,
        bool full,
        std::function<void(TEvBlobStorage::TEvVDefragResult::TPtr &)> check) {
    return new TWaitForDefragOneDisk(notifyID, vdiskInfo, full, std::move(check));
}


///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TWaitForDefrag : public TActorBootstrapped<TWaitForDefrag> {
    TActorId NotifyID;
    TConfiguration *Conf;
    bool Full;
    std::function<void(TEvBlobStorage::TEvVDefragResult::TPtr &)> Check;
    ui32 Counter = 0;

    friend class TActorBootstrapped<TWaitForDefrag>;

    void Bootstrap(const TActorContext &ctx) {
        Become(&TThis::StateFunc);

        ui32 total = Conf->GroupInfo->GetTotalVDisksNum();
        for (ui32 i = 0; i < total; i++) {
            TAllVDisks::TVDiskInstance &instance = Conf->VDisks->Get(i);
            ctx.RegisterWithSameMailbox(CreateDefrag(ctx.SelfID, instance, Full, Check));
            Counter++;
        }
    }

    void Handle(const TActorContext &ctx) {
        Counter--;
        if (Counter == 0) {
            // Finished
            ctx.Send(NotifyID, new TEvents::TEvCompleted());
            Die(ctx);
        }
    }

    STRICT_STFUNC(StateFunc,
        CFunc(TEvents::TSystem::Completed, Handle);
    )

public:
    TWaitForDefrag(
            const TActorId &notifyID,
            TConfiguration *conf,
            bool full,
            std::function<void(TEvBlobStorage::TEvVDefragResult::TPtr &)> &&check)
        : TActorBootstrapped<TWaitForDefrag>()
        , NotifyID(notifyID)
        , Conf(conf)
        , Full(full)
        , Check(std::move(check))
    {}
};

NActors::IActor *CreateDefrag(
        const NActors::TActorId &notifyID,
        TConfiguration *conf,
        bool full,
        std::function<void(TEvBlobStorage::TEvVDefragResult::TPtr &)> check) {
    return new TWaitForDefrag(notifyID, conf, full, std::move(check));
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TWaitForSync : public TActorBootstrapped<TWaitForSync> {
    // info we get from single VDisk
    struct TInfo {
        const TActorId ActorId;
        const TVDiskID VDiskId;

        bool SetUp = false;
        TVector<TSyncState> SyncStates;
        NSyncLog::TLogEssence SyncLogEssence;

        TInfo(const TActorId &id, const TVDiskID &vd)
            : ActorId(id)
            , VDiskId(vd)
        {}

        void Clear() {
            SetUp = false;
            SyncStates.clear();
        }
    };

    // Mutual State from All (active) VDisks
    class TMutualSyncState {
    public:
        TMutualSyncState(TConfiguration *conf) {
            for (ui32 i = 0; i < conf->GroupInfo->GetTotalVDisksNum(); i++) {
                TAllVDisks::TVDiskInstance &instance = conf->VDisks->Get(i);
                if (instance.Initialized) {
                    InfoVec.emplace_back(instance.ActorID, instance.VDiskID);
                } else {
                    PuncturedPoints.insert(i);
                }
            }
        }

        void Update(const TVDiskID &vd, const NKikimrBlobStorage::TEvVStatusResult &record) {
            // find idx for required Info
            ui32 idx = FindIdx(vd);
            // fill in info structure
            TInfo &info = InfoVec[idx];

            info.SetUp = true;
            for (ui32 i = 0; i < record.GetSyncerStatus().SyncStateSize(); i++) {
                if (PuncturedPoints.find(i) == PuncturedPoints.end()) {
                    // we use this info
                    info.SyncStates.push_back(SyncStateFromSyncState(record.GetSyncerStatus().GetSyncState(i)));
                }
            }

            const NKikimrBlobStorage::TSyncLogStatus &sls = record.GetSyncLogStatus();
            info.SyncLogEssence = NSyncLog::TLogEssence {
                (ui64)sls.GetLogStartLsn(),
                (bool)sls.GetMemLogEmpty(),
                (bool)sls.GetDiskLogEmpty(),
                (ui64)sls.GetFirstMemLsn(),
                (ui64)sls.GetLastMemLsn(),
                (ui64)sls.GetFirstDiskLsn(),
                (ui64)sls.GetLastDiskLsn()
            };
        }

        // returns true when synced
        bool IsSynced() {
            // we got all data, analyze it
            ::ForEach(InfoVec.begin(), InfoVec.end(), [] (const TInfo &i) { Y_ABORT_UNLESS(i.SetUp); });

            for (ui32 i = 0; i < InfoVec.size(); i++) {
                TInfo &info = InfoVec[i];
                for (ui32 j = 0; j < InfoVec.size(); j++) {
                    if (i != j) {
                        ui64 syncedLsn = info.SyncStates[j].SyncedLsn;
                        auto ri = [] { return TString("no internals"); };
                        NSyncLog::EReadWhatsNext whatsNext;
                        whatsNext = NSyncLog::WhatsNext(syncedLsn, 0, &InfoVec[j].SyncLogEssence, ri).WhatsNext;
                        if (whatsNext != NSyncLog::EWnDiskSynced)
                            return false;
                    }
                }
            }

            return true;
        }


        template <class TFunc>
        void ForEach(TFunc func) {
            ::ForEach(InfoVec.begin(), InfoVec.end(), func);
        }

        void Clear() {
            ::ForEach(InfoVec.begin(), InfoVec.end(), [] (TInfo &i) { i.Clear(); });
        }

        ui32 InfoVecSize() const {
            return InfoVec.size();
        }

    private:
        // seq number of vdisks we exclude
        TSet<ui32> PuncturedPoints;
        // vector of working VDisks with their info data
        TVector<TInfo> InfoVec;

        ui32 FindIdx(const TVDiskID &vd) {
            // found InfoVec element idx this vdisk
            for (ui32 idx = 0, size = InfoVecSize(); idx < size; ++idx) {
                if (InfoVec[idx].VDiskId == vd)
                    return idx;
            }
            Y_ABORT("nothing found");
        }
    };


    TActorId NotifyID;
    ui32 Counter = 0;
    TMutualSyncState MutualSyncState;

    friend class TActorBootstrapped<TWaitForSync>;

    void Bootstrap(const TActorContext &ctx) {
        Become(&TThis::StateFunc);
        GetStatuses(ctx);
    }

    void GetStatuses(const TActorContext &ctx) {
        // send TEvVStatus to all working VDisks
        auto sendFunc = [&ctx] (const TInfo &info) {
            ctx.Send(info.ActorId, new TEvBlobStorage::TEvVStatus(info.VDiskId));
        };
        MutualSyncState.ForEach(sendFunc);
        Counter = MutualSyncState.InfoVecSize();
    }

    void Handle(TEvBlobStorage::TEvVStatusResult::TPtr &ev, const TActorContext &ctx) {
        Y_ABORT_UNLESS(ev->Get()->Record.GetStatus() == NKikimrProto::OK);
        const NKikimrBlobStorage::TEvVStatusResult &record = ev->Get()->Record;
        TVDiskID vdisk = VDiskIDFromVDiskID(record.GetVDiskID());

        MutualSyncState.Update(vdisk, record);
        --Counter;
        if (Counter == 0) {
            if (MutualSyncState.IsSynced()) {
                // Finished
                ctx.Send(NotifyID, new TEvents::TEvCompleted());
                Die(ctx);
            } else {
                // Retry
                MutualSyncState.Clear();
                GetStatuses(ctx);
            }
        }
    }

    STRICT_STFUNC(StateFunc,
        HFunc(TEvBlobStorage::TEvVStatusResult, Handle);
    )

public:
    TWaitForSync(const TActorId &notifyID, TConfiguration *conf)
        : TActorBootstrapped<TWaitForSync>()
        , NotifyID(notifyID)
        , MutualSyncState(conf)
    {}
};

NActors::IActor *CreateWaitForSync(const NActors::TActorId &notifyID, TConfiguration *conf) {
    return new TWaitForSync(notifyID, conf);
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TCheckDbEmptynessActor : public TActorBootstrapped<TCheckDbEmptynessActor> {
    const TActorId NotifyID;
    const TAllVDisks::TVDiskInstance VDiskInfo;
    const bool ExpectEmpty;

    friend class TActorBootstrapped<TCheckDbEmptynessActor>;

    void Bootstrap(const TActorContext &ctx) {
        Become(&TThis::StateFunc);
        ctx.Send(VDiskInfo.ActorID,
                 new TEvBlobStorage::TEvVStatus(VDiskInfo.VDiskID));
    }

    void Handle(TEvBlobStorage::TEvVStatusResult::TPtr &ev, const TActorContext &ctx) {
        Y_UNUSED(ctx);
        Y_ABORT_UNLESS(ev->Get()->Record.GetStatus() == NKikimrProto::OK);

        const NKikimrBlobStorage::TEvVStatusResult &record = ev->Get()->Record;
        auto status = record.GetLocalRecoveryInfo();
        ui64 startLsn = status.GetStartLsn();
        Y_ABORT_UNLESS((ExpectEmpty && startLsn == 0) || (!ExpectEmpty && startLsn != 0));
        ctx.Send(NotifyID, new TEvents::TEvCompleted());
        TThis::Die(ctx);
    }

    STRICT_STFUNC(StateFunc,
        HFunc(TEvBlobStorage::TEvVStatusResult, Handle);
    )

public:
    TCheckDbEmptynessActor(const TActorId &notifyID, const TAllVDisks::TVDiskInstance &vdiskInfo, bool expectEmpty)
        : TActorBootstrapped<TCheckDbEmptynessActor>()
        , NotifyID(notifyID)
        , VDiskInfo(vdiskInfo)
        , ExpectEmpty(expectEmpty)
    {}
};


NActors::IActor *CreateCheckDbEmptyness(const NActors::TActorId &notifyID, const TAllVDisks::TVDiskInstance &vdiskInfo,
                                        bool expectEmpty) {
    return new TCheckDbEmptynessActor(notifyID, vdiskInfo, expectEmpty);
}
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TPutGCToCorrespondingVDisksActor : public TActorBootstrapped<TPutGCToCorrespondingVDisksActor> {
    const NActors::TActorId NotifyID;
    TConfiguration *Conf;
    ui32 Counter;
    ui64 TabletID;
    ui32 RecGen;
    ui32 RecGenCounter;
    ui32 Channel;
    ui32 Collect;
    ui32 CollectGen;
    ui32 CollectStep;
    TAutoPtr<TVector<NKikimr::TLogoBlobID>> Keep;
    TAutoPtr<TVector<NKikimr::TLogoBlobID>> DoNotKeep;

    friend class TActorBootstrapped<TPutGCToCorrespondingVDisksActor>;

    void Bootstrap(const TActorContext &ctx) {
        TThis::Become(&TThis::StateFunc);

        ui32 total = Conf->GroupInfo->GetTotalVDisksNum();
        for (ui32 i = 0; i < total; i++) {
            TAllVDisks::TVDiskInstance &instance = Conf->VDisks->Get(i);
            if (instance.Initialized) {
                ctx.Send(instance.ActorID,
                         new TEvBlobStorage::TEvVCollectGarbage(TabletID, RecGen, RecGenCounter, Channel, Collect,
                                                                CollectGen, CollectStep, false, Keep.Get(),
                                                                DoNotKeep.Get(), instance.VDiskID, TInstant::Max()));
                Counter++;
            }
        }
    }

    void Handle(TEvBlobStorage::TEvVCollectGarbageResult::TPtr &ev, const TActorContext &ctx) {
        Y_ABORT_UNLESS(ev->Get()->Record.GetStatus() == NKikimrProto::OK, "Status=%d", ev->Get()->Record.GetStatus());
        LOG_NOTICE(ctx, NActorsServices::TEST, "  TEvVCollectGarbageResult succeded");

        --Counter;
        if (Counter == 0) {
            // Finished
            ctx.Send(NotifyID, new TEvents::TEvCompleted());
            TThis::Die(ctx);
        }
    }

    STRICT_STFUNC(StateFunc,
        HFunc(TEvBlobStorage::TEvVCollectGarbageResult, Handle);
        IgnoreFunc(TEvBlobStorage::TEvVWindowChange);
    )

public:
    TPutGCToCorrespondingVDisksActor(const NActors::TActorId &notifyID, TConfiguration *conf, ui64 tabletID,
                                     ui32 recGen, ui32 recGenCounter, ui32 channel, bool collect, ui32 collectGen,
                                     ui32 collectStep, TAutoPtr<TVector<NKikimr::TLogoBlobID>> keep,
                                     TAutoPtr<TVector<NKikimr::TLogoBlobID>> doNotKeep)
        : TActorBootstrapped<TPutGCToCorrespondingVDisksActor>()
        , NotifyID(notifyID)
        , Conf(conf)
        , Counter(0)
        , TabletID(tabletID)
        , RecGen(recGen)
        , RecGenCounter(recGenCounter)
        , Channel(channel)
        , Collect(collect)
        , CollectGen(collectGen)
        , CollectStep(collectStep)
        , Keep(keep)
        , DoNotKeep(doNotKeep)
    {}
};


IActor *PutGCToCorrespondingVDisks(const NActors::TActorId &notifyID, TConfiguration *conf, ui64 tabletID,
                                   ui32 recGen, ui32 recGenCounter, ui32 channel, bool collect, ui32 collectGen,
                                   ui32 collectStep, TAutoPtr<TVector<NKikimr::TLogoBlobID>> keep,
                                   TAutoPtr<TVector<NKikimr::TLogoBlobID>> doNotKeep) {
    return new TPutGCToCorrespondingVDisksActor(notifyID, conf, tabletID, recGen, recGenCounter, channel, collect,
                                                collectGen, collectStep, keep, doNotKeep);
}

NActors::IActor *PutGCToCorrespondingVDisks(const NActors::TActorId &notifyID, TConfiguration *conf,
                                            const TGCSettings &settings,
                                            TAutoPtr<TVector<NKikimr::TLogoBlobID>> keep,
                                            TAutoPtr<TVector<NKikimr::TLogoBlobID>> doNotKeep) {
    return new TPutGCToCorrespondingVDisksActor(notifyID, conf, settings.TabletID, settings.RecGen,
                                                settings.RecGenCounter, settings.Channel, settings.Collect,
                                                settings.CollectGen, settings.CollectStep, keep, doNotKeep);
}


///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

void PutLogoBlobToVDisk(const TActorContext &ctx, const TActorId &actorID, const TVDiskID &vdiskID,
                        const TLogoBlobID &id, const TString &data, NKikimrBlobStorage::EPutHandleClass cls) {
    LOG_DEBUG(ctx, NActorsServices::TEST, "  Sending TEvPut: id=%s data='%s'", id.ToString().data(), LimitData(data).data());
    ctx.Send(actorID, new TEvBlobStorage::TEvVPut(id, TRope(data), vdiskID, false, nullptr, TInstant::Max(), cls));
}

// returns number of messages sent
ui32 PutLogoBlobToCorrespondingVDisks(const TActorContext &ctx, NKikimr::TBlobStorageGroupInfo *info,
                                      const TLogoBlobID &id, const TString &data, NKikimrBlobStorage::EPutHandleClass cls) {
    Y_ASSERT(id.PartId() == 0);
    ui32 msgsSent = 0;
    TBlobStorageGroupInfo::TVDiskIds outVDisks;
    TBlobStorageGroupInfo::TServiceIds outServIds;
    info->PickSubgroup(id.Hash(), &outVDisks, &outServIds);
    ui8 n = info->Type.TotalPartCount();
    for (ui8 i = 0; i < n; i++) {
        TLogoBlobID aid(id, i + 1);
        PutLogoBlobToVDisk(ctx, outServIds[i], outVDisks[i], aid, data, cls);
        msgsSent++;
    }
    return msgsSent;
}

ui32 GetLogoBlobFromCorrespondingVDisks(const NActors::TActorContext &ctx, NKikimr::TBlobStorageGroupInfo *info,
                                        const NKikimr::TLogoBlobID &id) {
    Y_ASSERT(id.PartId() == 0);
    ui32 msgsSent = 0;
    TBlobStorageGroupInfo::TVDiskIds outVDisks;
    TBlobStorageGroupInfo::TServiceIds outServIds;
    info->PickSubgroup(id.Hash(), &outVDisks, &outServIds);
    ui8 n = info->Type.TotalPartCount();
    for (ui8 i = 0; i < n; i++) {
        TLogoBlobID aid(id, i + 1);
        LOG_NOTICE(ctx, NActorsServices::TEST, "  Sending TEvGet: id=%s", aid.ToString().data());
        auto req = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(outVDisks[i],
                                                                   TInstant::Max(),
                                                                   NKikimrBlobStorage::EGetHandleClass::AsyncRead,
                                                                   TEvBlobStorage::TEvVGet::EFlags::None,
                                                                   {},
                                                                   {aid});
        ctx.Send(outServIds[i], req.release());
        msgsSent++;
    }
    return msgsSent;
}


///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
TDataSnapshot::TItem::TItem(const TVDiskID &vdisk, const TActorId &service, const NKikimr::TLogoBlobID &id,
                            const TString &data, const TIngress &ingress)
    : VDiskID(vdisk)
    , ServiceID(service)
    , Id(id)
    , Data(data)
    , Ingress(ingress)
{}

struct TDataSnapshot::TLess {
    bool operator () (const TItem &x, const TItem &y) const {
        return x.VDiskID < y.VDiskID || (x.VDiskID == y.VDiskID && x.Id < y.Id);
    }
};

TDataSnapshot::TDataSnapshot(TIntrusivePtr<NKikimr::TBlobStorageGroupInfo> info)
    : Info(info)
{}

void TDataSnapshot::PutExact(const TVDiskID &vdisk, const TActorId &service, const NKikimr::TLogoBlobID &id,
                             const TString &data, const TIngress &ingress) {
    Y_ASSERT(id.PartId() != 0);
    Data.push_back(TItem(vdisk, service, id, data, ingress));
}

void TDataSnapshot::PutCorresponding(const NKikimr::TLogoBlobID &id, const TString &data) {
    Y_ASSERT(id.PartId() == 0);

    TBlobStorageGroupInfo::TVDiskIds outVDisks;
    TBlobStorageGroupInfo::TServiceIds outServIds;
    Info->PickSubgroup(id.Hash(), &outVDisks, &outServIds);
    ui8 n = Info->Type.TotalPartCount();
    for (ui8 i = 0; i < n; i++) {
        TLogoBlobID aid(id, i + 1);
        Data.push_back(TItem(outVDisks[i], outServIds[i], aid, data, TIngress(0)));
    }
}

void TDataSnapshot::SortAndCheck() {
    // check that we can work with this data
    Sort(Data.begin(), Data.end(), TDataSnapshot::TLess());

    TVDiskID disk;
    TLogoBlobID id;
    bool start = true;
    for (const auto &i : Data) {
        if (start || disk != i.VDiskID) {
            start = false;
            disk = i.VDiskID;
            id = i.Id;
        } else {
            Y_ABORT_UNLESS(i.Id != id, "NOT IMPLEMENTED");
        }
    }
}

TDataSnapshot::TIterator TDataSnapshot::begin() {
    return Data.begin();
}
TDataSnapshot::TIterator TDataSnapshot::end() {
    return Data.end();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TLoadDataSnapshotActor : public TActorBootstrapped<TLoadDataSnapshotActor> {
private:
    const NActors::TActorId NotifyID;
    TConfiguration *Conf;
    TDataSnapshotPtr DataPtr;
    NKikimrBlobStorage::EPutHandleClass HandleClass;
    ui32 Counter;
    TPDiskPutStatusHandler PDiskPutStatusHandler;

    friend class TActorBootstrapped<TLoadDataSnapshotActor>;

    void Output(const TLogoBlobID &lb) {
        fprintf(stderr, "%s: %s\n", lb.ToString().data(),
                TIngress::PrintVDisksForLogoBlob(Conf->GroupInfo.Get(), TLogoBlobID(lb, 0)).data());
    }

    void Bootstrap(const TActorContext &ctx) {
        TThis::Become(&TThis::StateFunc);
        // send all messages
        for (const auto &it : *DataPtr) {
            PutLogoBlobToVDisk(ctx, it.ServiceID, it.VDiskID, it.Id, it.Data, HandleClass);
            Counter++;
        }
    }

    void Handle(TEvBlobStorage::TEvVPutResult::TPtr &ev, const TActorContext &ctx) {
        if ((*PDiskPutStatusHandler)(ctx, NotifyID, ev)) {
            TThis::Die(ctx);
            return;
        }

        --Counter;
        if (Counter == 0) {
            // Finished
            ctx.Send(NotifyID, new TEvents::TEvCompleted());
            TThis::Die(ctx);
        }
    }

    STRICT_STFUNC(StateFunc,
        HFunc(TEvBlobStorage::TEvVPutResult, Handle);
        IgnoreFunc(TEvBlobStorage::TEvVWindowChange);
    )

public:
    TLoadDataSnapshotActor(const NActors::TActorId &notifyID, TConfiguration *conf, TDataSnapshotPtr dataPtr,
                           NKikimrBlobStorage::EPutHandleClass cls, TPDiskPutStatusHandler hndl)
        : TActorBootstrapped<TLoadDataSnapshotActor>()
        , NotifyID(notifyID)
        , Conf(conf)
        , DataPtr(dataPtr)
        , HandleClass(cls)
        , Counter(0)
        , PDiskPutStatusHandler(hndl)
    {}
};

NActors::IActor *CreateLoadDataSnapshot(const NActors::TActorId &notifyID, TConfiguration *conf,
                                        TDataSnapshotPtr dataPtr, NKikimrBlobStorage::EPutHandleClass cls,
                                        TPDiskPutStatusHandler hndl) {
    return new TLoadDataSnapshotActor(notifyID, conf, dataPtr, cls, hndl);
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TCheckDataSnapshotActor : public TActorBootstrapped<TCheckDataSnapshotActor> {
    const NActors::TActorId NotifyID;
    TConfiguration *Conf;
    TDataSnapshotPtr DataPtr;
    TDataSnapshot::TIterator Cur;
    TDataSnapshot::TIterator End;
    TPDiskGetStatusHandler PDiskGetStatusHandler;

    friend class TActorBootstrapped<TCheckDataSnapshotActor>;

    void Output(const TLogoBlobID &lb) {
        fprintf(stderr, "%s: %s\n", lb.ToString().data(),
                TIngress::PrintVDisksForLogoBlob(Conf->GroupInfo.Get(), TLogoBlobID(lb, 0)).data());
    }

    void Bootstrap(const TActorContext &ctx) {
        TThis::Become(&TThis::StateFunc);
        DataPtr->SortAndCheck();
        Next(ctx);
    }

    void Next(const TActorContext &ctx) {
        if (Cur == End) {
            // Finished
            ctx.Send(NotifyID, new TEvents::TEvCompleted(0, 1));
            TThis::Die(ctx);
        } else {
            // send read
            auto req = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(Cur->VDiskID,
                                                                       TInstant::Max(),
                                                                       NKikimrBlobStorage::EGetHandleClass::AsyncRead,
                                                                       TEvBlobStorage::TEvVGet::EFlags::ShowInternals,
                                                                       {},
                                                                       {TLogoBlobID(Cur->Id, 0)});
            ctx.Send(Cur->ServiceID, req.release());
        }
    }

    void Handle(TEvBlobStorage::TEvVGetResult::TPtr &ev, const TActorContext &ctx) {
        if ((*PDiskGetStatusHandler)(ctx, NotifyID, ev)) {
            TThis::Die(ctx);
            return;
        }

        PrintDebug(ev, ctx, Conf->GroupInfo.Get(), Cur->VDiskID);

        // check result
        const auto &rec = ev->Get()->Record;
        int size = rec.GetResult().size();
        Y_ABORT_UNLESS(size == 1);
        const NKikimrBlobStorage::TQueryResult &q = rec.GetResult(0);
        const TLogoBlobID id = LogoBlobIDFromLogoBlobID(q.GetBlobID());
        TIngress ingress(q.GetIngress());
        if (q.GetStatus() == NKikimrProto::OK) {
            Y_ABORT_UNLESS(id == Cur->Id);
            const auto& data = ev->Get()->GetBlobData(q).ConvertToString();
            Y_ABORT_UNLESS(ingress.Raw() == Cur->Ingress.Raw() && data == Cur->Data,
                     "vdiskId# %s id# %s ingress# %s Cur->Ingress# %s"
                     " ingress.Raw# %" PRIu64 " Cur->Ingress.Raw# %" PRIu64
                     " buf# '%s' Cur->Data# '%s'",
                     Cur->VDiskID.ToString().data(),
                     id.ToString().data(),
                     ingress.ToString(&Conf->GroupInfo->GetTopology(), Cur->VDiskID, id).data(),
                     Cur->Ingress.ToString(&Conf->GroupInfo->GetTopology(), Cur->VDiskID, id).data(),
                     ingress.Raw(),
                     Cur->Ingress.Raw(),
                     data.data(),
                     Cur->Data.data());
        } else if (q.GetStatus() == NKikimrProto::NODATA) {
            Y_ABORT_UNLESS(Cur->Data.empty());
            Y_ABORT_UNLESS(ingress.Raw() == Cur->Ingress.Raw(),
                     "vdiskId# %s id# %s ingress# %s Cur->Ingress# %s"
                     " ingress.Raw# %" PRIu64 " Cur->Ingress.Raw# %" PRIu64,
                     Cur->VDiskID.ToString().data(),
                     id.ToString().data(),
                     ingress.ToString(&Conf->GroupInfo->GetTopology(), Cur->VDiskID, id).data(),
                     Cur->Ingress.ToString(&Conf->GroupInfo->GetTopology(), Cur->VDiskID, id).data(),
                     ingress.Raw(),
                     Cur->Ingress.Raw());
        }

        Cur++;
        Next(ctx);
    }

    STRICT_STFUNC(StateFunc,
        HFunc(TEvBlobStorage::TEvVGetResult, Handle);
        IgnoreFunc(TEvBlobStorage::TEvVWindowChange);
    )

public:
    TCheckDataSnapshotActor(const NActors::TActorId &notifyID, TConfiguration *conf, TDataSnapshotPtr dataPtr,
                            TPDiskGetStatusHandler hndl)
        : TActorBootstrapped<TCheckDataSnapshotActor>()
        , NotifyID(notifyID)
        , Conf(conf)
        , DataPtr(dataPtr)
        , Cur(DataPtr->begin())
        , End(DataPtr->end())
        , PDiskGetStatusHandler(hndl)
    {}
};

NActors::IActor *CreateCheckDataSnapshot(const NActors::TActorId &notifyID, TConfiguration *conf,
                                         TDataSnapshotPtr dataPtr, TPDiskGetStatusHandler hndl) {
    return new TCheckDataSnapshotActor(notifyID, conf, dataPtr, hndl);
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
void CheckQueryResult(NKikimr::TEvBlobStorage::TEvVGetResult::TPtr &ev, const NActors::TActorContext &ctx,
                      EQueryResult eqr, TExpectedSet *expSet, bool fullResult) {
    switch (eqr) {
        case EQR_OK_NODATA: {
            Y_ABORT_UNLESS(ev->Get()->Record.GetStatus() == NKikimrProto::OK);
            LOG_NOTICE(ctx, NActorsServices::TEST, "  TEvVGetResult succeded");
            const NKikimrBlobStorage::TEvVGetResult &rec = ev->Get()->Record;
            int size = rec.GetResult().size();
            Y_ABORT_UNLESS(size == 1);
            const NKikimrBlobStorage::TQueryResult &q = rec.GetResult(0);
            Y_ABORT_UNLESS(q.GetStatus() == NKikimrProto::NODATA);
            break;
        }
        case EQR_OK_EMPTY: {
            Y_ABORT_UNLESS(ev->Get()->Record.GetStatus() == NKikimrProto::OK);
            LOG_NOTICE(ctx, NActorsServices::TEST, "  TEvVGetResult succeded");
            const NKikimrBlobStorage::TEvVGetResult &rec = ev->Get()->Record;
            int size = rec.GetResult().size();
            Y_ABORT_UNLESS(size == 0);
            break;
        }
        case EQR_OK_EXPECTED_SET: {
            Y_ABORT_UNLESS(ev->Get()->Record.GetStatus() == NKikimrProto::OK, "Status=%d", ev->Get()->Record.GetStatus());
            LOG_NOTICE(ctx, NActorsServices::TEST, "  TEvVGetResult succeded");
            const NKikimrBlobStorage::TEvVGetResult &rec = ev->Get()->Record;
            int size = rec.GetResult().size();
            for (int i = 0; i < size; i++) {
                const NKikimrBlobStorage::TQueryResult &q = rec.GetResult(i);
                const TLogoBlobID id = LogoBlobIDFromLogoBlobID(q.GetBlobID());
                const TString& data = ev->Get()->GetBlobData(q).ConvertToString();
                LOG_NOTICE(ctx, NActorsServices::TEST, "    @@@@@@@@@@ Status=%s LogoBlob=%s Data='%s' Cookie=%" PRIu64,
                           NKikimrProto::EReplyStatus_Name(q.GetStatus()).data(), id.ToString().data(), LimitData(data).data(),
                           q.GetCookie());
                expSet->Check(id, q.GetStatus(), data);
            }
            if (fullResult)
                expSet->Finish();
            break;
        }
        default: Y_ABORT("Impossible case");
    }
}
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
void PrintDebug(NKikimr::TEvBlobStorage::TEvVGetResult::TPtr &ev, const NActors::TActorContext &ctx,
                NKikimr::TBlobStorageGroupInfo *info, const NKikimr::TVDiskID &vdisk) {
    Y_ABORT_UNLESS(ev->Get()->Record.GetStatus() == NKikimrProto::OK);

    // output result
    const NKikimrBlobStorage::TEvVGetResult &rec = ev->Get()->Record;
    int size = rec.GetResult().size();
    LOG_NOTICE(ctx, NActorsServices::TEST, "  TEvVGetResult succeded (%d items)", size);
    for (int i = 0; i < size; i++) {
        const NKikimrBlobStorage::TQueryResult &q = rec.GetResult(i);
        const TLogoBlobID id = LogoBlobIDFromLogoBlobID(q.GetBlobID());
        TString ingressStr;
        ui64 ingressRaw = 0;
        if (q.HasIngress()) {
            TIngress ingress(q.GetIngress());
            ingressStr = ingress.ToString(&info->GetTopology(), vdisk, id);
            ingressRaw = ingress.Raw();
        }
        const TString& data = ev->Get()->GetBlobData(q).ConvertToString();
        LOG_NOTICE(ctx, NActorsServices::TEST, "  @@@@@@@@@@ Status=%s LogoBlob=%s Data='%s' Ingress='%s' Raw=0x%lx",
                   NKikimrProto::EReplyStatus_Name(q.GetStatus()).data(), id.ToString().data(), LimitData(data).data(),
                   ingressStr.data(), ingressRaw);
    }
}
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
TExpectedSet::TExpectedResult::TExpectedResult(NKikimrProto::EReplyStatus status, const TString &data)
    : Status(status)
    , Data(data)
{}

void TExpectedSet::Put(const TLogoBlobID &id, NKikimrProto::EReplyStatus status, const TString &data) {
    bool res = Map.insert(TMapType::value_type(id, TExpectedResult(status, data))).second;
    Y_ABORT_UNLESS(res);
}

TString TExpectedSet::ToString() const {
    TStringStream str;
    str << "Map State:\n";
    for (const auto &x : Map) {
        str << x.first << " " << x.second << "\n";
    }
    return str.Str();
}

void TExpectedSet::Check(const TLogoBlobID &id, NKikimrProto::EReplyStatus status, const TString &data) {
    //fprintf(stderr, "Check: id=%s status=%s data=%s\n", ~id.ToString(), ~NKikimrProto::EReplyStatus_Name(status), ~data);
    TMapType::iterator it = Map.find(id);
    Y_ABORT_UNLESS(it != Map.end(), "TExpectedSet::Check: can't find id=%s; data# '%s' map# %s", id.ToString().data(), data.data(),
           ToString().data());
    Y_ABORT_UNLESS(it->second.Status == status, "TExpectedSet::Check: incorrect status %s instead of %s for %s",
           NKikimrProto::EReplyStatus_Name(status).data(), NKikimrProto::EReplyStatus_Name(it->second.Status).data(),
           id.ToString().data());
    Y_ABORT_UNLESS(it->second.Data == data, "TExpectedSet::Check: incorrect data '%s' instead of '%s' for %s; "
           "got string of size %u instead of string of size %u",
           data.data(), it->second.Data.data(), id.ToString().data(), unsigned(data.size()), unsigned(it->second.Data.size()));
    Map.erase(it);
}

void TExpectedSet::Finish() {
    Y_ABORT_UNLESS(Map.empty());
}

Y_DECLARE_OUT_SPEC(, TExpectedSet::TExpectedResult, stream, value) {
    stream << "Status# " << NKikimrProto::EReplyStatus_Name(value.Status) << " Data# " << value.Data;
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
struct TEvRunActor : public TEventLocal<TEvRunActor, TEvBlobStorage::EvRunActor> {
    TAutoPtr<IActor> Actor;

    TEvRunActor(TAutoPtr<IActor> actor)
        : Actor(actor)
    {}
};
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TSyncRunActor : public TActor<TSyncRunActor> {
    std::shared_ptr<TSystemEvent> _Event;
    std::shared_ptr<TSyncRunner::TReturnValue> ReturnValue;

    void Handle(TEvRunActor::TPtr &ev, const TActorContext &ctx) {
        ctx.ExecutorThread.RegisterActor(ev->Get()->Actor.Release());
    }

    void HandleDone(TEvents::TEvCompleted::TPtr &ev, const TActorContext &ctx) {
        Y_UNUSED(ctx);
        ReturnValue->Id = ev->Get()->Id;
        ReturnValue->Status = ev->Get()->Status;
        _Event->Signal();
    }

    STRICT_STFUNC(StateFunc,
        HFunc(TEvents::TEvCompleted, HandleDone);
        HFunc(TEvRunActor, Handle);
    )

public:
    TSyncRunActor(std::shared_ptr<TSystemEvent> event, std::shared_ptr<TSyncRunner::TReturnValue> returnValue)
        : TActor<TSyncRunActor>(&TThis::StateFunc)
        , _Event(event)
        , ReturnValue(returnValue)
    {}
};
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
TSyncRunner::TReturnValue::TReturnValue(ui32 id, ui32 status)
    : Id(id)
    , Status(status)
{}

TSyncRunner::TSyncRunner(TActorSystem *system, TConfiguration *conf)
    : ActorSystem(system)
    , WorkerID()
    , _Event(new TSystemEvent(TSystemEvent::rAuto))
    , ReturnValue (new TReturnValue {0, 0})
    , Conf(conf)
{
    WorkerID = ActorSystem->Register(new TSyncRunActor(_Event, ReturnValue));
}

TActorId TSyncRunner::NotifyID() const {
    return WorkerID;
}

TSyncRunner::TReturnValue TSyncRunner::Run(const TActorContext &ctx, TAutoPtr<IActor> actor) {
    TConfiguration::TTimeoutCallbackId handle = Conf->RegisterTimeoutCallback([&] {
            _Event->Signal();
        });
    _Event->Reset();
    *ReturnValue = TReturnValue {0, 0};
    ctx.Send(WorkerID, new TEvRunActor(actor));
    _Event->Wait();
    Conf->UnregisterTimeoutCallback(handle);
    return *ReturnValue;
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TSyncTestBase
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
TSyncTestBase::TSyncTestBase(TConfiguration *conf)
    : TActorBootstrapped<TSyncTestBase>()
    , Conf(conf)
    , SyncRunner()
{}

void TSyncTestBase::Bootstrap(const TActorContext &ctx) {
    SyncRunner.Reset(new TSyncRunner(ctx.ExecutorThread.ActorSystem, Conf));
    Scenario(ctx);
    AtomicIncrement(Conf->SuccessCount);
    Conf->SignalDoneEvent();
    Die(ctx);
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TSyncTestWithSmallCommonDataset
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
TSyncTestWithSmallCommonDataset::TSyncTestWithSmallCommonDataset(TConfiguration *conf)
    : TSyncTestBase(conf)
    , DataSet()
    , ExpectedSet()
{}


///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TManyPutsToOneVDiskActor
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TManyPutsToOneVDiskActor : public TActorBootstrapped<TManyPutsToOneVDiskActor> {
    typedef TManyPutsToOneVDiskActor TThis;

    const NActors::TActorId NotifyID;
    const TAllVDisks::TVDiskInstance VDiskInfo;
    const IDataSet *DataSet;
    ui8 PartId;
    NKikimrBlobStorage::EPutHandleClass HandleClass;
    ui32 Counter;
    TAutoPtr<IDataSet::TIterator> Cur;

    friend class TActorBootstrapped<TManyPutsToOneVDiskActor>;

    void Bootstrap(const TActorContext &ctx) {
        TThis::Become(&TThis::StateFunc);
        SendRequest(ctx);
    }

    void SendRequest(const TActorContext &ctx) {
        Y_ASSERT(Counter == 0);
        PutLogoBlobToVDisk(ctx, VDiskInfo.ActorID, VDiskInfo.VDiskID, TLogoBlobID(Cur->Get()->Id, PartId),
                           Cur->Get()->Data, HandleClass);
        Counter = 1;
        Cur->Next();
    }

    void Handle(TEvBlobStorage::TEvVPutResult::TPtr &ev, const TActorContext &ctx) {
        Y_ABORT_UNLESS(ev->Get()->Record.GetStatus() == NKikimrProto::OK, "Status=%d", ev->Get()->Record.GetStatus());
        LOG_DEBUG(ctx, NActorsServices::TEST, "  TEvVPutResult succeded");

        --Counter;
        if (Counter == 0) {
            if (Cur->IsValid()) {
                SendRequest(ctx);
            } else {
                // Finished
                ctx.Send(NotifyID, new TEvents::TEvCompleted());
                TThis::Die(ctx);
            }
        }
    }

    STRICT_STFUNC(StateFunc,
        HFunc(TEvBlobStorage::TEvVPutResult, Handle);
        IgnoreFunc(TEvBlobStorage::TEvVWindowChange);
    )

public:
    TManyPutsToOneVDiskActor(const NActors::TActorId &notifyID, const TAllVDisks::TVDiskInstance &vdiskInfo,
                             const IDataSet *dataSet, ui8 partId, NKikimrBlobStorage::EPutHandleClass cls)
        : TActorBootstrapped<TManyPutsToOneVDiskActor>()
        , NotifyID(notifyID)
        , VDiskInfo(vdiskInfo)
        , DataSet(dataSet)
        , PartId(partId)
        , HandleClass(cls)
        , Counter(0)
        , Cur(DataSet->First())
    {}
};

NActors::IActor *ManyPutsToOneVDisk(const NActors::TActorId &notifyID, const TAllVDisks::TVDiskInstance &vdiskInfo,
                                    const IDataSet *dataSet, ui8 partId, NKikimrBlobStorage::EPutHandleClass cls) {
    return new TManyPutsToOneVDiskActor(notifyID, vdiskInfo, dataSet, partId, cls);
}


///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TManyPutsToCorrespondingVDisksActor
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TManyPutsToCorrespondingVDisksActor : public TActorBootstrapped<TManyPutsToCorrespondingVDisksActor> {
    typedef TManyPutsToCorrespondingVDisksActor TThis;

    const NActors::TActorId NotifyID;
    TConfiguration *Conf;
    const IDataSet *DataSet;
    ui32 InFlight;
    ui32 Counter;
    ui32 BlobsSent;
    ui64 BytesSent;
    TInstant PrevTimestamp;
    TInstant UpdateTimestamp;
    TDuration Period = TDuration::Seconds(1);
    ui32 PrevBlobsSent;
    ui64 PrevBytesSent;
    TAutoPtr<IDataSet::TIterator> Cur;
    TPDiskPutStatusHandler PDiskPutStatusHandler;

    friend class TActorBootstrapped<TManyPutsToCorrespondingVDisksActor>;

    void Output(const TLogoBlobID &lb) {
        fprintf(stderr, "%s: %s\n", lb.ToString().data(),
                TIngress::PrintVDisksForLogoBlob(Conf->GroupInfo.Get(), TLogoBlobID(lb, 0)).data());
    }

    void Bootstrap(const TActorContext &ctx) {
        TThis::Become(&TThis::StateFunc);
        PrevTimestamp = Now();
        UpdateTimestamp = PrevTimestamp + Period;
        SendRequest(ctx);
    }

    void SendRequest(const TActorContext &ctx) {
        while (Counter < InFlight && Cur->IsValid()) {
            ++BlobsSent;
            BytesSent += Cur->Get()->Data.size();
            Counter += PutLogoBlobToCorrespondingVDisks(ctx, Conf->GroupInfo.Get(), Cur->Get()->Id,
                                                        Cur->Get()->Data, Cur->Get()->HandleClass);
            Cur->Next();
        }

        TInstant time = Now();
        if (Cur->IsValid() && time >= UpdateTimestamp) {
            ui32 deltaBlobs = BlobsSent - PrevBlobsSent;
            PrevBlobsSent = BlobsSent;
            ui64 deltaBytes = BytesSent - PrevBytesSent;
            PrevBytesSent = BytesSent;

            ui32 secs = Period.Seconds();
            double blobsPerSecond = (double)deltaBlobs / secs;
            double bytesPerSecond = (double)deltaBytes / secs;

            LOG_INFO_S(ctx, NActorsServices::TEST, "BlobsSent# " << BlobsSent << " BytesSent# " << BytesSent <<
                        " blobsPerSecond# " << blobsPerSecond << " bytesPerSecond# " << bytesPerSecond);

            PrevTimestamp = time;
            UpdateTimestamp += Period;
        }
    }

    void Handle(TEvBlobStorage::TEvVPutResult::TPtr &ev, const TActorContext &ctx) {
        if ((*PDiskPutStatusHandler)(ctx, NotifyID, ev)) {
            TThis::Die(ctx);
            return;
        }

        --Counter;
        if (Counter == 0 && !Cur->IsValid()) {
            // Finished
            ctx.Send(NotifyID, new TEvents::TEvCompleted());
            TThis::Die(ctx);
        } else if (Cur->IsValid()) {
            SendRequest(ctx);
        }
    }

    STRICT_STFUNC(StateFunc,
        HFunc(TEvBlobStorage::TEvVPutResult, Handle);
        IgnoreFunc(TEvBlobStorage::TEvVWindowChange);
    )

public:
    TManyPutsToCorrespondingVDisksActor(const NActors::TActorId &notifyID,
                                        TConfiguration *conf,
                                        const IDataSet *dataSet,
                                        TPDiskPutStatusHandler hndl,
                                        ui32 inFlight)
        : TActorBootstrapped<TManyPutsToCorrespondingVDisksActor>()
        , NotifyID(notifyID)
        , Conf(conf)
        , DataSet(dataSet)
        , InFlight(inFlight)
        , Counter(0)
        , BlobsSent(0)
        , BytesSent(0)
        , PrevBlobsSent(0)
        , PrevBytesSent(0)
        , Cur(DataSet->First())
        , PDiskPutStatusHandler(hndl)
    {}
};

NActors::IActor *ManyPutsToCorrespondingVDisks(const NActors::TActorId &notifyID, TConfiguration *conf,
                                               const IDataSet *dataSet, TPDiskPutStatusHandler hndl, ui32 inFlight) {
    return new TManyPutsToCorrespondingVDisksActor(notifyID, conf, dataSet, hndl, inFlight);
}
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


