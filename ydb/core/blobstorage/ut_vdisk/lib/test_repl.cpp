#include "test_repl.h"
#include "helpers.h"
#include <ydb/core/blobstorage/vdisk/repl/blobstorage_replproxy.h>
#include <ydb/core/blobstorage/backpressure/queue_backpressure_client.h>

using namespace NKikimr;
using namespace NKikimr::NRepl;

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TReadUntilSuccessActor : public TActorBootstrapped<TReadUntilSuccessActor> {
    TConfiguration *Conf;
    const IDataSet *DataSet;
    TAllVDisks::TVDiskInstance &VDisk;
    ui32 Counter;
    TDuration RepeatTimeout;
    bool Multipart;

    struct TVal {
        TString Data;
        ui32 PartMask;

        TVal(const TString &data)
            : Data(data)
            , PartMask(0)
        {}

        bool AddReply(const TLogoBlobID& id) {
            const ui8 partId = id.PartId();
            Y_ABORT_UNLESS(partId > 0);
            const ui32 mask = 1 << (partId - 1);
            Y_ABORT_UNLESS(!(PartMask & mask));
            PartMask |= mask;
            return PartMask == 7;
        }
    };
    typedef TMap<TLogoBlobID, TVal> TReadSet;
    TReadSet ReadSet;
    typedef TSet<TLogoBlobID> TPendingReads;
    TPendingReads PendingReads;
    bool HaveNoData;

    friend class TActorBootstrapped<TReadUntilSuccessActor>;

    void SendAllMessages(const TActorContext &ctx) {
        std::unique_ptr<TEvBlobStorage::TEvVGet> req;
        ui32 numBlobs = 0;

        // send at most 10 packets of 64 requests
        for (auto it = PendingReads.begin(), e = PendingReads.end(); Counter < 10; ++it) {
            if (!req)
                req = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(VDisk.VDiskID,
                                                                      TInstant::Max(),
                                                                      NKikimrBlobStorage::EGetHandleClass::AsyncRead);
            if (it != e)
                req->AddExtremeQuery(*it, 0, 0);
            if (++numBlobs == 64 || it == e) {
                if (numBlobs != 0) {
                    ctx.Send(VDisk.ActorID, req.release());
                    ++Counter;
                }
                numBlobs = 0;
                if (it == e)
                    break;
            }
        }

        HaveNoData = false;
    }

    void Bootstrap(const TActorContext &ctx) {
        ui32 numBlobs = 0;
        ui64 blobsSize = 0;

        TAutoPtr<IDataSet::TIterator> it = DataSet->First();
        Y_ABORT_UNLESS(it->IsValid());
        while (it->IsValid()) {
            ReadSet.insert(std::pair<TLogoBlobID, TVal>(it->Get()->Id, TVal(it->Get()->Data)));
            PendingReads.insert(it->Get()->Id);
            ++numBlobs;
            blobsSize += it->Get()->Data.size();
            it->Next();
        }

        LOG_INFO_S(ctx, NActorsServices::TEST, "numBlobs# " << numBlobs << " blobsSize# " << blobsSize);

        Become(&TThis::StateFunc);
        ctx.Schedule(RepeatTimeout, new TEvents::TEvWakeup());
        SendAllMessages(ctx);
    }

    void Handle(TEvBlobStorage::TEvVGetResult::TPtr &ev, const TActorContext &ctx) {
        --Counter;

        LOG_DEBUG_S(ctx, NActorsServices::TEST, "received reply " << ev->Get()->ToString());

        ui32 ok = 0, notOk = 0, dataCorrupt = 0, miss = 0, noData = 0;

        if (ev->Get()->Record.GetStatus() == NKikimrProto::OK) {
            const NKikimrBlobStorage::TEvVGetResult &rec = ev->Get()->Record;
            for (const auto& q : rec.GetResult()) {
                if (q.GetStatus() == NKikimrProto::OK) {
                    const TLogoBlobID id = TLogoBlobID(LogoBlobIDFromLogoBlobID(q.GetBlobID()), 0);
                    const TReadSet::iterator it = ReadSet.find(id);
                    if (it != ReadSet.end()) {
                        if (it->second.Data == ev->Get()->GetBlobData(q).ConvertToString()) {
                            if (!Multipart || it->second.AddReply(LogoBlobIDFromLogoBlobID(q.GetBlobID()))) {
                                ReadSet.erase(it);
                            }
                            PendingReads.erase(id);
                            ++ok;
                        } else {
                            ++dataCorrupt;
                        }
                    } else {
                        ++miss;
                    }
                } else if (q.GetStatus() == NKikimrProto::NODATA) {
                    HaveNoData = true;
                    ++noData;
                } else {
                    HaveNoData = true;
                    ++notOk;
                }
            }
        }

        LOG_INFO_S(ctx, NActorsServices::TEST, "ok# " << ok << " notOk# " << notOk <<
                " noData# " << noData << " dataCorrupt# " << dataCorrupt <<
                " miss# " << miss << " remain# " << PendingReads.size());

        if (!Counter && PendingReads.empty()) {
            if (ReadSet.empty()) {
                AtomicIncrement(Conf->SuccessCount);
            }
            Conf->SignalDoneEvent();
            Die(ctx);
        } else if (!Counter && !HaveNoData) {
            SendAllMessages(ctx);
        }
     }

    void HandleWakeup(const TActorContext &ctx) {
        if (!Counter)
            SendAllMessages(ctx);
        ctx.Schedule(RepeatTimeout, new TEvents::TEvWakeup());
    }


    STRICT_STFUNC(StateFunc,
        HFunc(TEvBlobStorage::TEvVGetResult, Handle);
        CFunc(TEvents::TSystem::Wakeup, HandleWakeup);
        IgnoreFunc(TEvBlobStorage::TEvVWindowChange);
    )

public:
    TReadUntilSuccessActor(TConfiguration *conf, const IDataSet *dataSet, ui32 vdiskNumber, const TDuration &repeatTimeout,
            bool multipart)
        : TActorBootstrapped<TReadUntilSuccessActor>()
        , Conf(conf)
        , DataSet(dataSet)
        , VDisk(Conf->VDisks->Get(vdiskNumber))
        , Counter(0)
        , RepeatTimeout(repeatTimeout)
        , Multipart(multipart)
    {}
};

void TReadUntilSuccess::operator ()(TConfiguration *conf) {
    conf->ActorSystem1->Register(new TReadUntilSuccessActor(conf, DataSet, VDiskNumber, RepeatTimeout, Multipart));
}



///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

class TVDiskReplProxyReaderActor : public TActorBootstrapped<TVDiskReplProxyReaderActor> {
private:
    const TActorId NotifyID;
    const TAllVDisks::TVDiskInstance VDiskInfo;
    TConfiguration *Conf;
    TVDiskContextPtr VCtx;
    std::shared_ptr<TReplCtx> ReplCtx;
    TVDiskProxyPtr Proxy;
    IDataSet *DataSet;
    TAutoPtr<TExpectedSet> ExpectedSetPtr;
    TActorId QueueId;
    bool Running = false;
    /*
    void Put(const NKikimr::TLogoBlobID &id, NKikimrProto::EReplyStatus status, const TString &data);
    void Check(const NKikimr::TLogoBlobID &id, NKikimrProto::EReplyStatus status, const TString &data);
    void Finish();
    */

    // FIXME: make logic ready for multiple Next

    friend class TActorBootstrapped<TVDiskReplProxyReaderActor>;

    void Bootstrap(const TActorContext &ctx) {
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters = new ::NMonitoring::TDynamicCounters;
        auto groupInfo = TBlobStorageGroupInfo(TBlobStorageGroupType::ErasureMirror3, 2, 4);
        VCtx.Reset(new TVDiskContext(ctx.SelfID, groupInfo.PickTopology(), counters, VDiskInfo.VDiskID,
                ctx.ExecutorThread.ActorSystem, NPDisk::DEVICE_TYPE_UNKNOWN));

        ReplCtx = std::make_shared<TReplCtx>(
                VCtx,
                nullptr,
                nullptr, // PDiskCtx
                nullptr, // HugeBlobCtx
                4097,
                nullptr,
                MakeIntrusive<TBlobStorageGroupInfo>(groupInfo),
                ctx.SelfID,
                VDiskInfo.Cfg,
                std::make_shared<std::atomic_uint64_t>());

        TBSProxyContextPtr bspctx = new TBSProxyContext(counters);
        TIntrusivePtr<NBackpressure::TFlowRecord> flowRecord(new NBackpressure::TFlowRecord);
        QueueId = ctx.Register(CreateVDiskBackpressureClient(Conf->GroupInfo, VDiskInfo.VDiskID,
            NKikimrBlobStorage::EVDiskQueueId::PutTabletLog, counters, bspctx, NBackpressure::TQueueClientId(),
            "PutTabletLog", 0, false, TDuration::Minutes(10), flowRecord,
            NMonitoring::TCountableBase::EVisibility::Public));
        Proxy.Reset(new TVDiskProxy(ReplCtx, VDiskInfo.VDiskID, QueueId));

        TAutoPtr<IDataSet::TIterator> it = DataSet->First();
        while (it->IsValid()) {
            Proxy->Put(it->Get()->Id, 0);
            it->Next();
        }

        Become(&TThis::StateRead);
    }

    void Handle(TEvReplProxyNextResult::TPtr &ev, const TActorContext &ctx) {
        Proxy->HandleNext(ev);

        while (Proxy->Valid()) {
            TLogoBlobID id;
            NKikimrProto::EReplyStatus status;
            TRope data;
            Proxy->FetchData(&id, &status, &data);
            if (status == NKikimrProto::OK) {
                ExpectedSetPtr->Check(id, status, data.ConvertToString());
            }
        }

        if (Proxy->IsEof()) {
            ExpectedSetPtr->Finish();
            ctx.Send(NotifyID, new TEvents::TEvCompleted());
            ctx.Send(QueueId, new TEvents::TEvPoisonPill);
            Die(ctx);
        } else {
            Proxy->SendNextRequest();
        }
    }

    void Handle(TEvProxyQueueState::TPtr& ev, const TActorContext& /*ctx*/) {
        if (ev->Get()->IsConnected && !Running) {
            Proxy->Run(SelfId());
            Running = true;
        }
    }

    STRICT_STFUNC(StateRead,
        HFunc(TEvReplProxyNextResult, Handle);
        HFunc(TEvProxyQueueState, Handle);
        IgnoreFunc(TEvBlobStorage::TEvVWindowChange);
    )

public:
    TVDiskReplProxyReaderActor(TConfiguration *conf, const TActorId &notifyID,
                               const TAllVDisks::TVDiskInstance &vdiskInfo, TAutoPtr<TExpectedSet> expSetPtr,
                               IDataSet *dataSet)
        : TActorBootstrapped<TVDiskReplProxyReaderActor>()
        , NotifyID(notifyID)
        , VDiskInfo(vdiskInfo)
        , Conf(conf)
        , VCtx()
        , Proxy()
        , DataSet(dataSet)
        , ExpectedSetPtr(expSetPtr)
    {}
};

IActor *CreateVDiskReplProxyReader(TConfiguration *conf, const TActorId &notifyID,
                                   const TAllVDisks::TVDiskInstance &vdiskInfo,
                                   TAutoPtr<TExpectedSet> expSetPtr, IDataSet *dataSet) {
    return new TVDiskReplProxyReaderActor(conf, notifyID, vdiskInfo, expSetPtr, dataSet);
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
SYNC_TEST_WITH_DATASET_BEGIN(TTestReplDataWriteAndSync)
virtual void Scenario(const TActorContext &ctx) {
    // load data
    SyncRunner->Run(ctx, ManyPutsToCorrespondingVDisks(SyncRunner->NotifyID(), Conf, DataSet));
    LOG_NOTICE(ctx, NActorsServices::TEST, "  Data is loaded");

    // wait for sync
    SyncRunner->Run(ctx, CreateWaitForSync(SyncRunner->NotifyID(), Conf));
    LOG_NOTICE(ctx, NActorsServices::TEST, "  SYNC done");
}
SYNC_TEST_WITH_DATASET_END(TTestReplDataWriteAndSync)

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
SYNC_TEST_WITH_DATASET_BEGIN(TTestReplDataWriteAndSyncMultipart)
virtual void Scenario(const TActorContext &ctx) {
    // load data
    SyncRunner->Run(ctx, ManyPutsToCorrespondingVDisks(SyncRunner->NotifyID(), Conf, DataSet));
    LOG_NOTICE(ctx, NActorsServices::TEST, "  Data is loaded");

    // duplicate data to handoff
    for (ui8 part = 1; part <= 3; ++part) {
        SyncRunner->Run(ctx, ManyPutsToOneVDisk(SyncRunner->NotifyID(), Conf->VDisks->Get(3), DataSet, part,
                NKikimrBlobStorage::EPutHandleClass::TabletLog));
    }
    LOG_NOTICE(ctx, NActorsServices::TEST, "  Data is duplicated");

    // wait for sync
    SyncRunner->Run(ctx, CreateWaitForSync(SyncRunner->NotifyID(), Conf));
    LOG_NOTICE(ctx, NActorsServices::TEST, "  SYNC done");
}
SYNC_TEST_WITH_DATASET_END(TTestReplDataWriteAndSyncMultipart)




///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
SYNC_TEST_BEGIN(TTestReplProxyData, TSyncTestWithSmallCommonDataset)
virtual void Scenario(const TActorContext &ctx) {
    // load data
    SyncRunner->Run(ctx, ManyPutsToCorrespondingVDisks(SyncRunner->NotifyID(), Conf, &DataSet));
    LOG_NOTICE(ctx, NActorsServices::TEST, "  Data is loaded");

    // wait for sync
    SyncRunner->Run(ctx, CreateWaitForSync(SyncRunner->NotifyID(), Conf));
    LOG_NOTICE(ctx, NActorsServices::TEST, "  SYNC done");

    TAllVDisks::TVDiskInstance &instance = Conf->VDisks->Get(0);
    for (ui32 i = 1; i < 7; i++) {
        TAutoPtr<TExpectedSet> expSetPtr(new TExpectedSet());
        TAutoPtr<IDataSet::TIterator> it = DataSet.First();
        while (it->IsValid()) {
            expSetPtr->Put(TLogoBlobID(it->Get()->Id, 1), NKikimrProto::OK, it->Get()->Data);
            it->Next();
        }

        SyncRunner->Run(ctx, CreateVDiskReplProxyReader(Conf, SyncRunner->NotifyID(), instance, expSetPtr, &DataSet));
        LOG_NOTICE(ctx, NActorsServices::TEST, "  REPL PROXY READER done");
    }
}
SYNC_TEST_END(TTestReplProxyData, TSyncTestWithSmallCommonDataset)


///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
SYNC_TEST_BEGIN(TTestReplProxyKeepBits, TSyncTestWithSmallCommonDataset)
virtual void Scenario(const TActorContext &ctx) {
    // prepare gc command
    ui64 tabletID = DefaultTestTabletId;
    ui32 recGen = 1;
    ui32 recGenCounter = 1;
    ui32 channel = 0;
    bool collect = true;
    ui32 collectGen = 1;
    ui32 collectStep = 40;
    TAutoPtr<TVector<NKikimr::TLogoBlobID>> keep(new TVector<NKikimr::TLogoBlobID>());
    keep->push_back(TLogoBlobID(DefaultTestTabletId, 1, 37, 0, 0, 0));
    TAutoPtr<IActor> gcCommand(PutGCToCorrespondingVDisks(SyncRunner->NotifyID(), Conf, tabletID, recGen, recGenCounter,
                                                          channel, collect, collectGen, collectStep, keep, nullptr));
    // set gc settings
    SyncRunner->Run(ctx, gcCommand);
    LOG_NOTICE(ctx, NActorsServices::TEST, "  GC Message sent");

    // wait for sync
    SyncRunner->Run(ctx, CreateWaitForSync(SyncRunner->NotifyID(), Conf));
    LOG_NOTICE(ctx, NActorsServices::TEST, "  SYNC done");
    // wait for compaction
    SyncRunner->Run(ctx, CreateWaitForCompaction(SyncRunner->NotifyID(), Conf));
    LOG_NOTICE(ctx, NActorsServices::TEST, "  COMPACTION done");


    TAllVDisks::TVDiskInstance &instance = Conf->VDisks->Get(0);
    for (ui32 i = 1; i < 7; i++) {
        TAutoPtr<TExpectedSet> expSetPtr(new TExpectedSet()); // empty, it's ok
        SyncRunner->Run(ctx, CreateVDiskReplProxyReader(Conf, SyncRunner->NotifyID(), instance, expSetPtr, &DataSet));
        LOG_NOTICE(ctx, NActorsServices::TEST, "  REPL PROXY READER done");
    }
}
SYNC_TEST_END(TTestReplProxyKeepBits, TSyncTestWithSmallCommonDataset)


///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
SYNC_TEST_BEGIN(TTestCollectAllSimpleDataset, TSyncTestBase)
virtual void Scenario(const TActorContext &ctx) {
    // prepare gc command
    ui64 tabletID = DefaultTestTabletId;
    ui32 recGen = 1;
    ui32 recGenCounter = 1;
    ui32 channel = 1;
    bool collect = true;
    ui32 collectGen = 1;
    ui32 collectStep = 1000;
    TAutoPtr<IActor> gcCommand(PutGCToCorrespondingVDisks(SyncRunner->NotifyID(), Conf, tabletID, recGen,
                                                          recGenCounter, channel, collect, collectGen, collectStep,
                                                          nullptr, nullptr));
    // set gc settings
    SyncRunner->Run(ctx, gcCommand);
    LOG_NOTICE(ctx, NActorsServices::TEST, "  GC Message sent");

    // wait for sync
    SyncRunner->Run(ctx, CreateWaitForSync(SyncRunner->NotifyID(), Conf));
    LOG_NOTICE(ctx, NActorsServices::TEST, "  SYNC done");

    // wait for compaction
    SyncRunner->Run(ctx, CreateWaitForCompaction(SyncRunner->NotifyID(), Conf));
    LOG_NOTICE(ctx, NActorsServices::TEST, "  COMPACTION done");

    // wait for sync
    SyncRunner->Run(ctx, CreateWaitForSync(SyncRunner->NotifyID(), Conf));
    LOG_NOTICE(ctx, NActorsServices::TEST, "  SYNC done");

    // wait for compaction
    SyncRunner->Run(ctx, CreateWaitForCompaction(SyncRunner->NotifyID(), Conf));
    LOG_NOTICE(ctx, NActorsServices::TEST, "  COMPACTION done");
}
SYNC_TEST_END(TTestCollectAllSimpleDataset, TSyncTestBase)

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
SYNC_TEST_BEGIN(TTestStub, TSyncTestBase)
virtual void Scenario(const TActorContext &ctx) {
    // do nothing
    Y_UNUSED(ctx);
}
SYNC_TEST_END(TTestStub, TSyncTestBase)

