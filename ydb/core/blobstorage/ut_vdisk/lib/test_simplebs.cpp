#include "test_simplebs.h"
#include "helpers.h"


using namespace NKikimr;

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TBase : public TActorBootstrapped<TBase> {
protected:
    // Implement this virtual call to send a query
    virtual void Start(const TActorContext &ctx) = 0;
    virtual void SendReadRequests(const TActorContext &ctx) = 0;

    TConfiguration *Conf;
    const TAllVDisks::TVDiskInstance VDiskInfo;
    const bool WaitForCompaction;
    ui32 Counter;
    TExpectedSet ExpectedSet;

private:
    friend class TActorBootstrapped<TBase>;

    void Bootstrap(const TActorContext &ctx) {
        Start(ctx);
        bool finished = HandlePutPhaseFinish(ctx);
        if (!finished)
            Become(&TThis::StateFuncPut);
    }

    void Handle(TEvBlobStorage::TEvVPutResult::TPtr &ev, const TActorContext &ctx) {
        Y_ABORT_UNLESS(ev->Get()->Record.GetStatus() == NKikimrProto::OK, "Status=%s",
               NKikimrProto::EReplyStatus_Name(ev->Get()->Record.GetStatus()).data());
        LOG_NOTICE(ctx, NActorsServices::TEST, "  TEvVPutResult succeded");

        --Counter;
        HandlePutPhaseFinish(ctx);
    }

    // true -- finished, false -- otherwise
    bool HandlePutPhaseFinish(const TActorContext &ctx) {
        bool finished = (Counter == 0);
        if (finished) {
            if (WaitForCompaction) {
                Become(&TThis::StateFuncWait);
                ctx.RegisterWithSameMailbox(CreateWaitForCompaction(ctx.SelfID, VDiskInfo));
            } else {
                HandleWaitDone(ctx);
            }
        }
        return finished;
    }

    void HandleWaitDone(const TActorContext &ctx) {
        Become(&TThis::StateFuncGet);
        SendReadRequests(ctx);

    }

    void Handle(TEvBlobStorage::TEvVGetResult::TPtr &ev, const TActorContext &ctx) {
        CheckQueryResult(ev, ctx, EQR_OK_EXPECTED_SET, &ExpectedSet, false);
        --Counter;
        if (Counter == 0) {
            ExpectedSet.Finish();
            AtomicIncrement(Conf->SuccessCount);
            Conf->SignalDoneEvent();
            Die(ctx);
        }

    }

    STRICT_STFUNC(StateFuncPut,
        HFunc(TEvBlobStorage::TEvVPutResult, Handle);
        IgnoreFunc(TEvBlobStorage::TEvVWindowChange);
    )

    STRICT_STFUNC(StateFuncWait,
        CFunc(TEvents::TSystem::Completed, HandleWaitDone);
        IgnoreFunc(TEvBlobStorage::TEvVWindowChange);
    )

    STRICT_STFUNC(StateFuncGet,
        HFunc(TEvBlobStorage::TEvVGetResult, Handle);
        IgnoreFunc(TEvBlobStorage::TEvVWindowChange);
    )

public:
    TBase(TConfiguration *conf, const TAllVDisks::TVDiskInstance &vDiskInfo, bool waitForCompaction)
        : TActorBootstrapped<TBase>()
        , Conf(conf)
        , VDiskInfo(vDiskInfo)
        , WaitForCompaction(waitForCompaction)
        , Counter(0)
        , ExpectedSet()
    {}
};

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TBasePutAllFromDataSet : public TBase {
protected:
    IDataSetPtr DataSetPtr;

    void Start(const TActorContext &ctx) {
        // put logo blob
        Counter = 0;
        TAutoPtr<IDataSet::TIterator> it = DataSetPtr->First();
        while (it->IsValid()) {
            const auto &x = *it->Get();
            PutLogoBlobToVDisk(ctx, VDiskInfo.ActorID, VDiskInfo.VDiskID, x.Id, x.Data, x.HandleClass);
            Counter++;
            it->Next();
        }
    }

public:
    TBasePutAllFromDataSet(TConfiguration *conf, const TAllVDisks::TVDiskInstance &vDiskInfo, IDataSetPtr ds,
                           bool waitForCompaction)
        : TBase(conf, vDiskInfo, waitForCompaction)
        , DataSetPtr(ds)
    {}
};


#define SIMPLE_TEST_BEGIN(name, base)                   \
class name##Actor : public base {                       \
protected:


#define SIMPLE_TEST_END(name, base)                         \
public:                                                     \
    name##Actor(TConfiguration *conf,                       \
                const TAllVDisks::TVDiskInstance &vDiskInfo,\
                IDataSetPtr ds,                             \
                bool waitForCompaction)                     \
        : base(conf, vDiskInfo, ds, waitForCompaction)      \
    {}                                                      \
};                                                          \
void name::operator ()(TConfiguration *conf) {              \
    conf->ActorSystem1->Register(new name##Actor(conf,      \
        conf->VDisks->Get(VDiskNum), DataSetPtr,            \
        WaitForCompaction));                                \
}


///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
SIMPLE_TEST_BEGIN(TSimple3Put3Get, TBasePutAllFromDataSet)
void SendReadRequests(const TActorContext &ctx) {
    // read logoblobs
    const TVector<TDataItem> &ds = DataSetPtr->ToVector();

    for (ui32 i = 0; i < 3; ++i) {
        auto req = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(VDiskInfo.VDiskID, TInstant::Max(),
                NKikimrBlobStorage::EGetHandleClass::AsyncRead, TEvBlobStorage::TEvVGet::EFlags::None, {}, {ds.at(i).Id});
        ctx.Send(VDiskInfo.ActorID, req.release());
    }

    ExpectedSet.Put(ds.at(0).Id, NKikimrProto::OK, ds.at(0).Data);
    ExpectedSet.Put(ds.at(1).Id, NKikimrProto::OK, ds.at(1).Data);
    ExpectedSet.Put(ds.at(2).Id, NKikimrProto::OK, ds.at(2).Data);

    Counter = 3;
}
SIMPLE_TEST_END(TSimple3Put3Get, TBasePutAllFromDataSet)

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
SIMPLE_TEST_BEGIN(TSimple3Put1SeqGetAll, TBasePutAllFromDataSet)
void SendReadRequests(const TActorContext &ctx) {
    // read logoblobs
    const TVector<TDataItem> &ds = DataSetPtr->ToVector();

    auto req = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(VDiskInfo.VDiskID, TInstant::Max(),
            NKikimrBlobStorage::EGetHandleClass::AsyncRead, TEvBlobStorage::TEvVGet::EFlags::None, {},
            {ds.at(0).Id, ds.at(1).Id, ds.at(2).Id});
    ctx.Send(VDiskInfo.ActorID, req.release());

    ExpectedSet.Put(ds.at(0).Id, NKikimrProto::OK, ds.at(0).Data);
    ExpectedSet.Put(ds.at(1).Id, NKikimrProto::OK, ds.at(1).Data);
    ExpectedSet.Put(ds.at(2).Id, NKikimrProto::OK, ds.at(2).Data);

    Counter = 1;
}
SIMPLE_TEST_END(TSimple3Put1SeqGetAll, TBasePutAllFromDataSet)

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
SIMPLE_TEST_BEGIN(TSimple3Put1SeqGet2, TBasePutAllFromDataSet)
void SendReadRequests(const TActorContext &ctx) {
    // read logoblobs
    const TVector<TDataItem> &ds = DataSetPtr->ToVector();

    auto req = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(VDiskInfo.VDiskID, TInstant::Max(),
            NKikimrBlobStorage::EGetHandleClass::AsyncRead, TEvBlobStorage::TEvVGet::EFlags::None, {},
            {ds.at(0).Id, ds.at(2).Id});
    ctx.Send(VDiskInfo.ActorID, req.release());

    ExpectedSet.Put(ds.at(0).Id, NKikimrProto::OK, ds.at(0).Data);
    ExpectedSet.Put(ds.at(2).Id, NKikimrProto::OK, ds.at(2).Data);

    Counter = 1;
}
SIMPLE_TEST_END(TSimple3Put1SeqGet2, TBasePutAllFromDataSet)

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
SIMPLE_TEST_BEGIN(TSimple3Put1SeqSubsOk, TBasePutAllFromDataSet)
void SendReadRequests(const TActorContext &ctx) {
    // use shift/size to read parts of logoblobs
    const TVector<TDataItem> &ds = DataSetPtr->ToVector();

    auto req = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(VDiskInfo.VDiskID, TInstant::Max(),
            NKikimrBlobStorage::EGetHandleClass::AsyncRead, TEvBlobStorage::TEvVGet::EFlags::None, {},
            {{ds.at(0).Id, 2, 5}, {ds.at(1).Id, 0, 3}, {ds.at(2).Id, 2, 1}});
    ctx.Send(VDiskInfo.ActorID, req.release());

    ExpectedSet.Put(ds.at(0).Id, NKikimrProto::OK, ds.at(0).Data.substr(2, 5));//"cdefg"
    ExpectedSet.Put(ds.at(1).Id, NKikimrProto::OK, ds.at(1).Data.substr(0, 3));//"pqr"
    ExpectedSet.Put(ds.at(2).Id, NKikimrProto::OK, ds.at(2).Data.substr(2, 1));//"z"

    Counter = 1;
}
SIMPLE_TEST_END(TSimple3Put1SeqSubsOk, TBasePutAllFromDataSet)

// FIXME: We need a test when we ask for different parts of a logoblob in one request

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
SIMPLE_TEST_BEGIN(TSimple3Put1SeqSubsError, TBasePutAllFromDataSet)
void SendReadRequests(const TActorContext &ctx) {
    // use shift/size to read parts of logoblobs
    const TVector<TDataItem> &ds = DataSetPtr->ToVector();

    auto req = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(VDiskInfo.VDiskID, TInstant::Max(),
            NKikimrBlobStorage::EGetHandleClass::AsyncRead, TEvBlobStorage::TEvVGet::EFlags::None, {},
            {{ds.at(0).Id, 65u << 10u, 5}, {ds.at(1).Id, 0, 65u << 10u}});
    ctx.Send(VDiskInfo.ActorID, req.release());

    ExpectedSet.Put(ds.at(0).Id, NKikimrProto::ERROR, "");
    ExpectedSet.Put(ds.at(1).Id, NKikimrProto::ERROR, "");

    Counter = 1;
}
SIMPLE_TEST_END(TSimple3Put1SeqSubsError, TBasePutAllFromDataSet)

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
SIMPLE_TEST_BEGIN(TSimple3Put1GetMissingKey, TBasePutAllFromDataSet)
void SendReadRequests(const TActorContext &ctx) {
    // use shift/size to read parts of logoblobs

    TLogoBlobID key(10, 1, 1, 0, 10, 0, 1);

    auto req = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(VDiskInfo.VDiskID, TInstant::Max(),
            NKikimrBlobStorage::EGetHandleClass::AsyncRead, TEvBlobStorage::TEvVGet::EFlags::None, {}, {key});
    ctx.Send(VDiskInfo.ActorID, req.release());

    ExpectedSet.Put(key, NKikimrProto::NODATA, "");

    Counter = 1;
}
SIMPLE_TEST_END(TSimple3Put1GetMissingKey, TBasePutAllFromDataSet)

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
SIMPLE_TEST_BEGIN(TSimple3Put1GetMissingPart, TBasePutAllFromDataSet)
void SendReadRequests(const TActorContext &ctx) {
    // use shift/size to read parts of logoblobs
    const TVector<TDataItem> &ds = DataSetPtr->ToVector();

    TLogoBlobID key(ds.at(0).Id, 2);

    auto req = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(VDiskInfo.VDiskID, TInstant::Max(),
            NKikimrBlobStorage::EGetHandleClass::AsyncRead, TEvBlobStorage::TEvVGet::EFlags::None, {}, {key});
    ctx.Send(VDiskInfo.ActorID, req.release());

    ExpectedSet.Put(key, NKikimrProto::NODATA, "");

    Counter = 1;
}
SIMPLE_TEST_END(TSimple3Put1GetMissingPart, TBasePutAllFromDataSet)

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
SIMPLE_TEST_BEGIN(TSimpleHnd6Put1SeqGet, TBasePutAllFromDataSet)
void SendReadRequests(const TActorContext &ctx) {
    // read logoblobs
    const TVector<TDataItem> &ds = DataSetPtr->ToVector();

    ui64 cookie1 = 1;
    ui64 cookie2 = 2;
    ui64 cookie3 = 3;

    TLogoBlobID LogoBlobID1 = TLogoBlobID(ds.at(0).Id, 0);        // LogoBlobID1Part1
    TLogoBlobID LogoBlobID2Part3 = TLogoBlobID(ds.at(3).Id, 3);   // LogoBlobID2Part2

    auto req = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(VDiskInfo.VDiskID, TInstant::Max(),
            NKikimrBlobStorage::EGetHandleClass::AsyncRead, TEvBlobStorage::TEvVGet::EFlags::None, {},
            {{LogoBlobID1, 0, 0, &cookie1}, {LogoBlobID2Part3, 0, 0, &cookie2}, {ds.at(4).Id, 0, 0, &cookie3}});

    ctx.Send(VDiskInfo.ActorID, req.release());

    ExpectedSet.Put(ds.at(0).Id, NKikimrProto::OK, ds.at(0).Data); // LogoBlobID1Part1 "abcdefghkj"
    ExpectedSet.Put(ds.at(1).Id, NKikimrProto::OK, ds.at(1).Data); // LogoBlobID1Part2 "abcdefghkj"
    ExpectedSet.Put(LogoBlobID2Part3, NKikimrProto::NODATA, "");
    ExpectedSet.Put(ds.at(4).Id, NKikimrProto::OK, ds.at(4).Data); // LogoBlobID3Part1 "xyz"

    Counter = 1;
}
SIMPLE_TEST_END(TSimpleHnd6Put1SeqGet, TBasePutAllFromDataSet)


///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
SIMPLE_TEST_BEGIN(TSimpleHnd2Put1Get, TBasePutAllFromDataSet)
void SendReadRequests(const TActorContext &ctx) {
    // read logoblobs
    const TVector<TDataItem> &ds = DataSetPtr->ToVector();

    ui64 cookie = 386;

    auto req = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(VDiskInfo.VDiskID, TInstant::Max(),
            NKikimrBlobStorage::EGetHandleClass::AsyncRead, TEvBlobStorage::TEvVGet::EFlags::None, {},
            {{ds.at(0).Id, 0, 0, &cookie}});
    ctx.Send(VDiskInfo.ActorID, req.release());

    ExpectedSet.Put(ds.at(0).Id, NKikimrProto::OK, ds.at(0).Data);

    Counter = 1;
}
SIMPLE_TEST_END(TSimpleHnd2Put1Get, TBasePutAllFromDataSet)


///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TBasePut3ForRange : public TBasePutAllFromDataSet {
protected:
    using TBasePutAllFromDataSet::SendReadRequests;

    virtual void SendReadRequests(const TActorContext &ctx, const TLogoBlobID &from, const TLogoBlobID &to) {
        LOG_NOTICE(ctx, NActorsServices::TEST, "  Test: from=%s to=%s\n", from.ToString().data(), to.ToString().data());
        auto req = TEvBlobStorage::TEvVGet::CreateRangeIndexQuery(VDiskInfo.VDiskID, TInstant::Max(),
                NKikimrBlobStorage::EGetHandleClass::AsyncRead, TEvBlobStorage::TEvVGet::EFlags::None, {},
                from, to, 10);
        ctx.Send(VDiskInfo.ActorID, req.release());
    }

    virtual void ExpectedSetAll() {
        const TVector<TDataItem> &ds = DataSetPtr->ToVector();
        ExpectedSet.Put(ds.at(0).Id.FullID(), NKikimrProto::OK, {});
        ExpectedSet.Put(ds.at(1).Id.FullID(), NKikimrProto::OK, {});
        ExpectedSet.Put(ds.at(2).Id.FullID(), NKikimrProto::OK, {});

        Counter = 1;
    }

    virtual void ExpectedSetNothing() {
        Counter = 1;
    }

    virtual void ExpectedSetMiddle() {
        const TVector<TDataItem> &ds = DataSetPtr->ToVector();
        ExpectedSet.Put(ds.at(1).Id.FullID(), NKikimrProto::OK, {});

        Counter = 1;
    }

public:
    TBasePut3ForRange(TConfiguration *conf, const TAllVDisks::TVDiskInstance &vDiskInfo, IDataSetPtr ds,
                      bool waitForCompaction)
        : TBasePutAllFromDataSet(conf, vDiskInfo, ds, waitForCompaction)
    {}
};


///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
SIMPLE_TEST_BEGIN(TSimple3PutRangeGetAllForward, TBasePut3ForRange)
using TBasePut3ForRange::SendReadRequests;
void SendReadRequests(const TActorContext &ctx) {
    ExpectedSetAll();
    TLogoBlobID from(DefaultTestTabletId, 0, 0, 0, 0, 0, 1);
    TLogoBlobID to(DefaultTestTabletId, 4294967295, 4294967295, 0, 0, 0, TLogoBlobID::MaxPartId);
    TBasePut3ForRange::SendReadRequests(ctx, from, to);
}
SIMPLE_TEST_END(TSimple3PutRangeGetAllForward, TBasePut3ForRange)

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
SIMPLE_TEST_BEGIN(TSimple3PutRangeGetAllBackward, TBasePut3ForRange)
using TBasePut3ForRange::SendReadRequests;
void SendReadRequests(const TActorContext &ctx) {
    ExpectedSetAll();
    TLogoBlobID from(DefaultTestTabletId, 4294967295, 4294967295, 0, 0, 0, TLogoBlobID::MaxPartId);
    TLogoBlobID to  (DefaultTestTabletId, 0, 0, 0, 0, 0, 1);
    TBasePut3ForRange::SendReadRequests(ctx, from, to);
}
SIMPLE_TEST_END(TSimple3PutRangeGetAllBackward, TBasePut3ForRange)


///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
SIMPLE_TEST_BEGIN(TSimple3PutRangeGetNothingForward, TBasePut3ForRange)
using TBasePut3ForRange::SendReadRequests;
void SendReadRequests(const TActorContext &ctx) {
    ExpectedSetNothing();
    TLogoBlobID from(DefaultTestTabletId + 1, 0, 0, 0, 0, 0, 1);
    TLogoBlobID to(DefaultTestTabletId + 1, 4294967295, 4294967295, 0, TLogoBlobID::MaxBlobSize, 0, TLogoBlobID::MaxPartId);
    TBasePut3ForRange::SendReadRequests(ctx, from, to);
}
SIMPLE_TEST_END(TSimple3PutRangeGetNothingForward, TBasePut3ForRange)

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
SIMPLE_TEST_BEGIN(TSimple3PutRangeGetNothingBackward, TBasePut3ForRange)
using TBasePut3ForRange::SendReadRequests;
void SendReadRequests(const TActorContext &ctx) {
    ExpectedSetNothing();
    TLogoBlobID from(DefaultTestTabletId + 1, 4294967295, 4294967295, 0, TLogoBlobID::MaxBlobSize, 0, TLogoBlobID::MaxPartId);
    TLogoBlobID to  (DefaultTestTabletId + 1, 0, 0, 0, 0, 0, 1);
    TBasePut3ForRange::SendReadRequests(ctx, from, to);
}
SIMPLE_TEST_END(TSimple3PutRangeGetNothingBackward, TBasePut3ForRange)


///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
SIMPLE_TEST_BEGIN(TSimple3PutRangeGetMiddleForward, TBasePut3ForRange)
using TBasePut3ForRange::SendReadRequests;
void SendReadRequests(const TActorContext &ctx) {
    ExpectedSetMiddle();
    TLogoBlobID from(DefaultTestTabletId, 1, 17, 0, 0, 0, 1);
    TLogoBlobID to  (DefaultTestTabletId, 1, 32, 0, TLogoBlobID::MaxBlobSize, 0, TLogoBlobID::MaxPartId);
    TBasePut3ForRange::SendReadRequests(ctx, from, to);
}
SIMPLE_TEST_END(TSimple3PutRangeGetMiddleForward, TBasePut3ForRange)

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
SIMPLE_TEST_BEGIN(TSimple3PutRangeGetMiddleBackward, TBasePut3ForRange)
using TBasePut3ForRange::SendReadRequests;
void SendReadRequests(const TActorContext &ctx) {
    ExpectedSetMiddle();
    TLogoBlobID from(DefaultTestTabletId, 1, 32, 0, TLogoBlobID::MaxBlobSize, 0, TLogoBlobID::MaxPartId);
    TLogoBlobID to  (DefaultTestTabletId, 1, 17, 0, 0, 0, 1);
    TBasePut3ForRange::SendReadRequests(ctx, from, to);
}
SIMPLE_TEST_END(TSimple3PutRangeGetMiddleBackward, TBasePut3ForRange)


///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TSimpleGetFromEmptyDBActor : public TActorBootstrapped<TSimpleGetFromEmptyDBActor> {
    TConfiguration *Conf;
    const TAllVDisks::TVDiskInstance VDiskInfo;
    TLogoBlobID LogoBlobID1;

    friend class TActorBootstrapped<TSimpleGetFromEmptyDBActor>;

    void Bootstrap(const TActorContext &ctx) {
        Become(&TThis::StateFunc);

        auto req = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(VDiskInfo.VDiskID, TInstant::Max(),
                NKikimrBlobStorage::EGetHandleClass::AsyncRead, TEvBlobStorage::TEvVGet::EFlags::None, {}, {LogoBlobID1});
        ctx.Send(VDiskInfo.ActorID, req.release());
    }

    void Handle(TEvBlobStorage::TEvVGetResult::TPtr &ev, const TActorContext &ctx) {
        CheckQueryResult(ev, ctx, EQR_OK_NODATA);
        AtomicIncrement(Conf->SuccessCount);
        Conf->SignalDoneEvent();
        Die(ctx);
    }

    STRICT_STFUNC(StateFunc,
        HFunc(TEvBlobStorage::TEvVGetResult, Handle);
        IgnoreFunc(TEvBlobStorage::TEvVWindowChange);
    )

public:
    TSimpleGetFromEmptyDBActor(TConfiguration *conf)
        : TActorBootstrapped<TSimpleGetFromEmptyDBActor>()
        , Conf(conf)
        , VDiskInfo(Conf->VDisks->Get(0))
        , LogoBlobID1(DefaultTestTabletId, 1, 1, 0, 0, 0)
    {}
};

void TSimpleGetFromEmptyDB::operator ()(TConfiguration *conf) {
    conf->ActorSystem1->Register(new TSimpleGetFromEmptyDBActor(conf));
}
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TRangeGetFromEmptyDBActor : public TActorBootstrapped<TRangeGetFromEmptyDBActor> {
    TConfiguration *Conf;
    const TAllVDisks::TVDiskInstance VDiskInfo;

    friend class TActorBootstrapped<TRangeGetFromEmptyDBActor>;

    void Bootstrap(const TActorContext &ctx) {
        Become(&TThis::StateFunc);

        TLogoBlobID from(DefaultTestTabletId, 4294967295, 4294967295, 0, 0, 0, TLogoBlobID::MaxPartId);
        TLogoBlobID to  (DefaultTestTabletId, 0, 0, 0, 0, 0, 1);
        LOG_NOTICE(ctx, NActorsServices::TEST, "  Test: from=%s to=%s\n", from.ToString().data(), to.ToString().data());
        auto req = TEvBlobStorage::TEvVGet::CreateRangeIndexQuery(VDiskInfo.VDiskID, TInstant::Max(),
                NKikimrBlobStorage::EGetHandleClass::AsyncRead, TEvBlobStorage::TEvVGet::EFlags::None, {}, from, to, 10);
        ctx.Send(VDiskInfo.ActorID, req.release());
    }

    void Handle(TEvBlobStorage::TEvVGetResult::TPtr &ev, const TActorContext &ctx) {
        CheckQueryResult(ev, ctx, EQR_OK_EMPTY);
        AtomicIncrement(Conf->SuccessCount);
        Conf->SignalDoneEvent();
        Die(ctx);
    }

    STRICT_STFUNC(StateFunc,
        HFunc(TEvBlobStorage::TEvVGetResult, Handle);
        IgnoreFunc(TEvBlobStorage::TEvVWindowChange);
    )

public:
    TRangeGetFromEmptyDBActor(TConfiguration *conf)
        : TActorBootstrapped<TRangeGetFromEmptyDBActor>()
        , Conf(conf)
        , VDiskInfo(Conf->VDisks->Get(0))
    {}
};

void TRangeGetFromEmptyDB::operator ()(TConfiguration *conf) {
    conf->ActorSystem1->Register(new TRangeGetFromEmptyDBActor(conf));
}
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

