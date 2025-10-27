#pragma once

#include "defs.h"
#include "dataset.h"
#include "prepare.h"
#include "helpers_impl.h"

#include <util/generic/ptr.h>

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Interface for generating EPutHandleClass values
////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class IPutHandleClassGenerator {
public:
    virtual NKikimrBlobStorage::EPutHandleClass GetHandleClass() = 0;
    virtual ~IPutHandleClassGenerator() {}
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TPutHandleClassGenerator -- default implementation for IPutHandleClassGenerator
////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TPutHandleClassGenerator : public IPutHandleClassGenerator {
public:
    TPutHandleClassGenerator(NKikimrBlobStorage::EPutHandleClass cls)
        : HandleClass(cls)
    {}

    NKikimrBlobStorage::EPutHandleClass GetHandleClass() override {
        return HandleClass;
    }

private:
    NKikimrBlobStorage::EPutHandleClass HandleClass;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// PDisk Put status handlers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////
using TPDiskPutStatusHandler = bool (*)(const NActors::TActorContext &,
                                        const NActors::TActorId &notifyId,
                                        NKikimr::TEvBlobStorage::TEvVPutResult::TPtr &);

bool PDiskPutStatusHandlerDefault(const NActors::TActorContext &ctx,
                                  const NActors::TActorId &notifyId,
                                  NKikimr::TEvBlobStorage::TEvVPutResult::TPtr &ev);

bool PDiskPutStatusHandlerErrorAware(const NActors::TActorContext &ctx,
                                     const NActors::TActorId &notifyId,
                                     NKikimr::TEvBlobStorage::TEvVPutResult::TPtr &ev);

bool PDiskPutStatusHandlerYellowMoveZone(const NActors::TActorContext &ctx,
                                     const NActors::TActorId &notifyId,
                                     NKikimr::TEvBlobStorage::TEvVPutResult::TPtr &ev);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// PDisk Get status handlers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////
using TPDiskGetStatusHandler = bool (*)(const NActors::TActorContext &,
                                        const NActors::TActorId &notifyId,
                                        NKikimr::TEvBlobStorage::TEvVGetResult::TPtr &);

bool PDiskGetStatusHandlerDefault(const NActors::TActorContext &ctx,
                                  const NActors::TActorId &notifyId,
                                  NKikimr::TEvBlobStorage::TEvVGetResult::TPtr &ev);

bool PDiskGetStatusHandlerErrorAware(const NActors::TActorContext &ctx,
                                     const NActors::TActorId &notifyId,
                                     NKikimr::TEvBlobStorage::TEvVGetResult::TPtr &ev);



////////////////////////////////////////////////////////////////////////////////////////////////////////////////
NActors::IActor *CreateRangeGet(const NActors::TActorId &notifyID, const TAllVDisks::TVDiskInstance &vdiskInfo,
                                const NKikimr::TLogoBlobID &readFrom, const NKikimr::TLogoBlobID &readTo,
                                bool indexOnly, ui32 maxResults);

NActors::IActor *CreateManyPuts(TConfiguration *conf, const NActors::TActorId &notifyID,
                                const TAllVDisks::TVDiskInstance &vdiskInfo,
                                ui32 msgDataSize, ui32 msgNum, ui64 tabletId, ui32 channel, ui32 gen,
                                std::shared_ptr<IPutHandleClassGenerator> cls, std::shared_ptr<TSet<ui32>> badSteps,
                                TDuration requestTimeout);

struct TMsgPackInfo {
    ui32 Count;
    TString MsgData;

    TMsgPackInfo(ui32 dataSize, ui32 count)
        : Count(count)
    {
        MsgData.reserve(dataSize);
        for (ui32 i = 0; i < dataSize; i++) {
            MsgData.append('a' + i % 26);
        }
    }
};


NActors::IActor *CreateManyPuts(TConfiguration *conf, const NActors::TActorId &notifyID,
                                const TAllVDisks::TVDiskInstance &vdiskInfo,
                                std::shared_ptr<TVector<TMsgPackInfo>> msgPacks, ui64 tabletId, ui32 channel, ui32 gen,
                                std::shared_ptr<IPutHandleClassGenerator> cls, std::shared_ptr<TSet<ui32>> badSteps,
                                TDuration requestTimeout);

NActors::IActor *CreateManyMultiPuts(TConfiguration *conf, const NActors::TActorId &notifyID,
                                     const TAllVDisks::TVDiskInstance &vdiskInfo,
                                     ui32 msgDataSize, ui32 msgNum, ui32 batchSize,
                                     ui64 tabletId, ui32 channel, ui32 gen,
                                     std::shared_ptr<IPutHandleClassGenerator> cls,
                                     std::shared_ptr<TSet<ui32>> badSteps, TDuration requestTimeout);

NActors::IActor *CreateManyGets(const NActors::TActorId &notifyID, const TAllVDisks::TVDiskInstance &vdiskInfo,
                                ui32 msgDataSize, ui32 msgNum, ui64 tabletId, ui32 channel, ui32 gen,
                                std::shared_ptr<TSet<ui32>> badSteps);

NActors::IActor *CreateGet(const NActors::TActorId &notifyID, const TAllVDisks::TVDiskInstance &vdiskInfo,
                           ui32 msgDataSize, ui32 msgNum, ui64 tabletId, ui32 channel, ui32 gen, ui64 shift,
                           bool withErrorResponse);

NActors::IActor *CreateGet(const NActors::TActorId &notifyID, const TAllVDisks::TVDiskInstance &vdiskInfo,
                           std::shared_ptr<TVector<TMsgPackInfo>> msgPacks, ui64 tabletId, ui32 channel, ui32 gen,
                           ui64 shift, bool withErrorResponse);

NActors::IActor *CreatePutGC(const NActors::TActorId &notifyID, const TAllVDisks::TVDiskInstance &vdiskInfo,
                             ui64 tabletID, ui32 recGen, ui32 recGenCounter, ui32 channel, bool collect, ui32 collectGen,
                             ui32 collectStep, TAutoPtr<TVector<NKikimr::TLogoBlobID>> keep,
                             TAutoPtr<TVector<NKikimr::TLogoBlobID>> doNotKeep);

NActors::IActor *CreateWaitForCompaction(const NActors::TActorId &notifyID, const TAllVDisks::TVDiskInstance &vdiskInfo, bool sync=false);
NActors::IActor *CreateWaitForCompaction(const NActors::TActorId &notifyID, TConfiguration *conf, bool sync=false);
NActors::IActor *CreateWaitForSync(const NActors::TActorId &notifyID, TConfiguration *conf);
NActors::IActor *CreateCheckDbEmptyness(const NActors::TActorId &notifyID, const TAllVDisks::TVDiskInstance &vdiskInfo,
                                        bool expectEmpty);
NActors::IActor *CreateDefrag(const NActors::TActorId &notifyID, const TAllVDisks::TVDiskInstance &vdiskInfo,
    bool full, std::function<void(NKikimr::TEvBlobStorage::TEvVDefragResult::TPtr &)> check);
NActors::IActor *CreateDefrag(const NActors::TActorId &notifyID, TConfiguration *conf,
    bool full, std::function<void(NKikimr::TEvBlobStorage::TEvVDefragResult::TPtr &)> check);

template <class TRequestFunc, class TCheckResponseFunc>
NActors::IActor *CreateOneGet(const NActors::TActorId &notifyID, TRequestFunc req, TCheckResponseFunc check);


////////////////////////////////////////////////////////////////////////////////////////////////////////////////
NActors::IActor *ManyPutsToOneVDisk(const NActors::TActorId &notifyID, const TAllVDisks::TVDiskInstance &vdiskInfo,
                                    const IDataSet *dataSet, ui8 partId, NKikimrBlobStorage::EPutHandleClass cls);

NActors::IActor *ManyPutsToCorrespondingVDisks(const NActors::TActorId &notifyID,
                                               TConfiguration *conf,
                                               const IDataSet *dataSet,
                                               TPDiskPutStatusHandler hndl = PDiskPutStatusHandlerDefault,
                                               ui32 inFlight = 300);

struct TGCSettings {
    ui64 TabletID;
    ui32 RecGen;
    ui32 RecGenCounter;
    ui32 Channel;
    bool Collect;
    ui32 CollectGen;
    ui32 CollectStep;
};
NActors::IActor *PutGCToCorrespondingVDisks(const NActors::TActorId &notifyID, TConfiguration *conf, ui64 tabletID,
                                            ui32 recGen, ui32 recGenCounter, ui32 channel, bool collect,
                                            ui32 collectGen, ui32 collectStep,
                                            TAutoPtr<TVector<NKikimr::TLogoBlobID>> keep,
                                            TAutoPtr<TVector<NKikimr::TLogoBlobID>> doNotKeep);
NActors::IActor *PutGCToCorrespondingVDisks(const NActors::TActorId &notifyID, TConfiguration *conf,
                                            const TGCSettings &settings,
                                            TAutoPtr<TVector<NKikimr::TLogoBlobID>> keep,
                                            TAutoPtr<TVector<NKikimr::TLogoBlobID>> doNotKeep);



////////////////////////////////////////////////////////////////////////////////////////////////////////////////


////////////////////////////////////////////////////////////////////////////////////////////////////////////////
void PutLogoBlobToVDisk(const NActors::TActorContext &ctx, const NActors::TActorId &actorID,
                        const NKikimr::TVDiskID &vdiskID, const NKikimr::TLogoBlobID &id, const TString &data,
                        NKikimrBlobStorage::EPutHandleClass cls);

// Generate a series of put messages of logoblob id to the corresponding VDisks
// Returns number of messages sent
ui32 PutLogoBlobToCorrespondingVDisks(const NActors::TActorContext &ctx, NKikimr::TBlobStorageGroupInfo *info,
                                      const NKikimr::TLogoBlobID &id, const TString &data,
                                      NKikimrBlobStorage::EPutHandleClass cls);

// Generate a series of get messages of logoblob id to the corresponding VDisks
// Returns number of messages sent
ui32 GetLogoBlobFromCorrespondingVDisks(const NActors::TActorContext &ctx, NKikimr::TBlobStorageGroupInfo *info,
                                        const NKikimr::TLogoBlobID &id);


////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TDataSnapshot : public TThrRefBase {
public:
    struct TItem {
        NKikimr::TVDiskID VDiskID;
        NKikimr::TActorId ServiceID;
        NKikimr::TLogoBlobID Id; // exact part
        TString Data;
        NKikimr::TIngress Ingress;

        TItem(const NKikimr::TVDiskID &vdisk, const NKikimr::TActorId &service, const NKikimr::TLogoBlobID &id,
              const TString &data, const NKikimr::TIngress &ingress);
    };

    struct TLess;

    typedef TVector<TItem> TData;
    typedef TData::iterator TIterator;

    TDataSnapshot(TIntrusivePtr<NKikimr::TBlobStorageGroupInfo> info);
    void PutExact(const NKikimr::TVDiskID &vdisk, const NKikimr::TActorId &service, const NKikimr::TLogoBlobID &id,
                  const TString &data, const NKikimr::TIngress &ingress = NKikimr::TIngress(0)); // must provide exact partId
    void PutCorresponding(const NKikimr::TLogoBlobID &id, const TString &data);
    void SortAndCheck();

    TIterator begin();
    TIterator end();
private:
    TIntrusivePtr<NKikimr::TBlobStorageGroupInfo> Info;
    TData Data;
};

typedef TIntrusivePtr<TDataSnapshot> TDataSnapshotPtr;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
NActors::IActor *CreateLoadDataSnapshot(const NActors::TActorId &notifyID, TConfiguration *conf,
                                        TDataSnapshotPtr dataPtr, NKikimrBlobStorage::EPutHandleClass cls,
                                        TPDiskPutStatusHandler hndl = &PDiskPutStatusHandlerDefault);
NActors::IActor *CreateCheckDataSnapshot(const NActors::TActorId &notifyID, TConfiguration *conf, TDataSnapshotPtr dataPtr,
                                         TPDiskGetStatusHandler hndl = &PDiskGetStatusHandlerDefault);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////

void PrintDebug(NKikimr::TEvBlobStorage::TEvVGetResult::TPtr &ev, const NActors::TActorContext &ctx,
                NKikimr::TBlobStorageGroupInfo *info, const NKikimr::TVDiskID &vdisk);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TExpectedSet {
public:
    void Put(const NKikimr::TLogoBlobID &id, NKikimrProto::EReplyStatus status, const TString &data);
    void Check(const NKikimr::TLogoBlobID &id, NKikimrProto::EReplyStatus status, const TString &data);
    void Finish();
    TString ToString() const;

    void Print() const {
        fprintf(stderr, "TExpectedSet:\n");
        for (auto i : Map) {
            fprintf(stderr, "  id=%s status=%s data=%s\n", i.first.ToString().data(),
                    NKikimrProto::EReplyStatus_Name(i.second.Status).data(), i.second.Data.data());
        }
    }

    struct TExpectedResult {
        NKikimrProto::EReplyStatus Status;
        TString Data;

        TExpectedResult(NKikimrProto::EReplyStatus status, const TString &data);
    };

private:
    typedef TMap<NKikimr::TLogoBlobID, TExpectedResult> TMapType;
    TMapType Map;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
enum EQueryResult {
    EQR_OK_NODATA = 1,
    EQR_OK_EMPTY = 2,
    EQR_OK_EXPECTED_SET = 3
};

void CheckQueryResult(NKikimr::TEvBlobStorage::TEvVGetResult::TPtr &ev, const NActors::TActorContext &ctx,
                      EQueryResult eqr, TExpectedSet *expSet = nullptr,
                      bool fullResult = true /* full result if provided in ev */);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////



///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TSyncRunner
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TSyncRunner {
public:
    struct TReturnValue {
        ui32 Id;
        ui32 Status;
        TReturnValue() = default;
        TReturnValue(ui32 id, ui32 status);
    };

    TSyncRunner(NActors::TActorSystem *system, TConfiguration *conf);
    NActors::TActorId NotifyID() const;
    // returns pair<Id, Status>
    TReturnValue Run(const NActors::TActorContext &ctx, TAutoPtr<NActors::IActor> actor);

private:
    NActors::TActorSystem *ActorSystem;
    NActors::TActorId WorkerID;
    std::shared_ptr<TSystemEvent> _Event;
    std::shared_ptr<TReturnValue> ReturnValue;
    TConfiguration *Conf;
};

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TSyncTestBase
// This class allows you to implement sequential (syncronous) test scenarios
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TSyncTestBase : public NActors::TActorBootstrapped<TSyncTestBase> {
protected:
    // Implement this virtual call to play the scenario you want
    virtual void Scenario(const NActors::TActorContext &ctx) = 0;

    TConfiguration *Conf;
    TAutoPtr<TSyncRunner> SyncRunner;

private:
    friend class NActors::TActorBootstrapped<TSyncTestBase>;
    void Bootstrap(const NActors::TActorContext &ctx);

public:
    TSyncTestBase(TConfiguration *conf);
};

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TSyncTestWithSmallCommonDataset
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TSyncTestWithSmallCommonDataset : public TSyncTestBase {
protected:
    TSmallCommonDataSet DataSet;
    TExpectedSet ExpectedSet;

public:
    TSyncTestWithSmallCommonDataset(TConfiguration *conf);
};


///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TSyncTestWithProvidedDataset
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TSyncTestWithProvidedDataset : public TSyncTestBase {
protected:
    const IDataSet *DataSet;

public:
    TSyncTestWithProvidedDataset(TConfiguration *conf, const IDataSet *dataSet)
        : TSyncTestBase(conf)
        , DataSet(dataSet)
    {}
};


#define SYNC_TEST_BEGIN(ClassName, BaseName)                                \
class ClassName##Actor : public BaseName {                                  \
protected:

#define SYNC_TEST_END(ClassName, BaseName)                                  \
public:                                                                     \
    ClassName##Actor(TConfiguration *conf)                                  \
        : BaseName(conf)                                                    \
    {}                                                                      \
};                                                                          \
void ClassName::operator ()(TConfiguration *conf) {                         \
    conf->ActorSystem1->Register(new ClassName##Actor(conf));               \
}


#define SYNC_TEST_WITH_DATASET_BEGIN(ClassName)                             \
class ClassName##Actor : public TSyncTestWithProvidedDataset {              \
protected:

#define SYNC_TEST_WITH_DATASET_END(ClassName)                               \
public:                                                                     \
    ClassName##Actor(TConfiguration *conf, const IDataSet *dataSet)         \
        : TSyncTestWithProvidedDataset(conf, dataSet)                       \
    {}                                                                      \
};                                                                          \
void ClassName::operator ()(TConfiguration *conf) {                         \
    conf->ActorSystem1->Register(new ClassName##Actor(conf, DataSet));      \
}


