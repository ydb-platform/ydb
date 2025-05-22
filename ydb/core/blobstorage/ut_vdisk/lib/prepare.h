#pragma once

#include "defs.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/event.h>
#include <util/system/condvar.h>
#include <util/folder/tempdir.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_config.h>
#include <ydb/core/erasure/erasure.h>
#include <ydb/library/pdisk_io/aio.h>

namespace NActors {
    struct TActorSystemSetup;
} // NActors

//////////////////////////////////////////////////////////////////////////////////////
// TAllPDisksConfiguration
//////////////////////////////////////////////////////////////////////////////////////
struct TAllPDisksConfiguration {
    const ui32 PDisksNum;
    const ui32 ChunkSize;
    const ui64 DiskSize;
    const TString Dir;
    const TString Device;
    const TString DeviceType;

    static TAllPDisksConfiguration MkDevice(const TString &devicePath, ui32 chunkSize,
                                            TString deviceType);
    static TAllPDisksConfiguration MkOneTmp(ui32 chunkSize, ui64 diskSize,
                                            TString deviceType);
    static TAllPDisksConfiguration MkManyTmp(ui32 pDisksNum, ui32 chunkSize, ui64 diskSize,
                                            TString deviceType);

    TAllPDisksConfiguration(const TAllPDisksConfiguration &) = default;

private:
    TAllPDisksConfiguration(ui32 num, ui32 chunkSize, ui64 diskSize,
                            const TString &dir, const TString &device,
                            TString deviceType);
};

//////////////////////////////////////////////////////////////////////////////////////
// TOnePDisk
//////////////////////////////////////////////////////////////////////////////////////
struct TOnePDisk {
    NActors::TActorId PDiskActorID;
    const ui32 PDiskID = 0;
    ui64 PDiskGuid = 0;
    const TString Filename;
    const ui32 ChunkSize;
    const ui64 DiskSize;
    bool ReadOnly = false;

    TOnePDisk(ui32 pDiskId, ui64 pDiskGuid, const TString &filename,
              ui32 chunkSize, ui64 diskSize);
    void FormatDisk(bool force = false);
    void EraseDisk(ui64 newGuid);
};

//////////////////////////////////////////////////////////////////////////////////////
// TAllPDisks
//////////////////////////////////////////////////////////////////////////////////////
class TAllPDisks {
public:
    TAllPDisks(const TAllPDisksConfiguration &cfg);
    void ActorSetupCmd(NActors::TActorSystemSetup *setup, ui32 node,
                       const TIntrusivePtr<::NMonitoring::TDynamicCounters> &counters);
    // get instance by pDiskID (sequential number starting from 1)
    TOnePDisk &Get(ui32 pDiskID);
    void EraseDisk(ui32 pDiskId, ui64 newGuid);
    ui32 GetSize() const {
        return PDisks.size();
    }

private:
    TAllPDisksConfiguration Cfg;
    TVector<TOnePDisk> PDisks;
    std::shared_ptr<TTempDir> TempDir;
};

//////////////////////////////////////////////////////////////////////////////////////
// TAllVDisks
//////////////////////////////////////////////////////////////////////////////////////
struct IVDiskSetup;

class TAllVDisks {
public:
    struct TVDiskInstance {
        NActors::TActorId ActorID;
        NKikimr::TVDiskID VDiskID;
        bool Initialized = false;
        TIntrusivePtr<NKikimr::TVDiskConfig> Cfg;
    };

    TAllVDisks(TAllPDisks *pdisks, ui32 domainsNum, ui32 disksInDomain, IVDiskSetup *vdiskSetup, bool onePDisk,
            bool runRepl, ui64 *inOutInitOwnerRound);
    ~TAllVDisks();
    TVDiskInstance &Get(ui32 vdisk) {
        return VDisks.at(vdisk);
    }
    void ActorSetupCmd(NActors::TActorSystemSetup *setup, NKikimr::TBlobStorageGroupInfo *groupInfo,
                       const TIntrusivePtr<::NMonitoring::TDynamicCounters> &counters);
    ui32 GetSize() const {
        return VDisks.size();
    }

    TVector<NKikimr::TVDiskID> GetVDiskIds() const {
        TVector<NKikimr::TVDiskID> vdisks;
        for (const auto &x : VDisks) {
            vdisks.push_back(x.VDiskID);
        }
        return vdisks;
    }

private:
    ui32 DomainsNum;
    ui32 DisksInDomain;
    TVector<TVDiskInstance> VDisks;
};

struct IVDiskSetup {
public:
    using TConfigModifier = std::function<void(NKikimr::TVDiskConfig*)>;
    void AddConfigModifier(TConfigModifier m) {
        ConfigModifiers.push_back(std::move(m));
    }

    virtual bool SetUp(TAllVDisks::TVDiskInstance &vdisk, TAllPDisks *pdisks, ui32 id, ui32 d, ui32 j,
                       ui32 pDiskID, ui32 slotId, bool runRepl, ui64 initOwnerRound) = 0;
    virtual ~IVDiskSetup() = default;

protected:
    TVector<TConfigModifier> ConfigModifiers;
};

struct TDefaultVDiskSetup : public IVDiskSetup {
    TDefaultVDiskSetup();
    bool SetUp(TAllVDisks::TVDiskInstance &vdisk, TAllPDisks *pdisks, ui32 id, ui32 d, ui32 j, ui32
               pDiskID, ui32 slotId, bool runRepl, ui64 initOwnerRound) override;
};



//////////////////////////////////////////////////////////////////////////////////////
// TConfiguration
//////////////////////////////////////////////////////////////////////////////////////
struct TConfiguration {
    TAtomic SuccessCount = 0;

    const ui32 DomainsNum;
    const ui32 DisksInDomain;
    const NKikimr::TErasureType::EErasureSpecies Erasure;
    TAllPDisksConfiguration PCfg;

    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    std::unique_ptr<NActors::TMon> Monitoring;
    std::unique_ptr<NKikimr::TAppData> AppData;
    std::shared_ptr<NKikimr::NPDisk::IIoContextFactory> IoContext;
    std::unique_ptr<NActors::TActorSystem> ActorSystem1;
    std::unique_ptr<TAllPDisks> PDisks;
    std::unique_ptr<TAllVDisks> VDisks;
    TIntrusivePtr<NKikimr::TBlobStorageGroupInfo> GroupInfo;

    TAtomic DoneCounter = 0;
    TSystemEvent DoneEvent { TSystemEvent::rAuto };
    TSystemEvent DbInitEvent { TSystemEvent::rAuto };

    using TTimeoutCallbackList = TList<std::function<void ()>>;
    using TTimeoutCallbackId = TTimeoutCallbackList::iterator;
    TTimeoutCallbackList TimeoutCallbacks;
    TMutex TimeoutCallbacksLock;
    TCondVar TimeoutCallbacksCV;

    TConfiguration(const TAllPDisksConfiguration &pcfg =
                      TAllPDisksConfiguration::MkOneTmp(512u << 10u, 16ull << 30ull, "ROT"),
                   ui32 domainsNum = 4u,
                   ui32 disksInDomain = 2u,
                   NKikimr::TErasureType::EErasureSpecies erasure =
                       NKikimr::TBlobStorageGroupType::ErasureMirror3);
    ~TConfiguration();

    void SignalDoneEvent() {
        AtomicIncrement(DoneCounter);
        DoneEvent.Signal();
    }

    template <class TTest>
    bool Run(TTest *test, const TDuration &testTimeout, TAtomicBase instances = 1, bool wait = true) {
        SuccessCount = 0;
        DoneCounter = 0;

        // wait for db to initialize
        if (wait)
            DbInitWait();

        (*test)(this);

        TAtomicBase doneCount = 0;
        bool isOk = true;
        TInstant startTime = Now();
        while (doneCount < instances && isOk) {
            TDuration passed = Now() - startTime;
            ui32 msRemaining = passed >= testTimeout ? 0 : (testTimeout - passed).MilliSeconds();
            isOk = DoneEvent.Wait(msRemaining);
            doneCount = AtomicGet(DoneCounter);
        }

        for (;;) {
            TGuard<TMutex> lock(TimeoutCallbacksLock);
            if (TimeoutCallbacks.empty()) {
                break;
            }
            for (std::function<void ()>& callback : TimeoutCallbacks) {
                if (callback) {
                    callback();
                    callback = {};
                }
            }
            TimeoutCallbacksCV.WaitI(TimeoutCallbacksLock);
        }

        UNIT_ASSERT_VALUES_EQUAL(doneCount, instances);
        UNIT_ASSERT_VALUES_EQUAL(SuccessCount, instances);

        return doneCount == instances && SuccessCount == instances;
    }

    TTimeoutCallbackId RegisterTimeoutCallback(std::function<void ()> callback) {
        TGuard<TMutex> lock(TimeoutCallbacksLock);
        auto res = TimeoutCallbacks.insert(TimeoutCallbacks.end(), std::move(callback));
        TimeoutCallbacksCV.Signal();
        return res;
    }

    void UnregisterTimeoutCallback(TTimeoutCallbackId handle) {
        TGuard<TMutex> lock(TimeoutCallbacksLock);
        TimeoutCallbacks.erase(handle);
        TimeoutCallbacksCV.Signal();
    }

    void Prepare(IVDiskSetup *vdiskSetup, bool newPDisks = true, bool runRepl = true);
    void Shutdown();
    void DbInitWait();
    void PoisonVDisks();
    void PoisonPDisks();
};

