#pragma once

#include <ydb/core/blobstorage/ut_vdisk/lib/prepare.h>
#include <ydb/core/blobstorage/ut_vdisk/lib/setup.h>
#include <ydb/core/blobstorage/ut_vdisk/lib/test_localrecovery.h>

struct TWriteRestartReadSettings {
    using EClass = NKikimrBlobStorage::EPutHandleClass;

    // number of messages
    const ui32 MsgNum;
    // each message size
    const ui32 MsgSize;
    const EClass Cls;
    // VDisk setup for write phase
    const std::shared_ptr<IVDiskSetup> WriteRunSetup;
    // VDisk setup for read phase
    const std::shared_ptr<IVDiskSetup> ReadRunSetup;

    TWriteRestartReadSettings(
            ui32 msgNum,
            ui32 msgSize,
            EClass cls,
            std::shared_ptr<IVDiskSetup> writeRunSetup,
            std::shared_ptr<IVDiskSetup> readRunSetup)
        : MsgNum(msgNum)
        , MsgSize(msgSize)
        , Cls(cls)
        , WriteRunSetup(writeRunSetup)
        , ReadRunSetup(readRunSetup)
    {}

    static TWriteRestartReadSettings OneSetup(
            ui32 msgNum,
            ui32 msgSize,
            EClass cls,
            std::shared_ptr<IVDiskSetup> setup) {
        return TWriteRestartReadSettings(msgNum, msgSize, cls, setup, setup);
    }
};

struct TMultiPutWriteRestartReadSettings {
    using EClass = NKikimrBlobStorage::EPutHandleClass;

    // number of messages
    const ui32 MsgNum;
    const ui32 BatchSize;
    // each message size
    const ui32 MsgSize;
    const EClass Cls;
    // VDisk setup for write phase
    const std::shared_ptr<IVDiskSetup> WriteRunSetup;
    // VDisk setup for read phase
    const std::shared_ptr<IVDiskSetup> ReadRunSetup;

    TMultiPutWriteRestartReadSettings(
            ui32 msgNum,
            ui32 batchSize,
            ui32 msgSize,
            EClass cls,
            std::shared_ptr<IVDiskSetup> writeRunSetup,
            std::shared_ptr<IVDiskSetup> readRunSetup)
        : MsgNum(msgNum)
        , BatchSize(batchSize)
        , MsgSize(msgSize)
        , Cls(cls)
        , WriteRunSetup(writeRunSetup)
        , ReadRunSetup(readRunSetup)
    {}

    static TMultiPutWriteRestartReadSettings OneSetup(
            ui32 msgNum,
            ui32 batchSize,
            ui32 msgSize,
            EClass cls,
            std::shared_ptr<IVDiskSetup> setup) {
        return TMultiPutWriteRestartReadSettings(msgNum, batchSize, msgSize, cls, setup, setup);
    }
};

struct TChaoticWriteRestartWriteSettings : public TWriteRestartReadSettings {
    // number of parallel writes/reads (ignored if just single read/write)]
    const std::shared_ptr<IVDiskSetup> SecondWriteRunSetup;
    const ui32 Parallel;
    const TDuration WorkingTime;
    const TDuration RequestTimeout;

    TChaoticWriteRestartWriteSettings(
            const TWriteRestartReadSettings &baseSettings,
            std::shared_ptr<IVDiskSetup> secondWriteRunSetup,
            ui32 parallel,
            TDuration workingTime,
            TDuration requestTimeout)
        : TWriteRestartReadSettings(baseSettings)
        , SecondWriteRunSetup(secondWriteRunSetup)
        , Parallel(parallel)
        , WorkingTime(workingTime)
        , RequestTimeout(requestTimeout)
    {}

    TChaoticWriteRestartWriteSettings(
            const TWriteRestartReadSettings &baseSettings,
            ui32 parallel,
            TDuration workingTime,
            TDuration requestTimeout)
        : TWriteRestartReadSettings(baseSettings)
        , SecondWriteRunSetup(baseSettings.WriteRunSetup)
        , Parallel(parallel)
        , WorkingTime(workingTime)
        , RequestTimeout(requestTimeout)
    {}

};


void WriteRestartRead(const TWriteRestartReadSettings &settings, TDuration testTimeout);
void MultiPutWriteRestartRead(const TMultiPutWriteRestartReadSettings &settings, TDuration testTimeout);
void ChaoticWriteRestartWrite(const TChaoticWriteRestartWriteSettings &settings, TDuration testTimeout);
