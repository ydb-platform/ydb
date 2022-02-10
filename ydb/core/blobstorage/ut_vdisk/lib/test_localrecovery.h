#pragma once

#include "defs.h"
#include "helpers.h"
#include "prepare.h"

#include <util/generic/set.h>

///////////////////////////////////////////////////////////////////////////
struct TCheckDbIsEmptyManyPutGet {
    const bool ExpectEmpty;
    const bool WaitForCompaction;
    const ui32 MsgNum;
    const ui32 MsgSize;
    const NKikimrBlobStorage::EPutHandleClass HandleClass;

    TCheckDbIsEmptyManyPutGet(bool expectEmpty, bool waitForCompaction, ui32 msgNum, ui32 msgSize,
                              NKikimrBlobStorage::EPutHandleClass cls)
        : ExpectEmpty(expectEmpty)
        , WaitForCompaction(waitForCompaction)
        , MsgNum(msgNum)
        , MsgSize(msgSize)
        , HandleClass(cls)
    {}

    void operator ()(TConfiguration *conf);
};


///////////////////////////////////////////////////////////////////////////
struct TManyPutsTest {
    const bool WaitForCompaction;
    const ui32 MsgNum;
    const ui32 MsgSize;
    const NKikimrBlobStorage::EPutHandleClass HandleClass;
    std::shared_ptr<TSet<ui32>> BadSteps;

    TManyPutsTest(bool waitForCompaction, ui32 msgNum, ui32 msgSize,
                  NKikimrBlobStorage::EPutHandleClass cls,
                  std::shared_ptr<TSet<ui32>> badSteps)
        : WaitForCompaction(waitForCompaction)
        , MsgNum(msgNum)
        , MsgSize(msgSize)
        , HandleClass(cls)
        , BadSteps(badSteps)
    {}

    void operator ()(TConfiguration *conf);
};


///////////////////////////////////////////////////////////////////////////
struct TManyMultiPutsTest {
    const bool WaitForCompaction;
    const ui32 MsgNum;
    const ui32 BatchSize;
    const ui32 MsgSize;
    const NKikimrBlobStorage::EPutHandleClass HandleClass;
    std::shared_ptr<TSet<ui32>> BadSteps;

    TManyMultiPutsTest(bool waitForCompaction, ui32 msgNum, ui32 batchSize, ui32 msgSize,
                  NKikimrBlobStorage::EPutHandleClass cls,
                  std::shared_ptr<TSet<ui32>> badSteps)
        : WaitForCompaction(waitForCompaction)
        , MsgNum(msgNum)
        , BatchSize(batchSize)
        , MsgSize(msgSize)
        , HandleClass(cls)
        , BadSteps(badSteps)
    {}

    void operator ()(TConfiguration *conf);
};


///////////////////////////////////////////////////////////////////////////
struct TManyGetsTest {
    const bool WaitForCompaction;
    const ui32 MsgNum;
    const ui32 MsgSize;
    const NKikimrBlobStorage::EPutHandleClass HandleClass;
    std::shared_ptr<TSet<ui32>> BadSteps;

    TManyGetsTest(bool waitForCompaction, ui32 msgNum, ui32 msgSize,
                  NKikimrBlobStorage::EPutHandleClass cls,
                  std::shared_ptr<TSet<ui32>> badSteps)
        : WaitForCompaction(waitForCompaction)
        , MsgNum(msgNum)
        , MsgSize(msgSize)
        , HandleClass(cls)
        , BadSteps(badSteps)
    {}

    void operator ()(TConfiguration *conf);
};

///////////////////////////////////////////////////////////////////////////
struct TChaoticManyPutsTest {
    const ui32 Parallel;
    const ui32 MsgNum;
    const ui32 MsgSize;
    std::shared_ptr<IPutHandleClassGenerator> HandleClassGen;
    const TDuration WorkingTime;
    const TDuration RequestTimeout; // zero means infinity

    TChaoticManyPutsTest(ui32 parallel, ui32 msgNum, ui32 msgSize,
                         std::shared_ptr<IPutHandleClassGenerator> cls,
                         TDuration workingTime, TDuration requestTimeout)
        : Parallel(parallel)
        , MsgNum(msgNum)
        , MsgSize(msgSize)
        , HandleClassGen(cls)
        , WorkingTime(workingTime)
        , RequestTimeout(requestTimeout)
    {}

    void operator ()(TConfiguration *conf);
};

