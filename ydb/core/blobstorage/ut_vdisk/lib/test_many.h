#pragma once

#include "defs.h"
#include "prepare.h"
#include "helpers.h"

///////////////////////////////////////////////////////////////////////////
struct TManyPutOneGet {
    const bool WaitForCompaction;
    std::shared_ptr<TVector<TMsgPackInfo>> MsgPacks;
    const NKikimrBlobStorage::EPutHandleClass HandleClass;
    const ui64 TabletId;
    const ui64 Shift;
    const bool WithErrorResponse;

    TManyPutOneGet(bool waitForCompaction, ui32 msgNum, ui32 msgSize, NKikimrBlobStorage::EPutHandleClass cls,
                   ui64 tabletId = DefaultTestTabletId, ui64 shift = 0, bool withErrorResponse = false)
        : WaitForCompaction(waitForCompaction)
        , MsgPacks(new TVector<TMsgPackInfo>{TMsgPackInfo(msgSize, msgNum)})
        , HandleClass(cls)
        , TabletId(tabletId)
        , Shift(shift)
        , WithErrorResponse(withErrorResponse)
    {}

    TManyPutOneGet(bool waitForCompaction, std::shared_ptr<TVector<TMsgPackInfo>> msgPacks,
                   NKikimrBlobStorage::EPutHandleClass cls, ui64 tabletId = DefaultTestTabletId,
                   ui64 shift = 0, bool withErrorResponse = false)
        : WaitForCompaction(waitForCompaction)
        , MsgPacks(msgPacks)
        , HandleClass(cls)
        , TabletId(tabletId)
        , Shift(shift)
        , WithErrorResponse(withErrorResponse)
    {}

    void operator ()(TConfiguration *conf);
};

///////////////////////////////////////////////////////////////////////////
struct TManyPutGet {
    const bool WaitForCompaction;
    const ui32 MsgNum;
    const ui32 MsgSize;
    const NKikimrBlobStorage::EPutHandleClass HandleClass;
    const ui64 TabletId;

    TManyPutGet(bool waitForCompaction, ui32 msgNum, ui32 msgSize, NKikimrBlobStorage::EPutHandleClass cls,
            ui64 tabletId = DefaultTestTabletId)
        : WaitForCompaction(waitForCompaction)
        , MsgNum(msgNum)
        , MsgSize(msgSize)
        , HandleClass(cls)
        , TabletId(tabletId)
    {}

    void operator ()(TConfiguration *conf);
};

///////////////////////////////////////////////////////////////////////////
struct TManyMultiPutGet {
    const bool WaitForCompaction;
    const ui32 MsgNum;
    const ui32 MsgSize;
    const ui32 BatchSize;
    const NKikimrBlobStorage::EPutHandleClass HandleClass;
    const ui64 TabletId;

    TManyMultiPutGet(bool waitForCompaction, ui32 msgNum, ui32 msgSize, ui32 batchSize,
            NKikimrBlobStorage::EPutHandleClass cls, ui64 tabletId = DefaultTestTabletId)
        : WaitForCompaction(waitForCompaction)
        , MsgNum(msgNum)
        , MsgSize(msgSize)
        , BatchSize(batchSize)
        , HandleClass(cls)
        , TabletId(tabletId)
    {}

    void operator ()(TConfiguration *conf);
};

///////////////////////////////////////////////////////////////////////////
struct TManyPutRangeGet {
    const bool WaitForCompaction;
    const bool IndexOnly;
    const ui32 MsgNum;
    const ui32 MsgSize;
    const NKikimrBlobStorage::EPutHandleClass HandleClass;

    TManyPutRangeGet(bool waitForCompaction, bool indexOnly, ui32 msgNum, ui32 msgSize,
                     NKikimrBlobStorage::EPutHandleClass cls)
        : WaitForCompaction(waitForCompaction)
        , IndexOnly(indexOnly)
        , MsgNum(msgNum)
        , MsgSize(msgSize)
        , HandleClass(cls)
    {
        Y_ABORT_UNLESS(indexOnly);
    }

    void operator ()(TConfiguration *conf);
};

///////////////////////////////////////////////////////////////////////////
struct TManyPutRangeGet2Channels {
    const bool WaitForCompaction;
    const bool IndexOnly;
    const ui32 MsgNum;
    const ui32 MsgSize;
    const NKikimrBlobStorage::EPutHandleClass HandleClass;

    TManyPutRangeGet2Channels(bool waitForCompaction, bool indexOnly, ui32 msgNum, ui32 msgSize,
                              NKikimrBlobStorage::EPutHandleClass cls)
        : WaitForCompaction(waitForCompaction)
        , IndexOnly(indexOnly)
        , MsgNum(msgNum)
        , MsgSize(msgSize)
        , HandleClass(cls)
    {
        Y_ABORT_UNLESS(indexOnly);
    }

    void operator ()(TConfiguration *conf);
};
