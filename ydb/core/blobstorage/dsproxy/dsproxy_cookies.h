#pragma once

#include "dsproxy.h"

namespace NKikimr {

struct TVGetCookie {
    ui64 Raw;
    // [0,  27] 28bit queryBeginIdx
    // [28, 55] 28bit queryEndIdx
    // [56, 63] 8bit causeIdx

    TVGetCookie(ui64 raw)
        : Raw(raw)
    {}

    TVGetCookie(ui64 queryBeginIdx, ui64 queryEndIdx)
        : Raw(queryBeginIdx | (queryEndIdx << 28))
    {
        Y_ABORT_UNLESS(queryBeginIdx < (1LL << 28));
        Y_ABORT_UNLESS(queryEndIdx < (1LL << 28));
    }

    ui64 GetQueryBeginIdx() const {
        return Raw & 0xFFFFFFF;
    }

    ui8 GetQueryEndIdx() const {
        return (Raw >> 28) & 0xFFFFFFF;
    }

    ui64 GetCauseIdx() const {
        return (Raw >> 56) & 0xFF;
    }

    void SetCauseIdx(ui64 causeIdx) {
        // It's a debug feature support, it's a bad idea to VERIFY here
        if (causeIdx > 255) {
            causeIdx = 255;
        }
        ui64 prevCauseIdx = GetCauseIdx();
        Raw = Raw ^ ((causeIdx ^ prevCauseIdx) << 56);
    }

    operator ui64 () const {
        return Raw;
    }
};

struct TBlobCookie {
    ui64 Raw;
    // [0,  7 ] 8bit  vDiskOrderNumber
    // [8,  15] 8bit  partId .
    // [16, 31] 16bit blobIdx
    // [32, 55] 24bit requestIdx
    // [56, 63] 8bit causeIdx

    TBlobCookie(ui64 raw)
        : Raw(raw)
    {}

    TBlobCookie(ui64 vDiskOrderNumber, ui64 blobIdx, ui64 partId, ui64 requestIdx)
        : Raw(vDiskOrderNumber | (partId << 8) | (blobIdx << 16) | (requestIdx << 32))
    {
        Y_ABORT_UNLESS(vDiskOrderNumber < 256);
        Y_ABORT_UNLESS(blobIdx < (1LL << 16));
        Y_ABORT_UNLESS(partId < 256);
        Y_ABORT_UNLESS(requestIdx < (1LL << 24));
    }

    ui64 GetVDiskOrderNumber() const {
        return Raw & 0xFF;
    }

    ui8 GetPartId() const {
        return (Raw >> 8) & 0xFF;
    }

    ui64 GetBlobIdx() const {
        return (Raw >> 16) & 0xFFFF;
    }

    ui64 GetCauseIdx() const {
        return (Raw >> 56) & 0xFF;
    }

    void SetCauseIdx(ui64 causeIdx) {
        // It's a debug feature support, it's a bad idea to VERIFY here
        if (causeIdx > 255) {
            causeIdx = 255;
        }
        ui64 prevCauseIdx = GetCauseIdx();
        Raw = Raw ^ ((causeIdx ^ prevCauseIdx) << 56);
    }

    ui64 GetRequestIdx() const {
        return (Raw >> 32) & 0xFFFFFF;
    }

    operator ui64 () const {
        return Raw;
    }
};

struct TVMultiPutCookie {
    ui64 Raw;
    // [0,  7 ] 8bit  vDiskOrderNumber
    // [8,  15] 8bit  itemCount
    // [16, 31] 16bit ---
    // [32, 55] 24bit requestIdx
    // [56, 63] 8bit causeIdx

    TVMultiPutCookie(ui64 raw)
        : Raw(raw)
    {}

    TVMultiPutCookie(ui64 vDiskOrderNumber, ui64 itemCount, ui64 requestIdx)
        : Raw(vDiskOrderNumber | (itemCount << 8) | (requestIdx << 32))
    {
        Y_ABORT_UNLESS(vDiskOrderNumber < 256);
        Y_ABORT_UNLESS(itemCount < 256);
        Y_ABORT_UNLESS(requestIdx < (1LL << 24));
    }

    ui64 GetVDiskOrderNumber() const {
        return Raw & 0xFF;
    }

    ui64 GetItemCount() const {
        return (Raw >> 8) & 0xFF;
    }

    ui64 GetCauseIdx() const {
        return (Raw >> 56) & 0xFF;
    }

    void SetCauseIdx(ui64 causeIdx) {
        // It's a debug feature support, it's a bad idea to VERIFY here
        if (causeIdx > 255) {
            causeIdx = 255;
        }
        ui64 prevCauseIdx = GetCauseIdx();
        Raw = Raw ^ ((causeIdx ^ prevCauseIdx) << 56);
    }

    ui64 GetRequestIdx() const {
        return (Raw >> 32) & 0xFFFFFF;
    }

    operator ui64 () const {
        return Raw;
    }
};

}//NKikimr
