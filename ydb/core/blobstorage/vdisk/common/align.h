#pragma once

#include "defs.h"

#include <ydb/core/blobstorage/base/transparent.h>

namespace NKikimr {

    /////////////////////////////////////////////////////////////////////////////////////////
    // Alignments
    /////////////////////////////////////////////////////////////////////////////////////////
    static inline ui32 AlignUpAppendBlockSize(ui32 val, ui32 appendBlockSize) {
        val += appendBlockSize - 1;
        return val - val % appendBlockSize;
    }

    template <class T>
    static inline void AlignUpAppendBlockSize(T &buf, ui32 appendBlockSize) {
        ui32 bufSize = buf.Size();
        ui32 expected = AlignUpAppendBlockSize(bufSize, appendBlockSize);
        Y_ABORT_UNLESS(expected >= bufSize);
        ui32 padding = expected - bufSize;

        struct TZeroPadder {
            static void Append(TBuffer &buf, const char *zero, size_t len) {
                buf.Append(zero, len);
            }

            static void Append(TTrackableBuffer &buf, const char *zero, size_t len) {
                buf.Append(zero, len);
            }

            static void Append(TString &buf, const char *zero, size_t len) {
                buf.append(zero, len);
            }

            static void Append(TTrackableString &buf, const char *zero, size_t len) {
                buf.append(zero, len);
            }
        };

        static const char zero[256] = {0};
        while (size_t len = Min<size_t>(sizeof(zero), padding)) {
            TZeroPadder::Append(buf, zero, len);
            padding -= len;
        }
    }

} // NKikimr
