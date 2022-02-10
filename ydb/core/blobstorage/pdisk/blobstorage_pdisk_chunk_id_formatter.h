#pragma once
#include "defs.h"

#include <util/stream/str.h>

namespace NKikimr {
namespace NPDisk {

struct TChunkIdFormatter {
    TChunkIdFormatter(IOutputStream& str)
        : Str(str)
        , FirstOutput(true)
        , FirstChunkId(-1)
        , RunLen(0)
    {}

    template <class T>
    void PrintBracedChunksList(const T& chunkIds) {
        Str << "{";
        for (ui32 idx : chunkIds) {
            PrintChunk(idx);
        }
        Finish();
        Str << "}";
    }

    void PrintChunk(ui32 chunkId) {
        if (chunkId == FirstChunkId + RunLen) {
            ++RunLen;
        } else {
            OutputGroup();
            FirstChunkId = chunkId;
            RunLen = 1;
        }
    }

    void Finish() {
        OutputGroup();
    }

    void OutputGroup() {
        if (!RunLen) {
            return;
        }
        if (FirstOutput) {
            FirstOutput = false;
        } else {
            Str << ", ";
        }
        Str << FirstChunkId;
        if (RunLen > 2) {
            Str << ".." << FirstChunkId + RunLen - 1;
        } else if (RunLen == 2) {
            Str << ", " << FirstChunkId + 1;
        }
    }

    IOutputStream& Str;
    bool FirstOutput;
    ui32 FirstChunkId;
    ui32 RunLen;
};

} // NPDisk
} // NKikimr
