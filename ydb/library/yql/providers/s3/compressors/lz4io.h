#pragma once

#include <ydb/library/yql/udfs/common/clickhouse/client/src/IO/ReadBuffer.h>
#define LZ4F_STATIC_LINKING_ONLY
#include <contrib/libs/lz4/lz4frame.h>

#include "output_queue.h"

namespace NYql {

namespace NLz4 {

enum class EStreamType {
    Unknown = 0,
    Frame,
    Legacy
};

class TReadBuffer : public NDB::ReadBuffer {
public:
    TReadBuffer(NDB::ReadBuffer& source);
    ~TReadBuffer();
private:
    bool nextImpl() final;

    size_t DecompressFrame();
    size_t DecompressLegacy();

    NDB::ReadBuffer& Source;
    const EStreamType StreamType;
    std::vector<char> InBuffer, OutBuffer;

    LZ4F_decompressionContext_t Ctx;
    LZ4F_errorCode_t NextToLoad;
    size_t Pos, Remaining;
};

IOutputQueue::TPtr MakeCompressor(std::optional<int> cLevel = {});

}

}
