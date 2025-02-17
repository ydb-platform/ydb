#pragma once

#include <ydb/library/yql/udfs/common/clickhouse/client/src/IO/ReadBuffer.h>
#define ZSTD_STATIC_LINKING_ONLY
#include <contrib/libs/zstd/include/zstd.h>
#include "output_queue.h"

namespace NYql {

namespace NZstd {

class TReadBuffer : public NDB::ReadBuffer {
public:
    TReadBuffer(NDB::ReadBuffer& source);
    ~TReadBuffer();
private:
    bool nextImpl() final;

    NDB::ReadBuffer& Source_;
    std::vector<char> InBuffer, OutBuffer;
    ::ZSTD_DStream *const ZCtx_;
    size_t Offset_;
    size_t Size_;
    bool Finished_ = false;
};

IOutputQueue::TPtr MakeCompressor(std::optional<int> cLevel = {});

}

}
