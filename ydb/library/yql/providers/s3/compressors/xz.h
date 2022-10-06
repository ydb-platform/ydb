#pragma once

#include <ydb/library/yql/udfs/common/clickhouse/client/src/IO/ReadBuffer.h>
#include <contrib/libs/lzma/liblzma/api/lzma.h>
#include "output_queue.h"

namespace NYql {

namespace NXz {

class TReadBuffer : public NDB::ReadBuffer {
public:
    TReadBuffer(NDB::ReadBuffer& source);
    ~TReadBuffer();
private:
    bool nextImpl() final;

    NDB::ReadBuffer& Source_;
    std::vector<char> InBuffer, OutBuffer;

    lzma_stream Strm_;

    bool IsInFinished_ = false;
    bool IsOutFinished_ = false;
};

IOutputQueue::TPtr MakeCompressor(std::optional<int> cLevel = {});

}

}

