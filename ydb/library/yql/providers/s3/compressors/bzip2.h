#pragma once

#include <ydb/library/yql/udfs/common/clickhouse/client/src/IO/ReadBuffer.h>
#include <contrib/libs/libbz2/bzlib.h>
#include "output_queue.h"

namespace NYql {

namespace NBzip2 {

class TReadBuffer : public NDB::ReadBuffer {
public:
    TReadBuffer(NDB::ReadBuffer& source);
    ~TReadBuffer();
private:
    bool nextImpl() final;

    NDB::ReadBuffer& Source_;
    std::vector<char> InBuffer, OutBuffer;

    bz_stream BzStream_;

    void InitDecoder();
    void FreeDecoder();
};

IOutputQueue::TPtr MakeCompressor(std::optional<int> blockSize100k = {});

}

}

