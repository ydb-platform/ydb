#pragma once

#include <ydb/library/yql/udfs/common/clickhouse/client/src/IO/ReadBuffer.h>
#include <contrib/libs/brotli/include/brotli/decode.h>
#include "output_queue.h"

namespace NYql {

namespace NBrotli {

class TReadBuffer : public NDB::ReadBuffer {
public:
    TReadBuffer(NDB::ReadBuffer& source);
    ~TReadBuffer();
private:
    bool nextImpl() final;

    NDB::ReadBuffer& Source_;
    std::vector<char> InBuffer, OutBuffer;

    BrotliDecoderState* DecoderState_;

    bool SubstreamFinished_ = false;
    bool InputExhausted_ = false;
    size_t InputAvailable_ = 0;
    size_t InputSize_ = 0;

    void InitDecoder();
    void FreeDecoder();
};

IOutputQueue::TPtr MakeCompressor(std::optional<int> cLevel = {});

}

}
