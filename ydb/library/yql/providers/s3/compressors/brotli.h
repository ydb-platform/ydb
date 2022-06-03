#pragma once

#include <ydb/library/yql/udfs/common/clickhouse/client/src/IO/ReadBuffer.h>
#include <contrib/libs/brotli/include/brotli/decode.h>

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

    void InitDecoder();
    void FreeDecoder();
};

}

}
