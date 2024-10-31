#include "yql_codec_buf_input_stream.h"

#include <ydb/library/yql/public/udf/arrow/defs.h>

#include <arrow/buffer.h>
#include <arrow/buffer.h>

namespace NYql {
namespace NCommon {

arrow::Result<int64_t> TInputBufArrowInputStream::Read(int64_t bytesToRead, void* outBuffer) {
    auto outBufferPtr = static_cast<char*>(outBuffer);

    YQL_ENSURE(bytesToRead > 0);
    if (!Buffer_.TryRead(*outBufferPtr)) {
        EOSReached_ = true;
        return 0;
    }

    Buffer_.ReadMany(outBufferPtr + 1, bytesToRead - 1);
    BytesRead_ += bytesToRead;
    return bytesToRead;
}

arrow::Result<std::shared_ptr<arrow::Buffer>> TInputBufArrowInputStream::Read(int64_t nbytes) {
    auto outBuffer = ARROW_RESULT(AllocateResizableBuffer(nbytes, Pool_));
    auto bytesRead = ARROW_RESULT(Read(nbytes, outBuffer->mutable_data()));
    if (bytesRead == 0) {
        return std::make_shared<arrow::Buffer>(nullptr, 0);
    }

    YQL_ENSURE(bytesRead == nbytes);
    return outBuffer;
}

} // NCommon
} // NYql
