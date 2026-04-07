#include "yql_codec_buf_output_stream.h"

#include <yql/essentials/public/udf/arrow/defs.h>

#include <arrow/buffer.h>
#include <arrow/buffer.h>

namespace NYql::NCommon {

arrow::Status TOutputBufArrowOutputStream::Write(const void* data, int64_t nbytes) {
    Buffer_.WriteMany(static_cast<const char*>(data), nbytes);
    BytesWritten_ += nbytes;
    return arrow::Status::OK();
}

arrow::Status TOutputBufArrowOutputStream::Flush() {
    Buffer_.Flush();
    return arrow::Status::OK();
}

} // namespace NYql::NCommon
