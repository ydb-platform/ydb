#pragma once

#include <yql/essentials/providers/common/codec/yql_codec_buf.h>

#include <arrow/io/interfaces.h>
#include <arrow/result.h>

namespace NYql::NCommon {

class TOutputBufArrowOutputStream: public arrow::io::OutputStream {
public:
    explicit TOutputBufArrowOutputStream(TOutputBuf& buffer)
        : Buffer_(buffer)
    {
        set_mode(arrow::io::FileMode::type::WRITE);
    }

    arrow::Status Write(const void* data, int64_t nbytes) override;
    arrow::Status Flush() override;

    arrow::Status Close() override {
        return arrow::Status::OK();
    }

    arrow::Result<int64_t> Tell() const override {
        return BytesWritten_;
    }

    bool closed() const override {
        return false;
    }

private:
    TOutputBuf& Buffer_;
    int64_t BytesWritten_ = 0;
};

} // namespace NYql::NCommon
