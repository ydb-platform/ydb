#pragma once

#include <yql/essentials/providers/common/codec/yql_codec_buf.h>

#include <arrow/io/interfaces.h>
#include <arrow/result.h>

namespace NYql::NCommon {

class TOutputBufArrowOutputStream: public arrow20::io::OutputStream {
public:
    explicit TOutputBufArrowOutputStream(TOutputBuf& buffer)
        : Buffer_(buffer)
    {
        set_mode(arrow20::io::FileMode::type::WRITE);
    }

    arrow20::Status Write(const void* data, int64_t nbytes) override;
    arrow20::Status Flush() override;

    arrow20::Status Close() override {
        return arrow20::Status::OK();
    }

    arrow20::Result<int64_t> Tell() const override {
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
