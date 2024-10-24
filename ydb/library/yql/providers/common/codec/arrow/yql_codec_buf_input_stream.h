#include <ydb/library/yql/providers/common/codec/yql_codec_buf.h>

#include <arrow/io/interfaces.h>
#include <arrow/result.h>

namespace NYql {
namespace NCommon {

class TInputBufArrowInputStream : public arrow::io::InputStream {
public:
    explicit TInputBufArrowInputStream(TInputBuf& buffer, arrow::MemoryPool* pool)
        : Buffer_(buffer)
        , Pool_(pool)
    {
    }

    arrow::Result<int64_t> Read(int64_t bytesToRead, void* outBuffer) override;
    arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override;

    arrow::Status Close() override {
        return arrow::Status::OK();
    }

    arrow::Result<int64_t> Tell() const override {
        return BytesRead_;
    }

    bool closed() const override {
        return false;
    }

    bool EOSReached() const {
        return EOSReached_;
    }
    
private:
    TInputBuf& Buffer_;
    int64_t BytesRead_ = 0;
    bool EOSReached_ = false;

    arrow::MemoryPool* Pool_;
};

} // NCommon
} // NYql
