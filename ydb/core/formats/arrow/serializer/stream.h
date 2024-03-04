#pragma once

#include <util/generic/string.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/io/interfaces.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/status.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/result.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/buffer.h>

namespace NKikimr::NArrow::NSerialization {

// Arrow internally keeps references to Buffer objects with the data
// This helper class implements arrow::Buffer over TString that owns
// the actual memory
// Its use for no-compression mode, where RecordBatch dont own memory
class TBufferOverString: public arrow::Buffer {
    TString Str;
public:
    explicit TBufferOverString(TString str)
        : arrow::Buffer((const unsigned char*)str.data(), str.size())
        , Str(str) {
        Y_ABORT_UNLESS(data() == (const unsigned char*)Str.data());
    }
};

class TFixedStringOutputStream final: public arrow::io::OutputStream {
public:
    TFixedStringOutputStream(TString* out)
        : Out(out)
        , Position(0) {
    }

    arrow::Status Close() override;

    bool closed() const override {
        return Out == nullptr;
    }

    arrow::Result<int64_t> Tell() const override {
        return Position;
    }

    arrow::Status Write(const void* data, int64_t nbytes) override;

    size_t GetPosition() const {
        return Position;
    }

private:
    TString* Out;
    size_t Position;
};

class TStringOutputStream final: public arrow::io::OutputStream {
public:
    TStringOutputStream(TString* out)
        : Out(out)
        , Position(0) {
    }

    arrow::Status Close() override {
        Out = nullptr;
        return arrow::Status::OK();
    }

    bool closed() const override {
        return Out == nullptr;
    }

    arrow::Result<int64_t> Tell() const override {
        return Position;
    }

    arrow::Status Write(const void* data, int64_t nbytes) override {
        if (Y_LIKELY(nbytes > 0)) {
            Out->append((const char*)data, nbytes);
            Position += nbytes;
        }

        return arrow::Status::OK();
    }

    size_t GetPosition() const {
        return Position;
    }

private:
    TString* Out;
    size_t Position;
};

}
