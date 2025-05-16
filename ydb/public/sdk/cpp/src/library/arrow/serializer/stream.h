#pragma once

#include <contrib/libs/apache/arrow/cpp/src/arrow/io/interfaces.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/result.h>

namespace NYdb {

inline namespace Dev {

namespace NArrow {

namespace NSerialization {

class TFixedStringOutputStream final: public arrow::io::OutputStream {
public:
    TFixedStringOutputStream(std::string* out)
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
    std::string* Out;
    size_t Position;
};

}
}
}
}
