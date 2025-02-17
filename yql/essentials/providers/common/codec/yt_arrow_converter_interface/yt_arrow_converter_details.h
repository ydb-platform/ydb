#pragma once

#include <yql/essentials/public/udf/arrow/block_reader.h>

#include <yql/essentials/utils/yql_panic.h>

#include <library/cpp/yson/detail.h>
#include <library/cpp/yson/varint.h>

namespace NYql {

using namespace NYson::NDetail;

class TYsonBuffer {
public:
    TYsonBuffer(const std::string_view& s) : Data_(s.data()), Available_(s.size()) {}

    constexpr char Next() {
        YQL_ENSURE(Available_-- > 0);
        return *(++Data_);
    }

    constexpr char Current() {
        return *Data_;
    }

    ui32 ReadVarUI32() {
        char prev = Current();
        if (Y_LIKELY(!(prev & 0x80))) {
            Next();
            return prev;
        }

        return ReadVarSlow<ui32>();
    }

    ui64 ReadVarUI64() {
        char prev = Current();
        if (Y_LIKELY(!(prev & 0x80))) {
            Next();
            return prev;
        }

        return ReadVarSlow<ui64>();
    }

    i32 ReadVarI32() {
        return NYson::ZigZagDecode32(ReadVarUI32());
    }

    i64 ReadVarI64() {
        return NYson::ZigZagDecode64(ReadVarUI64());
    }

    double NextDouble() {
        double val = *reinterpret_cast<const double*>(Data_);
        Data_ += sizeof(double);
        return val;
    }

    void Skip(i32 cnt) {
        Data_ += cnt;
    }

    const char* Data() {
        return Data_;
    }

    size_t Available() const {
        return Available_;
    }
private:
    template<typename T>
    constexpr T ReadVarSlow() {
        T shift = 0;
        T value = Current() & 0x7f;
        for (;;) {
            shift += 7;
            value |= T(Next() & 0x7f) << shift;
            if (!(Current() & 0x80)) {
                break;
            }
        }
        Next();
        return value;
    }

    const char* Data_;
    size_t Available_;
};


class IYsonComplexTypeReader {
public:
    using TPtr = std::unique_ptr<IYsonComplexTypeReader>;
    virtual ~IYsonComplexTypeReader() = default;
    virtual NUdf::TBlockItem GetItem(TYsonBuffer& buf) = 0;
};

}
