#pragma once

#include <ydb/library/yql/minikql/defs.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/pack_num.h>
#include <ydb/library/yql/public/decimal/yql_decimal_serialize.h>
#include <ydb/library/yql/public/udf/udf_value.h>

#include <library/cpp/packedtypes/zigzag.h>
#include <ydb/library/actors/util/rope.h>

#include <util/generic/buffer.h>
#include <util/generic/strbuf.h>

namespace NKikimr {
namespace NMiniKQL {

namespace NDetails {

template<typename TBuf>
inline void PackUInt64(ui64 val, TBuf& buf) {
    buf.Advance(MAX_PACKED64_SIZE);
    char* dst = buf.Pos() - MAX_PACKED64_SIZE;
    buf.EraseBack(MAX_PACKED64_SIZE - Pack64(val, dst));
}

template<typename TBuf>
inline void PackInt64(i64 val, TBuf& buf) {
    PackUInt64(ZigZagEncode(val), buf);
}

template<typename TBuf>
inline void PackUInt32(ui32 val, TBuf& buf) {
    buf.Advance(MAX_PACKED32_SIZE);
    char* dst = buf.Pos() - MAX_PACKED32_SIZE;
    buf.EraseBack(MAX_PACKED32_SIZE - Pack32(val, dst));
}

template<typename TBuf>
inline void PackInt32(i32 val, TBuf& buf) {
    PackUInt32(ZigZagEncode(val), buf);
}

template<typename TBuf>
inline void PackUInt16(ui16 val, TBuf& buf) {
    buf.Advance(MAX_PACKED32_SIZE);
    char* dst = buf.Pos() - MAX_PACKED32_SIZE;
    buf.EraseBack(MAX_PACKED32_SIZE - Pack32(val, dst));
}

template<typename TBuf>
inline void PackInt16(i16 val, TBuf& buf) {
    PackUInt16(ZigZagEncode(val), buf);
}

template <typename T, typename TBuf>
void PutRawData(T val, TBuf& buf) {
    buf.Advance(sizeof(T));
    std::memcpy(buf.Pos() - sizeof(T), &val, sizeof(T));
}

constexpr size_t MAX_PACKED_DECIMAL_SIZE = sizeof(NYql::NDecimal::TInt128);
template<typename TBuf>
void PackDecimal(NYql::NDecimal::TInt128 val, TBuf& buf) {
    buf.Advance(MAX_PACKED_DECIMAL_SIZE);
    char* dst = buf.Pos() - MAX_PACKED_DECIMAL_SIZE;
    buf.EraseBack(MAX_PACKED_DECIMAL_SIZE - NYql::NDecimal::Serialize(val, dst));
}

class TChunkedInputBuffer : private TNonCopyable {
public:
    explicit TChunkedInputBuffer(TRope&& rope)
        : Rope_(std::move(rope))
    {
        Next();
    }

    explicit TChunkedInputBuffer(TStringBuf input)
        : Rope_(TRope{})
        , Data_(input.data())
        , Len_(input.size())
    {
    }

    inline const char* data() const {
        return Data_;
    }

    inline size_t length() const {
        return Len_;
    }

    inline size_t size() const {
        return Len_;
    }

    inline void Skip(size_t size) {
        Y_DEBUG_ABORT_UNLESS(size <= Len_);
        Data_ += size;
        Len_ -= size;
    }

    bool IsEmpty() {
        if (size()) {
            return false;
        }
        Next();
        return size() == 0;
    }

    inline void CopyTo(char* dst, size_t toCopy) {
        if (Y_LIKELY(toCopy <= size())) {
            std::memcpy(dst, data(), toCopy);
            Skip(toCopy);
        } else {
            CopyToChunked(dst, toCopy);
        }
    }

    inline TRope ReleaseRope() {
        Y_DEBUG_ABORT_UNLESS(OriginalLen_ >= Len_);
        Rope_.EraseFront(OriginalLen_ - Len_);
        TRope result = std::move(Rope_);

        Data_ = nullptr;
        Len_ = OriginalLen_ = 0;
        Rope_.clear();

        return result;
    }

    void Next() {
        Y_DEBUG_ABORT_UNLESS(Len_ == 0);
        Rope_.EraseFront(OriginalLen_);
        if (!Rope_.IsEmpty()) {
            Len_ = OriginalLen_ = Rope_.begin().ContiguousSize();
            Data_ = Rope_.begin().ContiguousData();
            Y_DEBUG_ABORT_UNLESS(Len_ > 0);
        } else {
            Len_ = OriginalLen_ = 0;
            Data_ = nullptr;
        }
    }

private:
    void CopyToChunked(char* dst, size_t toCopy) {
        while (toCopy) {
            size_t chunkSize = std::min(size(), toCopy);
            std::memcpy(dst, data(), chunkSize);
            Skip(chunkSize);
            dst += chunkSize;
            toCopy -= chunkSize;
            if (toCopy) {
                Next();
                MKQL_ENSURE(size(), "Unexpected end of buffer");
            }
        }
    }

    TRope Rope_;
    const char* Data_ = nullptr;
    size_t Len_ = 0;
    size_t OriginalLen_ = 0;
};

template <typename T>
T GetRawData(TChunkedInputBuffer& buf) {
    T val;
    buf.CopyTo(reinterpret_cast<char*>(&val), sizeof(val));
    return val;
}

template<typename T>
T UnpackInteger(TChunkedInputBuffer& buf) {
    T res;
    size_t read;
    if constexpr (std::is_same_v<T, NYql::NDecimal::TInt128>) {
        std::tie(res, read) = NYql::NDecimal::Deserialize(buf.data(), buf.size());
        Y_DEBUG_ABORT_UNLESS((read != 0) xor (NYql::NDecimal::IsError(res)));
    } else if constexpr (std::is_same_v<T, ui64>) {
        read = Unpack64(buf.data(), buf.size(), res);
    } else {
        static_assert(std::is_same_v<T, ui32>, "Only ui32/ui64/TInt128 are supported");
        read = Unpack32(buf.data(), buf.size(), res);
    }

    if (Y_LIKELY(read > 0)) {
        buf.Skip(read);
        return res;
    }

    static_assert(MAX_PACKED_DECIMAL_SIZE > MAX_PACKED64_SIZE);
    char tmpBuf[MAX_PACKED_DECIMAL_SIZE];
    Y_DEBUG_ABORT_UNLESS(buf.size() < MAX_PACKED_DECIMAL_SIZE);
    std::memcpy(tmpBuf, buf.data(), buf.size());
    size_t pos = buf.size();
    buf.Skip(buf.size());

    for (;;) {
        if (buf.size() == 0) {
            buf.Next();
            MKQL_ENSURE(buf.size() > 0, (std::is_same_v<T, NYql::NDecimal::TInt128> ? "Bad decimal packed data" : "Bad uint packed data"));
        }
        Y_DEBUG_ABORT_UNLESS(pos < MAX_PACKED_DECIMAL_SIZE);
        tmpBuf[pos++] = *buf.data();
        buf.Skip(1);
        if constexpr (std::is_same_v<T, NYql::NDecimal::TInt128>) {
            std::tie(res, read) = NYql::NDecimal::Deserialize(tmpBuf, pos);
            Y_DEBUG_ABORT_UNLESS((read != 0) xor (NYql::NDecimal::IsError(res)));
        } else if constexpr (std::is_same_v<T, ui64>) {
            read = Unpack64(tmpBuf, pos, res);
        } else {
            read = Unpack32(tmpBuf, pos, res);
        }
        if (read) {
            break;
        }
    }
    return res;
}

inline NYql::NDecimal::TInt128 UnpackDecimal(TChunkedInputBuffer& buf) {
    return UnpackInteger<NYql::NDecimal::TInt128>(buf);
}

inline ui64 UnpackUInt64(TChunkedInputBuffer& buf) {
    return UnpackInteger<ui64>(buf);
}

inline i64 UnpackInt64(TChunkedInputBuffer& buf) {
    return ZigZagDecode(UnpackUInt64(buf));
}

inline ui32 UnpackUInt32(TChunkedInputBuffer& buf) {
    return UnpackInteger<ui32>(buf);
}

inline i32 UnpackInt32(TChunkedInputBuffer& buf) {
    return ZigZagDecode(UnpackUInt32(buf));
}

inline ui16 UnpackUInt16(TChunkedInputBuffer& buf) {
    ui32 res = UnpackUInt32(buf);
    MKQL_ENSURE(res <= Max<ui16>(), "Corrupted data");
    return res;
}

inline i16 UnpackInt16(TChunkedInputBuffer& buf) {
    return ZigZagDecode(UnpackUInt16(buf));
}

} // NDetails

}
}
