#pragma once

#include <ydb/library/yql/minikql/defs.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/pack_num.h>
#include <ydb/library/yql/public/udf/udf_value.h>

#include <library/cpp/packedtypes/zigzag.h>
#include <library/cpp/actors/util/rope.h>

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

class TChunkedInputBuffer : private TNonCopyable {
public:
    explicit TChunkedInputBuffer(TRope&& rope)
        : Rope_(std::move(rope))
        , It_(Rope_.begin())
    {
        Next();
    }

    explicit TChunkedInputBuffer(TStringBuf input)
        : Rope_(TRope{})
        , It_(Rope_.begin())
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
        Y_VERIFY_DEBUG(size <= Len_);
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

    void Next() {
        Y_VERIFY_DEBUG(Len_ == 0);
        Len_ = 0;
        Data_ = nullptr;
        while (It_ != Rope_.end()) {
            Len_ = It_.ContiguousSize();
            Data_ = It_.ContiguousData();
            ++It_;
            if (Len_) {
                break;
            }
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
    TRope::TConstIterator It_;
    const char* Data_ = nullptr;
    size_t Len_ = 0;
};

template <typename T>
T GetRawData(TChunkedInputBuffer& buf) {
    T val;
    buf.CopyTo(reinterpret_cast<char*>(&val), sizeof(val));
    return val;
}

template<typename T>
T UnpackUInt(TChunkedInputBuffer& buf) {
    T res;
    size_t read;
    if constexpr (std::is_same_v<T, ui64>) {
        read = Unpack64(buf.data(), buf.size(), res);
    } else {
        static_assert(std::is_same_v<T, ui32>, "Only ui32/ui64 are supported");
        read = Unpack32(buf.data(), buf.size(), res);
    }

    if (Y_LIKELY(read > 0)) {
        buf.Skip(read);
        return res;
    }

    char tmpBuf[MAX_PACKED64_SIZE];
    Y_VERIFY_DEBUG(buf.size() < MAX_PACKED64_SIZE);
    std::memcpy(tmpBuf, buf.data(), buf.size());
    size_t pos = buf.size();
    buf.Skip(buf.size());

    for (;;) {
        if (buf.size() == 0) {
            buf.Next();
            MKQL_ENSURE(buf.size() > 0, "Bad uint packed data");
        }
        Y_VERIFY_DEBUG(pos < MAX_PACKED64_SIZE);
        tmpBuf[pos++] = *buf.data();
        buf.Skip(1);
        if constexpr (std::is_same_v<T, ui64>) {
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

inline ui64 UnpackUInt64(TChunkedInputBuffer& buf) {
    return UnpackUInt<ui64>(buf);
}

inline i64 UnpackInt64(TChunkedInputBuffer& buf) {
    return ZigZagDecode(UnpackUInt64(buf));
}

inline ui32 UnpackUInt32(TChunkedInputBuffer& buf) {
    return UnpackUInt<ui32>(buf);
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
