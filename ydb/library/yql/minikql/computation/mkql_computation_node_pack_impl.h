#pragma once

#include <ydb/library/yql/minikql/defs.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/pack_num.h>
#include <ydb/library/yql/public/udf/udf_value.h>

#include <util/generic/buffer.h>
#include <util/generic/strbuf.h>
#include <library/cpp/packedtypes/zigzag.h>

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

inline ui64 UnpackUInt64(TStringBuf& buf) {
    ui64 res = 0;
    size_t read = Unpack64(buf.data(), buf.length(), res);
    MKQL_ENSURE(read, "Bad ui64 packed data");
    buf.Skip(read);
    return res;
}

inline i64 UnpackInt64(TStringBuf& buf) {
    return ZigZagDecode(UnpackUInt64(buf));
}

inline ui32 UnpackUInt32(TStringBuf& buf) {
    ui32 res = 0;
    size_t read = Unpack32(buf.data(), buf.length(), res);
    MKQL_ENSURE(read, "Bad ui32 packed data");
    buf.Skip(read);
    return res;
}

inline i32 UnpackInt32(TStringBuf& buf) {
    return ZigZagDecode(UnpackUInt32(buf));
}

inline ui16 UnpackUInt16(TStringBuf& buf) {
    ui32 res = 0;
    size_t read = Unpack32(buf.data(), buf.length(), res);
    MKQL_ENSURE(read, "Bad ui32 packed data");
    buf.Skip(read);
    MKQL_ENSURE(res <= Max<ui16>(), "Corrupted data");
    return res;
}

inline i16 UnpackInt16(TStringBuf& buf) {
    return ZigZagDecode(UnpackUInt16(buf));
}

template <typename T, typename TBuf>
void PutRawData(T val, TBuf& buf) {
    buf.Advance(sizeof(T));
    std::memcpy(buf.Pos() - sizeof(T), &val, sizeof(T));
}

template <typename T>
T GetRawData(TStringBuf& buf) {
    MKQL_ENSURE(sizeof(T) <= buf.size(), "Bad packed data. Buffer too small");
    T val;
    std::memcpy(&val, buf.data(), sizeof(T));
    buf.Skip(sizeof(T));
    return val;
}

} // NDetails

}
}
