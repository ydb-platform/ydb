#define LLVM_BC
#include "yt_codec_cg.h"
#include <ydb/library/yql/utils/swap_bytes.h>
#include <ydb/library/yql/providers/common/codec/yql_codec_buf.h>
#include <ydb/library/yql/public/decimal/yql_decimal_serialize.h>
#include <ydb/library/yql/public/decimal/yql_decimal.h>
#include <ydb/library/yql/public/udf/udf_value.h>

#include <ydb/library/yql/public/decimal/yql_decimal_serialize.cpp>
#include <ydb/library/yql/public/decimal/yql_decimal.cpp>

using namespace NYql;

extern "C" void WriteJust(void* vbuf) {
    NCommon::TOutputBuf& buf = *(NCommon::TOutputBuf*)vbuf;
    buf.Write('\1');
}

extern "C" void WriteNothing(void* vbuf) {
    NCommon::TOutputBuf& buf = *(NCommon::TOutputBuf*)vbuf;
    buf.Write('\0');
}

extern "C" void WriteBool(void* vbuf, bool value) {
   NCommon::TOutputBuf& buf = *(NCommon::TOutputBuf*)vbuf;
   buf.Write(value ? '\1' : '\0');
}

extern "C" void Write8(void* vbuf, ui8 value) {
    NCommon::TOutputBuf& buf = *(NCommon::TOutputBuf*)vbuf;
    buf.Write(value);
}

extern "C" void Write16(void* vbuf, ui16 value) {
    NCommon::TOutputBuf& buf = *(NCommon::TOutputBuf*)vbuf;
    buf.WriteMany((const char*)&value, sizeof(value));
}

extern "C" void Write32(void* vbuf, ui32 value) {
    NCommon::TOutputBuf& buf = *(NCommon::TOutputBuf*)vbuf;
    buf.WriteMany((const char*)&value, sizeof(value));
}

extern "C" void Write64(void* vbuf, ui64 value) {
    NCommon::TOutputBuf& buf = *(NCommon::TOutputBuf*)vbuf;
    buf.WriteMany((const char*)&value, sizeof(value));
}
extern "C" void Write120(void* vbuf, const void* decimal) {
    auto value = reinterpret_cast<const NDecimal::TInt128*>(decimal);
    char b[sizeof(*value)];
    const ui32 size = NDecimal::Serialize(*value, b);
    NCommon::TOutputBuf& buf = *(NCommon::TOutputBuf*)vbuf;
    buf.WriteMany(reinterpret_cast<const char*>(&size), sizeof(size));
    buf.WriteMany(b, size);
}

extern "C" void WriteDecimal32(void* vbuf, const void* decimal) {
    auto value = reinterpret_cast<const NDecimal::TInt128*>(decimal);
    NCommon::TOutputBuf& buf = *(NCommon::TOutputBuf*)vbuf;
    i32 data = NDecimal::ToYtDecimal<i32>(*value);
    buf.WriteMany(reinterpret_cast<const char*>(&data), sizeof(data));
}
extern "C" void WriteDecimal64(void* vbuf, const void* decimal) {
    auto value = reinterpret_cast<const NDecimal::TInt128*>(decimal);
    NCommon::TOutputBuf& buf = *(NCommon::TOutputBuf*)vbuf;
    i64 data = NDecimal::ToYtDecimal<i64>(*value);
    buf.WriteMany(reinterpret_cast<const char*>(&data), sizeof(data));
}
extern "C" void WriteDecimal128(void* vbuf, const void* decimal) {
    auto value = reinterpret_cast<const NDecimal::TInt128*>(decimal);
    NCommon::TOutputBuf& buf = *(NCommon::TOutputBuf*)vbuf;
    NDecimal::TInt128 data = NDecimal::ToYtDecimal<NDecimal::TInt128>(*value);
    buf.WriteMany(reinterpret_cast<const char*>(&data), sizeof(data));
}
extern "C" void WriteFloat(void* vbuf, ui32 value) {
    NCommon::TOutputBuf& buf = *(NCommon::TOutputBuf*)vbuf;
    double data = (double)*(const float*)&value;
    buf.WriteMany((const char*)&data, sizeof(data));
}

extern "C" void WriteDouble(void* vbuf, ui64 value) {
    NCommon::TOutputBuf& buf = *(NCommon::TOutputBuf*)vbuf;
    buf.WriteMany((const char*)&value, sizeof(value));
}

extern "C" void WriteString(void* vbuf, const char* buffer, ui32 len) {
    NCommon::TOutputBuf& buf = *(NCommon::TOutputBuf*)vbuf;
    buf.WriteMany(buffer, len);
}

extern "C" void ReadBool(void* vbuf, void* vpod) {
    NCommon::TInputBuf& buf = *(NCommon::TInputBuf*)vbuf;
    char cmd = buf.Read();
    new (vpod) NUdf::TUnboxedValuePod(cmd != 0);
}

extern "C" void ReadInt8(void* vbuf, void* vpod) {
    NCommon::TInputBuf& buf = *(NCommon::TInputBuf*)vbuf;
    i64 data;
    buf.ReadMany((char*)&data, sizeof(data));
    new (vpod) NUdf::TUnboxedValuePod(i8(data));
}

extern "C" void ReadUint8(void* vbuf, void* vpod) {
    NCommon::TInputBuf& buf = *(NCommon::TInputBuf*)vbuf;
    ui64 data;
    buf.ReadMany((char*)&data, sizeof(data));
    new (vpod) NUdf::TUnboxedValuePod(ui8(data));
}

extern "C" void ReadInt16(void* vbuf, void* vpod) {
    NCommon::TInputBuf& buf = *(NCommon::TInputBuf*)vbuf;
    i64 data;
    buf.ReadMany((char*)&data, sizeof(data));
    new (vpod) NUdf::TUnboxedValuePod(i16(data));
}

extern "C" void ReadUint16(void* vbuf, void* vpod) {
    NCommon::TInputBuf& buf = *(NCommon::TInputBuf*)vbuf;
    ui64 data;
    buf.ReadMany((char*)&data, sizeof(data));
    new (vpod) NUdf::TUnboxedValuePod(ui16(data));
}

extern "C" void ReadInt32(void* vbuf, void* vpod) {
    NCommon::TInputBuf& buf = *(NCommon::TInputBuf*)vbuf;
    i64 data;
    buf.ReadMany((char*)&data, sizeof(data));
    new (vpod) NUdf::TUnboxedValuePod(i32(data));
}

extern "C" void ReadUint32(void* vbuf, void* vpod) {
    NCommon::TInputBuf& buf = *(NCommon::TInputBuf*)vbuf;
    ui64 data;
    buf.ReadMany((char*)&data, sizeof(data));
    new (vpod) NUdf::TUnboxedValuePod(ui32(data));
}

extern "C" void ReadInt64(void* vbuf, void* vpod) {
    NCommon::TInputBuf& buf = *(NCommon::TInputBuf*)vbuf;
    i64 data;
    buf.ReadMany((char*)&data, sizeof(data));
    new (vpod) NUdf::TUnboxedValuePod(data);
}

extern "C" void ReadUint64(void* vbuf, void* vpod) {
    NCommon::TInputBuf& buf = *(NCommon::TInputBuf*)vbuf;
    ui64 data;
    buf.ReadMany((char*)&data, sizeof(data));
    new (vpod) NUdf::TUnboxedValuePod(data);
}
extern "C" void ReadInt120(void* vbuf, void* vpod) {
    NCommon::TInputBuf& buf = *(NCommon::TInputBuf*)vbuf;
    ui32 size;
    buf.ReadMany(reinterpret_cast<char*>(&size), sizeof(size));
    
    if (size <= sizeof(NDecimal::TInt128)) {
        char data[sizeof(NDecimal::TInt128)];
        buf.ReadMany(data, size);
        auto v = NDecimal::Deserialize(data, size).first;
        if (v == NDecimal::Err()) {
            ThrowBadDecimal();
        } else {
            new (vpod) NUdf::TUnboxedValuePod(v);
        }
    } else {
        ThrowBadDecimal();
    }
}

extern "C" void ReadDecimal32(void* vbuf, void* vpod) {
    NCommon::TInputBuf& buf = *(NCommon::TInputBuf*)vbuf;
    i32 data;
    buf.ReadMany((char*)&data, sizeof(data));
    new (vpod) NUdf::TUnboxedValuePod(NDecimal::FromYtDecimal(data));
}
extern "C" void ReadDecimal64(void* vbuf, void* vpod) {
    NCommon::TInputBuf& buf = *(NCommon::TInputBuf*)vbuf;
    i64 data;
    buf.ReadMany((char*)&data, sizeof(data));
    new (vpod) NUdf::TUnboxedValuePod(NDecimal::FromYtDecimal(data));
}
extern "C" void ReadDecimal128(void* vbuf, void* vpod) {
    NCommon::TInputBuf& buf = *(NCommon::TInputBuf*)vbuf;
    NDecimal::TInt128 data;
    buf.ReadMany((char*)&data, sizeof(data));
    new (vpod) NUdf::TUnboxedValuePod(NDecimal::FromYtDecimal(data));
}
extern "C" void ReadFloat(void* vbuf, void* vpod) {
    NCommon::TInputBuf& buf = *(NCommon::TInputBuf*)vbuf;
    double data;
    buf.ReadMany((char*)&data, sizeof(data));
    new (vpod) NUdf::TUnboxedValuePod(float(data));
}

extern "C" void ReadDouble(void* vbuf, void* vpod) {
    NCommon::TInputBuf& buf = *(NCommon::TInputBuf*)vbuf;
    double data;
    buf.ReadMany((char*)&data, sizeof(data));
    new (vpod) NUdf::TUnboxedValuePod(data);
}

extern "C" ui8 ReadOptional(void* vbuf) {
    NCommon::TInputBuf& buf = *(NCommon::TInputBuf*)vbuf;
    return buf.Read();
}

extern "C" ui16 ReadVariantData(void* vbuf, ui8 oneByte) {
    NCommon::TInputBuf& buf = *(NCommon::TInputBuf*)vbuf;
    if (oneByte) {
        return buf.Read();
    } else {
        ui16 data = 0;
        buf.ReadMany((char*)&data, sizeof(data));
        return data;
    }
}

extern "C" void SkipFixedData(void* vbuf, ui64 size) {
    NCommon::TInputBuf& buf = *(NCommon::TInputBuf*)vbuf;
    buf.SkipMany(size);
}

extern "C" void SkipVarData(void* vbuf) {
    NCommon::TInputBuf& buf = *(NCommon::TInputBuf*)vbuf;
    ui32 size;
    buf.ReadMany((char*)&size, sizeof(size));
    buf.SkipMany(size);
}

extern "C" void ReadTzDate(void* vbuf, void* vpod) {
    NCommon::TInputBuf& buf = *(NCommon::TInputBuf*)vbuf;
    ui32 size;
    buf.ReadMany((char*)&size, sizeof(size));
    ui16 data;
    buf.ReadMany((char*)&data, sizeof(data));
    ui16 tzId;
    buf.ReadMany((char*)&tzId, sizeof(tzId));
    data = SwapBytes(data);
    tzId = SwapBytes(tzId);
    (new (vpod) NUdf::TUnboxedValuePod(data))->SetTimezoneId(tzId);
}

extern "C" void ReadTzDatetime(void* vbuf, void* vpod) {
    NCommon::TInputBuf& buf = *(NCommon::TInputBuf*)vbuf;
    ui32 size;
    buf.ReadMany((char*)&size, sizeof(size));
    ui32 data;
    buf.ReadMany((char*)&data, sizeof(data));
    ui16 tzId;
    buf.ReadMany((char*)&tzId, sizeof(tzId));
    data = SwapBytes(data);
    tzId = SwapBytes(tzId);
    (new (vpod) NUdf::TUnboxedValuePod(data))->SetTimezoneId(tzId);
}

extern "C" void ReadTzTimestamp(void* vbuf, void* vpod) {
    NCommon::TInputBuf& buf = *(NCommon::TInputBuf*)vbuf;
    ui32 size;
    buf.ReadMany((char*)&size, sizeof(size));
    ui64 data;
    buf.ReadMany((char*)&data, sizeof(data));
    ui16 tzId;
    buf.ReadMany((char*)&tzId, sizeof(tzId));
    data = SwapBytes(data);
    tzId = SwapBytes(tzId);
    (new (vpod) NUdf::TUnboxedValuePod(data))->SetTimezoneId(tzId);
}

extern "C" void ReadTzDate32(void* vbuf, void* vpod) {
    NCommon::TInputBuf& buf = *(NCommon::TInputBuf*)vbuf;
    ui32 size;
    buf.ReadMany((char*)&size, sizeof(size));
    ui32 data;
    buf.ReadMany((char*)&data, sizeof(data));
    ui16 tzId;
    buf.ReadMany((char*)&tzId, sizeof(tzId));
    i32 value = SwapBytes(0x80 ^ data);
    tzId = SwapBytes(tzId);
    (new (vpod) NUdf::TUnboxedValuePod(value))->SetTimezoneId(tzId);
}

extern "C" void ReadTzDatetime64(void* vbuf, void* vpod) {
    NCommon::TInputBuf& buf = *(NCommon::TInputBuf*)vbuf;
    ui32 size;
    buf.ReadMany((char*)&size, sizeof(size));
    ui64 data;
    buf.ReadMany((char*)&data, sizeof(data));
    ui16 tzId;
    buf.ReadMany((char*)&tzId, sizeof(tzId));
    i64 value = SwapBytes(0x80 ^ data);
    tzId = SwapBytes(tzId);
    (new (vpod) NUdf::TUnboxedValuePod(value))->SetTimezoneId(tzId);
}

extern "C" void ReadTzTimestamp64(void* vbuf, void* vpod) {
    NCommon::TInputBuf& buf = *(NCommon::TInputBuf*)vbuf;
    ui32 size;
    buf.ReadMany((char*)&size, sizeof(size));
    ui64 data;
    buf.ReadMany((char*)&data, sizeof(data));
    ui16 tzId;
    buf.ReadMany((char*)&tzId, sizeof(tzId));
    i64 value = SwapBytes(0x80 ^ data);
    tzId = SwapBytes(tzId);
    (new (vpod) NUdf::TUnboxedValuePod(value))->SetTimezoneId(tzId);
}

extern "C" void WriteTzDate(void* vbuf, ui16 value, ui16 tzId) {
    value = SwapBytes(value);
    tzId = SwapBytes(tzId);
    NCommon::TOutputBuf& buf = *(NCommon::TOutputBuf*)vbuf;
    const ui32 size = sizeof(value) + sizeof(tzId);
    buf.WriteMany((const char*)&size, sizeof(size));
    buf.WriteMany((const char*)&value, sizeof(value));
    buf.WriteMany((const char*)&tzId, sizeof(tzId));
}

extern "C" void WriteTzDatetime(void* vbuf, ui32 value, ui16 tzId) {
    value = SwapBytes(value);
    tzId = SwapBytes(tzId);
    NCommon::TOutputBuf& buf = *(NCommon::TOutputBuf*)vbuf;
    const ui32 size = sizeof(value) + sizeof(tzId);
    buf.WriteMany((const char*)&size, sizeof(size));
    buf.WriteMany((const char*)&value, sizeof(value));
    buf.WriteMany((const char*)&tzId, sizeof(tzId));
}

extern "C" void WriteTzTimestamp(void* vbuf, ui64 value, ui16 tzId) {
    value = SwapBytes(value);
    tzId = SwapBytes(tzId);
    NCommon::TOutputBuf& buf = *(NCommon::TOutputBuf*)vbuf;
    const ui32 size = sizeof(value) + sizeof(tzId);
    buf.WriteMany((const char*)&size, sizeof(size));
    buf.WriteMany((const char*)&value, sizeof(value));
    buf.WriteMany((const char*)&tzId, sizeof(tzId));
}

extern "C" void WriteTzDate32(void* vbuf, i32 value, ui16 tzId) {
    ui32 data = 0x80 ^ SwapBytes((ui32)value);
    tzId = SwapBytes(tzId);
    NCommon::TOutputBuf& buf = *(NCommon::TOutputBuf*)vbuf;
    const ui32 size = sizeof(data) + sizeof(tzId);
    buf.WriteMany((const char*)&size, sizeof(size));
    buf.WriteMany((const char*)&data, sizeof(data));
    buf.WriteMany((const char*)&tzId, sizeof(tzId));
}

extern "C" void WriteTzDatetime64(void* vbuf, i64 value, ui16 tzId) {
    ui64 data = 0x80 ^ SwapBytes((ui64)value);
    tzId = SwapBytes(tzId);
    NCommon::TOutputBuf& buf = *(NCommon::TOutputBuf*)vbuf;
    const ui32 size = sizeof(data) + sizeof(tzId);
    buf.WriteMany((const char*)&size, sizeof(size));
    buf.WriteMany((const char*)&data, sizeof(data));
    buf.WriteMany((const char*)&tzId, sizeof(tzId));
}

extern "C" void WriteTzTimestamp64(void* vbuf, i64 value, ui16 tzId) {
    ui64 data = 0x80 ^ SwapBytes((ui64)value);
    tzId = SwapBytes(tzId);
    NCommon::TOutputBuf& buf = *(NCommon::TOutputBuf*)vbuf;
    const ui32 size = sizeof(data) + sizeof(tzId);
    buf.WriteMany((const char*)&size, sizeof(size));
    buf.WriteMany((const char*)&data, sizeof(data));
    buf.WriteMany((const char*)&tzId, sizeof(tzId));
}

extern "C" ui64 GetWrittenBytes(void* vbuf) {
    NCommon::TOutputBuf& buf = *(NCommon::TOutputBuf*)vbuf;
    return buf.GetWrittenBytes();
}

extern "C" void FillZero(void* vpod) {
    new (vpod) NUdf::TUnboxedValuePod(NUdf::TUnboxedValuePod::Zero());
}
