#include <util/generic/buffer.h>

#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/minikql/pack_num.h>
#include <library/cpp/packedtypes/zigzag.h>

#include <ydb/library/yql/minikql/defs.h>
#include "mkql_optional_usage_mask.h"

using namespace NKikimr;

namespace NDetails {

void PackUInt64(ui64 pos, ui64 val, TBuffer& buf) {
    const auto actual = Pack64(val, buf.Data() + pos);
    buf.Chop(pos + actual, MAX_PACKED64_SIZE - actual);
}

void PackUInt64(ui64 val, TBuffer& buf) {
    size_t off = buf.Size();
    buf.Advance(MAX_PACKED64_SIZE);
    buf.EraseBack(MAX_PACKED64_SIZE - Pack64(val, buf.Data() + off));
}

void PackInt64(i64 val, TBuffer& buf) {
    PackUInt64(ZigZagEncode(val), buf);
}

void PackUInt32(ui32 val, TBuffer& buf) {
    size_t off = buf.Size();
    buf.Advance(MAX_PACKED32_SIZE);
    buf.EraseBack(MAX_PACKED32_SIZE - Pack32(val, buf.Data() + off));
}

void PackInt32(i32 val, TBuffer& buf) {
    PackUInt32(ZigZagEncode(val), buf);
}

ui64 UnpackUInt64(TStringBuf& buf) {
    ui64 res = 0;
    size_t read = Unpack64(buf.data(), buf.length(), res);
    MKQL_ENSURE(read, "Bad ui64 packed data");
    buf.Skip(read);
    return res;
}

i64 UnpackInt64(TStringBuf& buf) {
    return ZigZagDecode(UnpackUInt64(buf));
}

ui32 UnpackUInt32(TStringBuf& buf) {
    ui32 res = 0;
    size_t read = Unpack32(buf.data(), buf.length(), res);
    MKQL_ENSURE(read, "Bad ui32 packed data");
    buf.Skip(read);
    return res;
}

i32 UnpackInt32(TStringBuf& buf) {
    return ZigZagDecode(UnpackUInt32(buf));
}

template <typename T>
void PutRawData(T val, TBuffer& buf) {
    buf.Append(reinterpret_cast<const char*>(&val), sizeof(T));
}

template <typename T>
T GetRawData(TStringBuf& buf) {
    MKQL_ENSURE(sizeof(T) <= buf.size(), "Bad packed data. Buffer too small");
    T val = 0;
    std::memcpy(&val, buf.data(), sizeof(T));
    buf.Skip(sizeof(T));
    return val;
}

}

extern "C" void GetElement(const TRawUV* value, ui32 index, TRawUV* item) {
    const auto result(reinterpret_cast<const NUdf::TUnboxedValuePod*>(value)->GetElement(index));
    *item = reinterpret_cast<const TRawUV&>(result);
}

extern "C" bool FetchNextItem(const TRawUV* value, TRawUV* item) {
    NUdf::TUnboxedValue fetch;
    if (NUdf::EFetchStatus::Finish == reinterpret_cast<const NUdf::TUnboxedValuePod*>(value)->Fetch(fetch))
        return false;
    *item = reinterpret_cast<TRawUV&>(fetch);
    return true;
}

extern "C" bool NextListItem(TRawUV* value, TRawUV* item) {
    NUdf::TUnboxedValue next;
    const auto v = reinterpret_cast<NUdf::TUnboxedValuePod*>(value);
    if (!v->Next(next)) {
        v->DeleteUnreferenced();
        return false;
    }

    *item = reinterpret_cast<TRawUV&>(next);
    return true;
}

extern "C" bool NextDictItem(TRawUV* value, TRawUV* first, TRawUV* second) {
    NUdf::TUnboxedValue key, payload;
    const auto v = reinterpret_cast<NUdf::TUnboxedValuePod*>(value);
    if (!v->NextPair(key, payload)) {
        v->DeleteUnreferenced();
        return false;
    }

    *first = reinterpret_cast<TRawUV&>(key);
    *second = reinterpret_cast<TRawUV&>(payload);
    return true;
}

extern "C" void PackString(const TRawUV* value, ui64* buffer) {
    const auto& stringRef = reinterpret_cast<const NUdf::TUnboxedValue*>(value)->AsStringRef();
    NDetails::PackUInt32(stringRef.Size(), *reinterpret_cast<TBuffer*>(buffer));
    reinterpret_cast<TBuffer*>(buffer)->Append(stringRef.Data(), stringRef.Size());
}

extern "C" void PackStringData(const TRawUV* value, ui64* buffer) {
    const auto& stringRef = reinterpret_cast<const NUdf::TUnboxedValue*>(value)->AsStringRef();
    reinterpret_cast<TBuffer*>(buffer)->Append(stringRef.Data(), stringRef.Size());
}

extern "C" void PackBool(const TRawUV* value, ui64* buffer) {
    NDetails::PutRawData(reinterpret_cast<const NUdf::TUnboxedValue*>(value)->Get<bool>(), *reinterpret_cast<TBuffer*>(buffer));
}

extern "C" void PackByte(const TRawUV* value, ui64* buffer) {
    NDetails::PutRawData(reinterpret_cast<const NUdf::TUnboxedValue*>(value)->Get<ui8>(), *reinterpret_cast<TBuffer*>(buffer));
}

extern "C" void PackInt32(const TRawUV* value, ui64* buffer) {
    NDetails::PackInt32(reinterpret_cast<const NUdf::TUnboxedValue*>(value)->Get<i32>(), *reinterpret_cast<TBuffer*>(buffer));
}

extern "C" void PackUInt32(const TRawUV* value, ui64* buffer) {
    NDetails::PackUInt32(reinterpret_cast<const NUdf::TUnboxedValue*>(value)->Get<ui32>(), *reinterpret_cast<TBuffer*>(buffer));
}

extern "C" void PackInt64(const TRawUV* value, ui64* buffer) {
    NDetails::PackInt64(reinterpret_cast<const NUdf::TUnboxedValue*>(value)->Get<i64>(), *reinterpret_cast<TBuffer*>(buffer));
}

extern "C" void PackUInt64(const TRawUV* value, ui64* buffer) {
    NDetails::PackUInt64(reinterpret_cast<const NUdf::TUnboxedValue*>(value)->Get<ui64>(), *reinterpret_cast<TBuffer*>(buffer));
}

extern "C" void PackFloat(const TRawUV* value, ui64* buffer) {
    NDetails::PutRawData(reinterpret_cast<const NUdf::TUnboxedValue*>(value)->Get<float>(), *reinterpret_cast<TBuffer*>(buffer));
}

extern "C" void PackDouble(const TRawUV* value, ui64* buffer) {
    NDetails::PutRawData(reinterpret_cast<const NUdf::TUnboxedValue*>(value)->Get<double>(), *reinterpret_cast<TBuffer*>(buffer));
}

extern "C" ui32 GetVariantItem(const TRawUV* value, TRawUV* item, ui64* buffer) {
    const auto v = reinterpret_cast<const NUdf::TUnboxedValuePod*>(value);
    const ui32 index = v->GetVariantIndex();
    NDetails::PackUInt32(index, *reinterpret_cast<TBuffer*>(buffer));
    const auto result(v->GetVariantItem());
    *item = reinterpret_cast<const TRawUV&>(result);
    return index;
}

extern "C" bool GetListIterator(const TRawUV* value, TRawUV* item, ui64* buffer) {
    const auto v = reinterpret_cast<const NUdf::TUnboxedValuePod*>(value);
    const auto size = v->GetListLength();
    NDetails::PackUInt64(size, *reinterpret_cast<TBuffer*>(buffer));
    if (!size)
        return false;
    const auto result(v->GetListIterator().Release());
    *item = reinterpret_cast<const TRawUV&>(result);
    return true;
}

extern "C" bool GetDictIterator(const TRawUV* value, TRawUV* item, ui64* buffer) {
    const auto v = reinterpret_cast<const NUdf::TUnboxedValuePod*>(value);
    const auto size = v->GetDictLength();
    NDetails::PackUInt64(size, *reinterpret_cast<TBuffer*>(buffer));
    if (!size)
        return false;
    const auto result(v->GetDictIterator().Release());
    *item = reinterpret_cast<const TRawUV&>(result);
    return true;
}

extern "C" bool GetOptionalValue(const TRawUV* value, TRawUV* item, ui64* mask) {
    const auto v = reinterpret_cast<const NUdf::TUnboxedValuePod*>(value);
    const bool has = bool(*v);
    reinterpret_cast<NMiniKQL::NDetails::TOptionalUsageMask*>(mask)->SetNextEmptyOptional(!*v);
    if (has) {
        const auto result(v->GetOptionalValue());
        *item = reinterpret_cast<const TRawUV&>(result);
    }
    return has;
}
