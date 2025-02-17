#pragma once

#include "scheme_type_info.h"

#include <util/generic/array_ref.h>
#include <util/string/builder.h>

namespace NKikimr {

class TRawTypeValue
{
public:
    //
    TRawTypeValue()
        : Buffer(nullptr)
        , BufferSize(0)
        , ValueType()
    {}

    TRawTypeValue(const void* buf, ui32 bufSize, NScheme::TTypeId vtype)
        : Buffer(buf)
        , BufferSize(bufSize)
        , ValueType(vtype)
    {
        Y_DEBUG_ABORT_UNLESS(!buf || vtype != 0);
    }

    TRawTypeValue(TArrayRef<const char> ref, NScheme::TTypeId vtype)
        : TRawTypeValue((void*)ref.data(), ref.size(), vtype)
    {}

    const void* Data() const { return Buffer; }
    ui32 Size() const { return BufferSize; }
    NScheme::TTypeId Type() const { return ValueType; }

    // we must distinguish empty raw type value (nothing, buffer == nullptr)
    // and zero-length string (value exists, but zero-length)
    bool IsEmpty() const { return Buffer == nullptr; }
    explicit operator bool() const noexcept { return !IsEmpty(); }

    TString ToString() const {
        TStringBuilder builder;
        builder << "(type:" << ValueType;
        if (!IsEmpty()) {
            builder << ", value:" << TString((const char*)Buffer, BufferSize).Quote();
        }
        builder << ")";
        return std::move(builder);
    }

    TStringBuf ToStringBuf() const {
        return TStringBuf((const char*)Buffer, BufferSize);
    }

    TArrayRef<const char> AsRef() const noexcept {
        return { static_cast<const char*>(Data()), Size() };
    }

private:
    const void* Buffer;
    ui32 BufferSize;
    NScheme::TTypeId ValueType;
};

} // namspace NKikimr

inline IOutputStream& operator << (IOutputStream& out, const NKikimr::TRawTypeValue& v) {
    out << v.ToString();
    return out;
}
