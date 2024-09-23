#pragma once

#include "scheme_types.h"
#include "scheme_raw_type_value.h"

#include <util/charset/utf8.h>
#include <util/generic/hash.h>
#include <util/stream/output.h> // for IOutputStream
#include <util/string/ascii.h>

#define DECLARE_TYPED_TYPE_NAME(x)\
    const char x[] = #x;

namespace NKikimr {
namespace NScheme {

const ui32 MaxKeyValueSize = 4096;

////////////////////////////////////////////////////////
template<typename T, typename TDerived, TTypeId TypeId_, const char* Name_>
class TTypedType : public IType {
public:
    //
    typedef T TValueType;
    static constexpr TTypeId TypeId = TypeId_;

    //
    class TInstance {
    public:
        //
        const T Value;

        TInstance(const T& v) : Value(v) {}

        operator TRawTypeValue() const {
            return TDerived::ToRawTypeValue(Value);
        }
    };

    TTypedType() = default;

    // IType interface
    const char* GetName() const override {
        return Name_;
    }

    static constexpr ui32 GetFixedSize() {
        return sizeof(T);
    }
    TTypeId GetTypeId() const override { return TypeId; }
    static TRawTypeValue ToRawTypeValue(const T& value) {
        return TRawTypeValue((void*)&value, sizeof(T), TTypeInfo(TypeId));
    }

    static const char* TypeName() {
        return Name_;
    }
};

////////////////////////////////////////////////////////

template<typename T, ui32 TypeId, const char* Name>
class IIntegerTypeWithKeyString : public TTypedType<T, IIntegerTypeWithKeyString<T, TypeId, Name>, TypeId, Name> {
    static_assert(std::is_integral<T>::value, "expect std::is_integral<T>::value");
public:
};

////////////////////////////////////////////////////////
/// Integer types
/// 0x01 - 0x20
/// DO NOT FORGET TO REGISTER THE TYPES in Library::OpenLibrary() / file tablet_library.h
namespace NNames {
    extern const char Int32[6];
    extern const char Uint32[7];
    extern const char Int64[6];
    extern const char Uint64[7];
}

class TInt32 : public IIntegerTypeWithKeyString<i32, NTypeIds::Int32, NNames::Int32> {};
class TUint32 : public IIntegerTypeWithKeyString<ui32, NTypeIds::Uint32, NNames::Uint32> {};
class TInt64 : public IIntegerTypeWithKeyString<i64, NTypeIds::Int64, NNames::Int64> {};
class TUint64 : public IIntegerTypeWithKeyString<ui64, NTypeIds::Uint64, NNames::Uint64> {};

// upyachka to get around undefined tryfromstring for chars

namespace NNames {
    extern const char Int8[5];
    extern const char Uint8[6];
    extern const char Int16[6];
    extern const char Uint16[7];
}

class TInt8 : public TTypedType<i8, TInt8, NTypeIds::Int8, NNames::Int8> {
public:
};

class TUint8 : public TTypedType<ui8, TUint8, NTypeIds::Uint8, NNames::Uint8> {
public:
};

class TInt16 : public TTypedType<i16, TInt16, NTypeIds::Int16, NNames::Int16> {
public:
};

class TUint16 : public TTypedType<ui16, TUint16, NTypeIds::Uint16, NNames::Uint16> {
public:
};

namespace NNames {
    extern const char Bool[5];
}

class TBool : public TTypedType<bool, TBool, NTypeIds::Bool, NNames::Bool> {
public:
};

namespace NNames {
    extern const char Float[6];
    extern const char Double[7];
}

template<typename T, typename TDerived, ui32 TypeId, const char *Name>
class TRealBase : public TTypedType<T, TDerived, TypeId, Name> {
public:
};

class TDouble : public TRealBase<double, TDouble, NTypeIds::Double, NNames::Double> {};
class TFloat : public TRealBase<float, TFloat, NTypeIds::Float, NNames::Float> {};

////////////////////////////////////////////////////////
template<typename TFirst, typename TSecond, ui32 TypeId, const char* Name>
class IIntegerPair : public TTypedType<
    std::pair<TFirst, TSecond>,
    IIntegerPair<TFirst, TSecond, TypeId, Name>,
    TypeId, Name
    >
{
};

////////////////////////////////////////////////////////
/// Integer pair types
/// 0x101 - 0x200
/// DO NOT FORGET TO REGISTER THE TYPES in Library::OpenLibrary() / file tablet_library.h
namespace NNames {
    extern const char PairUi64Ui64[13];
}

class TPairUi64Ui64 : public IIntegerPair<ui64, ui64, NTypeIds::PairUi64Ui64, NNames::PairUi64Ui64> {};


////////////////////////////////////////////////////////
/// Byte strings
/// 0x1001 - 0x2000
/// DO NOT FORGET TO REGISTER THE TYPES in Library::OpenLibrary() / file tablet_library.h
// TODO: this String implementation is not quite correct - think about it later - once we decide to use it - AL
class TStringImpl {
public:
};

template <typename TDerived, TTypeId TypeId, const char* Name>
class TStringBase : public TTypedType<::TString, TDerived, TypeId, Name>
{
public:
    static constexpr ui32 GetFixedSize() {
        return 0;
    }

    static TRawTypeValue ToRawTypeValue(const ::TString& value) {
        return TRawTypeValue((const void*)value.data(), value.size(), TTypeInfo(TypeId));
    }
};

namespace NNames {
    extern const char String[7];
    extern const char Utf8[5];
    extern const char Yson[5];
    extern const char Json[5];
    extern const char JsonDocument[13];
    extern const char DyNumber[9];
}

void WriteEscapedValue(IOutputStream &out, const char *data, size_t size);

class TString : public TStringBase<TString, NTypeIds::String, NNames::String> {};

class TUtf8 : public TStringBase<TUtf8, NTypeIds::Utf8, NNames::Utf8> {
public:
};

class TYson : public TStringBase<TYson, NTypeIds::Yson, NNames::Yson> {
public:
};

class TJson : public TStringBase<TJson, NTypeIds::Json, NNames::Json> {
public:
};

class TJsonDocument : public TStringBase<TJsonDocument, NTypeIds::JsonDocument, NNames::JsonDocument> {
public:
};

class TDyNumber : public TStringBase<TDyNumber, NTypeIds::DyNumber, NNames::DyNumber> {
public:
};

template <ui32 TMaxSize, TTypeId TypeId, const char* Name>
class TBoundedString : public TStringBase<TBoundedString<TMaxSize, TypeId, Name>, TypeId, Name> {
public:
    static constexpr ui32 MaxSize = TMaxSize;
    static constexpr ui32 GetFixedSize() {
        return 0;
    }

    static TRawTypeValue ToRawTypeValue(const ::TString& value) {
        Y_ABORT_UNLESS(value.size() <= MaxSize);
        return TRawTypeValue((const void*)value.data(), value.size(), TTypeInfo(TypeId));
    }
};

namespace NNames {
    extern const char SmallBoundedString[19];
    extern const char LargeBoundedString[19];
}

using TSmallBoundedString = TBoundedString<0x1000, NTypeIds::String4k, NNames::SmallBoundedString>; // 4K
using TLargeBoundedString = TBoundedString<0x200000, NTypeIds::String2m, NNames::LargeBoundedString>; // 2Mb

namespace NNames {
    extern const char Decimal[8];
    extern const char Uuid[5];
}

class TDecimal : public IIntegerPair<ui64, i64, NTypeIds::Decimal, NNames::Decimal> {};

class TUuid : public TTypedType<char[16], TUuid, NTypeIds::Uuid, NNames::Uuid> {
public:
};

////////////////////////////////////////////////////////
/// Datetime types
namespace NNames {
    extern const char Date[5];
    extern const char Datetime[9];
    extern const char Timestamp[10];
    extern const char Interval[9];
    extern const char Date32[7];
    extern const char Datetime64[11];
    extern const char Timestamp64[12];
    extern const char Interval64[11];
}

class TDate : public IIntegerTypeWithKeyString<ui16, NTypeIds::Date, NNames::Date> {};
class TDatetime : public IIntegerTypeWithKeyString<ui32, NTypeIds::Datetime, NNames::Datetime> {};
class TTimestamp : public IIntegerTypeWithKeyString<ui64, NTypeIds::Timestamp, NNames::Timestamp> {};
class TInterval : public IIntegerTypeWithKeyString<i64, NTypeIds::Interval, NNames::Interval> {};

class TDate32 : public IIntegerTypeWithKeyString<i32, NTypeIds::Date32, NNames::Date32> {};
class TDatetime64 : public IIntegerTypeWithKeyString<i64, NTypeIds::Datetime64, NNames::Datetime64> {};
class TTimestamp64 : public IIntegerTypeWithKeyString<i64, NTypeIds::Timestamp64, NNames::Timestamp64> {};
class TInterval64 : public IIntegerTypeWithKeyString<i64, NTypeIds::Interval64, NNames::Interval64> {};

#define KIKIMR_FOREACH_MINIKQL_TYPE_I(name, size, macro, ...) macro(name, T##name, __VA_ARGS__)

#define KIKIMR_FOREACH_MINIKQL_TYPE(xx, ...) \
    xx(Int8, TInt8, __VA_ARGS__) \
    xx(Uint8, TUint8, __VA_ARGS__) \
    xx(Int16, TInt16, __VA_ARGS__) \
    xx(Uint16, TUint16, __VA_ARGS__) \
    xx(Int32, TInt32, __VA_ARGS__) \
    xx(Uint32, TUint32, __VA_ARGS__) \
    xx(Int64, TInt64, __VA_ARGS__) \
    xx(Uint64, TUint64, __VA_ARGS__) \
    xx(Bool, TBool, __VA_ARGS__) \
    xx(Double, TDouble, __VA_ARGS__) \
    xx(Float, TFloat, __VA_ARGS__) \
    xx(PairUi64Ui64, TPairUi64Ui64, __VA_ARGS__) \
    xx(String, TString, __VA_ARGS__) \
    xx(String4k, TSmallBoundedString, __VA_ARGS__) \
    xx(String2m, TLargeBoundedString, __VA_ARGS__) \
    xx(Utf8, TUtf8, __VA_ARGS__) \
    xx(Yson, TYson, __VA_ARGS__) \
    xx(Json, TJson, __VA_ARGS__) \
    xx(JsonDocument, TJsonDocument, __VA_ARGS__) \
    xx(Decimal, TDecimal, __VA_ARGS__) \
    xx(Date, TDate, __VA_ARGS__) \
    xx(Datetime, TDatetime, __VA_ARGS__) \
    xx(Timestamp, TTimestamp, __VA_ARGS__) \
    xx(Interval, TInterval, __VA_ARGS__) \
    xx(DyNumber, TDyNumber, __VA_ARGS__) \
    xx(Uuid, TUuid, __VA_ARGS__) \
    xx(Date32, TDate32, __VA_ARGS__) \
    xx(Datetime64, TDatetime64, __VA_ARGS__) \
    xx(Timestamp64, TTimestamp64, __VA_ARGS__) \
    xx(Interval64, TInterval64, __VA_ARGS__) \
    /**/


static inline bool IsValidMinikqlTypeId(TTypeId typeId) {
    #define check_valid(v, t, ...) if (NTypeIds::v == typeId) return true;
    KIKIMR_FOREACH_MINIKQL_TYPE(check_valid)
    #undef check_valid
    return false;
}

static inline ::TString GetTypeName(TTypeId typeId) {
    #define getTypeName(v, t, ...) if (NTypeIds::v == typeId) return t::TypeName();
    KIKIMR_FOREACH_MINIKQL_TYPE(getTypeName)
    #undef getTypeName
    return ::TString("UnknownType(" + ToString(typeId) + ")");
}

static inline bool TryGetTypeName(TTypeId typeId, ::TString& typeName) {
    #define getTypeName(v, t, ...) if (NTypeIds::v == typeId) { typeName = t::TypeName(); return true; }
    KIKIMR_FOREACH_MINIKQL_TYPE(getTypeName)
    #undef getTypeName
    return false;
}

} // namespace NScheme
} // namespace NKikimr
