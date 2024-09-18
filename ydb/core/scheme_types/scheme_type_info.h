#pragma once

#include "scheme_decimal_type.h"

#include <ydb/public/lib/scheme_types/scheme_type_id.h>

namespace NKikimr::NPg {
struct ITypeDesc;
TString TypeModFromPgTypeName(const TStringBuf name);
}

namespace NKikimr::NScheme {

class TTypeInfo {
public:
    typedef uintptr_t TTypeDesc;

    constexpr TTypeInfo()
        : TypeId(0)
        , RawDesc(0)
    {}
    
    constexpr TTypeInfo(const TTypeInfo& typeInfo)
        : TypeId(typeInfo.TypeId)
        , RawDesc(typeInfo.RawDesc)
    {}

    constexpr TTypeInfo(TTypeId typeId)
        : TypeId(typeId)
        // TODO Remove after parametrized decimal in KQP
        , DecimalTypeDesc(typeId == NTypeIds::Decimal ? TDecimalType(DECIMAL_PRECISION, DECIMAL_SCALE) : TDecimalType(0, 0))
    {
        // TODO Uncomment after parametrized decimal in KQP
        //Y_ABORT(TypeId != NTypeIds::Decimal)
    }

    constexpr TTypeInfo(TTypeId typeId, const TTypeDesc typeDesc)
        : TypeId(typeId)
        , RawDesc(typeDesc)
    {
        if (NTypeIds::IsParametrizedType(TypeId)) {
            Y_ABORT_UNLESS(RawDesc);
        } else {
            Y_ABORT_UNLESS(!RawDesc);
        }
    }

    constexpr TTypeInfo(TTypeId typeId, const NKikimr::NPg::ITypeDesc* typeDesc)
        : TypeId(typeId)
        , PgTypeDesc(typeDesc)
    {
        Y_ABORT_UNLESS(TypeId == NTypeIds::Pg);
    }

    constexpr TTypeInfo(TTypeId typeId, const TDecimalType& decimalType)
        : TypeId(typeId)
        // TODO Remove after parametrized decimal in KQP
        , DecimalTypeDesc(decimalType.GetPrecision() == 0 && decimalType.GetScale() == 0 ? TDecimalType(DECIMAL_PRECISION, DECIMAL_SCALE) : decimalType)
    {
        Y_ABORT_UNLESS(TypeId == NTypeIds::Decimal);
    }

    constexpr bool operator==(const TTypeInfo& other) const {
        return TypeId == other.TypeId && RawDesc == other.RawDesc;
    }

    constexpr bool operator!=(const TTypeInfo& other) const {
        return !operator==(other);
    }

    constexpr TTypeInfo& operator=(const TTypeInfo& other) {
        TypeId = other.TypeId;
        RawDesc = other.RawDesc;
        return *this;
    }

    constexpr TTypeId GetTypeId() const {
        return TypeId;
    }

    constexpr TTypeDesc GetTypeDesc() const {
        return RawDesc;
    }

    constexpr const NKikimr::NPg::ITypeDesc* GetPgTypeDesc() const {
        Y_ABORT_UNLESS (TypeId == NTypeIds::Pg);
        return PgTypeDesc;
    }

    const TString GetPgTypeMod(const TStringBuf name) const {
        return TypeId != NTypeIds::Pg ? TString{} : NPg::TypeModFromPgTypeName(name);
    }

    constexpr const TDecimalType GetDecimalType() const {
        Y_ABORT_UNLESS (TypeId == NTypeIds::Decimal);
        return DecimalTypeDesc;
    }

private:
    TTypeId TypeId = 0;

    // Storage for parameters of types
    union {
        TTypeDesc RawDesc;                          // internal descriptor, used for passing inside YDB Core
        const NKikimr::NPg::ITypeDesc* PgTypeDesc;  // PG descriptor, used for pg_wrapper
        TDecimalType DecimalTypeDesc;               // Decimal parameters, stored inplace
    };

    static_assert(sizeof(TDecimalType) == sizeof(TTypeDesc));
};

} // namespace NKikimr::NScheme

