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
        //Y_ABORT_UNLESS(NTypeIds::IsParametrizedType(TypeId))
    }

    constexpr TTypeInfo(const NKikimr::NPg::ITypeDesc* typeDesc)
        : TypeId(NTypeIds::Pg)
        , PgTypeDesc(typeDesc)
    {}

    constexpr TTypeInfo(const TDecimalType& decimalType)
        : TypeId(NTypeIds::Decimal)
        // TODO Remove after parametrized decimal in KQP
        , DecimalTypeDesc(decimalType.GetPrecision() == 0 && decimalType.GetScale() == 0 ? TDecimalType(DECIMAL_PRECISION, DECIMAL_SCALE) : decimalType)
    {}

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
    friend struct TTypeInfoOrder;
    typedef uintptr_t TRawTypeDesc;  

    constexpr TTypeInfo(TTypeId typeId, const TRawTypeDesc typeDesc)
        : TypeId(typeId)
        , RawDesc(typeDesc)
    {}

    TTypeId TypeId = 0;

    // Storage for parameters of types
    union {
        TRawTypeDesc RawDesc;                       // internal descriptor, used for passing inside YDB Core
        const NKikimr::NPg::ITypeDesc* PgTypeDesc;  // PG descriptor, used for pg_wrapper
        TDecimalType DecimalTypeDesc;               // Decimal parameters, stored inplace
    };

    static_assert(sizeof(TDecimalType) == sizeof(TRawTypeDesc));
};

} // namespace NKikimr::NScheme

