#pragma once

#include <ydb/public/lib/scheme_types/scheme_type_id.h>

namespace NKikimr::NPg {

struct ITypeDesc;

}

namespace NKikimr::NScheme {

struct ITypeDesc;

class TTypeInfo {
public:
    constexpr TTypeInfo()
    {}

    constexpr TTypeInfo(TTypeId typeId)
        : TypeId(typeId)
    { }

    constexpr TTypeInfo(TTypeId typeId, const ITypeDesc* typeDesc)
        : TypeId(typeId)
        , TypeDesc(typeDesc)
    {
        if (TypeId != NTypeIds::Pg) {
            Y_ABORT_UNLESS(!TypeDesc);
        }
    }

    TTypeInfo(TTypeId typeId, const NPg::ITypeDesc* typeDesc)
        : TypeId(typeId)
        , TypeDesc(reinterpret_cast<const ITypeDesc *>(typeDesc))
    {
        Y_ABORT_UNLESS (TypeId == NTypeIds::Pg);
    }

    bool operator==(const TTypeInfo& other) const {
        return TypeId == other.TypeId && TypeDesc == other.TypeDesc;
    }

    bool operator!=(const TTypeInfo& other) const {
        return !operator==(other);
    }

    TTypeId GetTypeId() const {
        return TypeId;
    }

    const ITypeDesc* GetTypeDesc() const {
        return TypeDesc;
    }

    const NPg::ITypeDesc* GetPgTypeDesc() const {
        return reinterpret_cast<const NPg::ITypeDesc*>(TypeDesc);
    }

private:
    TTypeId TypeId = 0;
    const ITypeDesc* TypeDesc = nullptr;
};

} // namespace NKikimr::NScheme

