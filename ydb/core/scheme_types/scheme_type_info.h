#pragma once


#include <ydb/public/lib/scheme_types/scheme_type_id.h>

namespace NKikimr::NScheme {

struct TTypeDesc;

class TTypeInfo {
public:
    constexpr TTypeInfo()
    {}

    constexpr TTypeInfo(TTypeId typeId)
        : TypeId(typeId)
    { }

    constexpr TTypeInfo(TTypeId typeId, const void* typeDesc)
        : TypeId(typeId)
        , TypeDesc(static_cast<const TTypeDesc *>(typeDesc))
    {
        if (TypeId != NTypeIds::Pg) {
            Y_ABORT_UNLESS(!TypeDesc);
        } else {
            Y_ABORT_UNLESS(TypeDesc);
        }
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

    const TTypeDesc* GetTypeDesc() const {
        return TypeDesc;
    }

private:
    TTypeId TypeId = 0;
    const TTypeDesc* TypeDesc = nullptr;
};

} // namespace NKikimr::NScheme

