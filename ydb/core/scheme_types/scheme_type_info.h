#pragma once

#include <ydb/public/lib/scheme_types/scheme_type_id.h>

namespace NKikimr::NScheme {

class TTypeInfo {
public:
    constexpr TTypeInfo()
    {}

    explicit constexpr TTypeInfo(TTypeId typeId, void* typeDesc = {})
        : TypeId(typeId)
        , TypeDesc(typeDesc)
    {
        if (TypeId != NTypeIds::Pg) {
            Y_VERIFY(!TypeDesc);
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

    void* GetTypeDesc() const {
        return TypeDesc;
    }

private:
    TTypeId TypeId = 0;
    void* TypeDesc = {};
};

} // namespace NKikimr::NScheme

