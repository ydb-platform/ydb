#pragma once

#include "scheme_type_desc.h"

#include <ydb/public/lib/scheme_types/scheme_type_id.h>

namespace NKikimr::NScheme {

struct TTypeDesc;

class TTypeInfo {
public:
    constexpr TTypeInfo()
    {}

    explicit constexpr TTypeInfo(TTypeId typeId, const TTypeDesc* typeDesc = {})
        : TypeId(typeId)
        , TypeDesc(typeDesc)
    {
        if (TypeId != NTypeIds::Pg) {
            Y_ABORT_UNLESS(!TypeDesc);
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
    const TTypeDesc* TypeDesc = {};
};

} // namespace NKikimr::NScheme

