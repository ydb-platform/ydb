#pragma once

#include "scheme_type_id.h"

#include <ydb/library/actors/core/actorid.h>
#include <ydb/core/scheme_types/scheme_types_defs.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/type_desc.h>

#include <util/stream/output.h>


#define KIKIMR_FOREACH_TYPE(xx, ...) \
    KIKIMR_FOREACH_MINIKQL_TYPE(xx, __VA_ARGS__) \
    xx(ActorId, TActorId, __VA_ARGS__) \
    xx(StepOrderId, TStepOrderId, __VA_ARGS__) \
    /**/

namespace NKikimr {

    struct TCell;

namespace NScheme {

////////////////////////////////////////////////////////
/// other internal types
/// 0x2001 - 0x8000
/// DO NOT FORGET TO REGISTER THE TYPES in Library::OpenLibrary() / file tablet_library.h
namespace NNames {
    extern const char ActorID[8];
}

class TActorId : public TTypedType<NActors::TActorId, TActorId, NTypeIds::ActorId, NNames::ActorID>
{
public:
};

namespace NNames {
    extern const char StepOrderId[12];
}

class TStepOrderId : public IIntegerPair<ui64, ui64, NTypeIds::StepOrderId, NNames::StepOrderId> {};

////////////////////////////////////////////////////////
/// user types
/// 0x8001 - 0xFFFF
/// DO NOT FORGET TO REGISTER THE TYPES in Library::OpenLibrary() / file tablet_library.h

// todo: range enum

////////////////////////////////////////////////////////
/// 0x10000 - 0xFFFFFFFF reserved
/// DO NOT FORGET TO REGISTER THE TYPES in Library::OpenLibrary() / file tablet_library.h


////////////////////////////////////////////////////////

inline ui32 GetFixedSize(TTypeId typeId) {
    switch (typeId) {
#define KIKIMR_TYPE_MACRO(typeEnum, typeType, ...) case NTypeIds::typeEnum: return typeType::GetFixedSize();
    KIKIMR_FOREACH_TYPE(KIKIMR_TYPE_MACRO)
#undef KIKIMR_TYPE_MACRO
    default:
        return 0;
    }
}

inline ui32 GetFixedSize(const TTypeInfo& typeInfo) {
    switch (typeInfo.GetTypeId()) {
#define KIKIMR_TYPE_MACRO(typeEnum, typeType, ...) case NTypeIds::typeEnum: return typeType::GetFixedSize();
    KIKIMR_FOREACH_TYPE(KIKIMR_TYPE_MACRO)
#undef KIKIMR_TYPE_MACRO
    case NTypeIds::Pg:
        return NPg::TypeDescGetStoredSize(typeInfo.GetPgTypeDesc());
    default:
        return 0;
    }
}

/**
 * Checks if the given value matches the expected type size
 *
 * Returns empty string on success or an error description in case of failure
 */
::TString HasUnexpectedValueSize(const ::NKikimr::TRawTypeValue& value);

/**
 * Checks if the given cell/type combination matches the expected type size
 * 
 * Returns empty string on success or an error description in case of failure
 */
::TString HasUnexpectedValueSize(const ::NKikimr::TCell& value, const TTypeInfo& typeInfo);

} // namespace NScheme
} // namespace NKikimr
