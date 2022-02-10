#pragma once

#include "scheme_types_defs.h"

#include <ydb/core/scheme_types/scheme_type_registry.h>


namespace NKikimr {
namespace NScheme {

class TKikimrTypeRegistry: public TTypeRegistry
{
public:
    TKikimrTypeRegistry() {
        RegisterType<TActorId>();
        RegisterType<TStepOrderId>();
    }
};

} // namespace NScheme
} // namespace NKikimr
