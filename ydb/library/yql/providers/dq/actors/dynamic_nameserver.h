#pragma once

#include <ydb/library/actors/interconnect/interconnect.h>

namespace NYql::NDqs {
    NActors::IActor* CreateDynamicNameserver(const TIntrusivePtr<NActors::TTableNameserverSetup>& setup, ui32 poolId = 0);
}
