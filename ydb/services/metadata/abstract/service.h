#pragma once

#include <ydb/library/actors/core/actorsystem_fwd.h>

#include <util/system/types.h>

namespace NKikimr::NMetadata::NProvider {

NActors::TActorId MakeServiceId(const ui32 node);

} // namespace NKikimr::NMetadata::NProvider
