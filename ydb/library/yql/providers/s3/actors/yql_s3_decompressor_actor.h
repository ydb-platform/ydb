#pragma once

#include <ydb/library/actors/core/actor.h>

namespace NYql::NDq {

NActors::IActor* CreateS3DecompressorActor(const NActors::TActorId& parent, const TString& compression);

} // namespace NYql::NDq
