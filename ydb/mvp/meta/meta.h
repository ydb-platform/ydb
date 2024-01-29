#pragma once

#include <ydb/mvp/core/core_ydb.h>

void InitMeta(NActors::TActorSystem& actorSystem, const NActors::TActorId& httpProxyId, const TYdbLocation& location);
