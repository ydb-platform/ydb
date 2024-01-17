#pragma once

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/grpc/server/logger.h>

namespace NYdbGrpc {

TLoggerPtr CreateActorSystemLogger(NActors::TActorSystem& as, NActors::NLog::EComponent component);

} // namespace NYdbGrpc
