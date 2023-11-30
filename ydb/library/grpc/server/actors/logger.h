#pragma once

#include <library/cpp/actors/core/actorsystem.h>
#include <library/cpp/actors/core/log.h>
#include <ydb/library/grpc/server/logger.h>

namespace NYdbGrpc {

TLoggerPtr CreateActorSystemLogger(NActors::TActorSystem& as, NActors::NLog::EComponent component);

} // namespace NYdbGrpc
