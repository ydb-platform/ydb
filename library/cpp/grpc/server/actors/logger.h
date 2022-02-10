#pragma once 
 
#include <library/cpp/actors/core/actorsystem.h> 
#include <library/cpp/actors/core/log.h> 
#include <library/cpp/grpc/server/logger.h> 
 
namespace NGrpc { 
 
TLoggerPtr CreateActorSystemLogger(NActors::TActorSystem& as, NActors::NLog::EComponent component); 
 
} // namespace NGrpc 
