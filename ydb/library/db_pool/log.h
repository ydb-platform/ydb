#pragma once

#include <ydb/library/services/services.pb.h>

#include <ydb/library/actors/core/log.h>

#define LOG_IMPL_AS(actorSystem, level, component, logRecordStream) \
    LOG_LOG_S(actorSystem, ::NActors::NLog:: Y_CAT(PRI_, level), ::NKikimrServices::component, logRecordStream);

#define LOG_F_AS(actorSystem, logRecordStream) LOG_IMPL_AS(*actorSystem, EMERG, DB_POOL, logRecordStream)
#define LOG_A_AS(actorSystem, logRecordStream) LOG_IMPL_AS(*actorSystem, ALERT, DB_POOL, logRecordStream)
#define LOG_C_AS(actorSystem, logRecordStream) LOG_IMPL_AS(*actorSystem, CRIT, DB_POOL, logRecordStream)
#define LOG_E_AS(actorSystem, logRecordStream) LOG_IMPL_AS(*actorSystem, ERROR, DB_POOL, logRecordStream)
#define LOG_W_AS(actorSystem, logRecordStream) LOG_IMPL_AS(*actorSystem, WARN, DB_POOL, logRecordStream)
#define LOG_N_AS(actorSystem, logRecordStream) LOG_IMPL_AS(*actorSystem, NOTICE, DB_POOL, logRecordStream)
#define LOG_I_AS(actorSystem, logRecordStream) LOG_IMPL_AS(*actorSystem, INFO, DB_POOL, logRecordStream)
#define LOG_D_AS(actorSystem, logRecordStream) LOG_IMPL_AS(*actorSystem, DEBUG, DB_POOL, logRecordStream)
#define LOG_T_AS(actorSystem, logRecordStream) LOG_IMPL_AS(*actorSystem, TRACE, DB_POOL, logRecordStream)

#define LOG_F(logRecordStream) LOG_F_AS(::NActors::TActivationContext::ActorSystem(), logRecordStream)
#define LOG_A(logRecordStream) LOG_A_AS(::NActors::TActivationContext::ActorSystem(), logRecordStream)
#define LOG_C(logRecordStream) LOG_C_AS(::NActors::TActivationContext::ActorSystem(), logRecordStream)
#define LOG_E(logRecordStream) LOG_E_AS(::NActors::TActivationContext::ActorSystem(), logRecordStream)
#define LOG_W(logRecordStream) LOG_W_AS(::NActors::TActivationContext::ActorSystem(), logRecordStream)
#define LOG_N(logRecordStream) LOG_N_AS(::NActors::TActivationContext::ActorSystem(), logRecordStream)
#define LOG_I(logRecordStream) LOG_I_AS(::NActors::TActivationContext::ActorSystem(), logRecordStream)
#define LOG_D(logRecordStream) LOG_D_AS(::NActors::TActivationContext::ActorSystem(), logRecordStream)
#define LOG_T(logRecordStream) LOG_T_AS(::NActors::TActivationContext::ActorSystem(), logRecordStream)
