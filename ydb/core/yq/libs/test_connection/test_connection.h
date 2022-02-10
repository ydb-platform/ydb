#pragma once 
 
#include <ydb/core/yq/libs/actors/logging/log.h>
#include <ydb/core/yq/libs/config/protos/test_connection.pb.h> 
 
#include <library/cpp/actors/core/actor.h> 
#include <library/cpp/monlib/dynamic_counters/counters.h> 
 
 
#define TC_LOG_D(s) \ 
    LOG_YQ_TEST_CONNECTION_DEBUG(s) 
#define TC_LOG_I(s) \ 
    LOG_YQ_TEST_CONNECTION_INFO(s) 
#define TC_LOG_W(s) \ 
    LOG_YQ_TEST_CONNECTION_WARN(s) 
#define TC_LOG_E(s) \ 
    LOG_YQ_TEST_CONNECTION_ERROR(s) 
#define TC_LOG_T(s) \ 
    LOG_YQ_TEST_CONNECTION_TRACE(s) 
 
namespace NYq { 
 
NActors::TActorId TestConnectionActorId(); 
 
NActors::IActor* CreateTestConnectionActor(const NConfig::TTestConnectionConfig& config, const NMonitoring::TDynamicCounterPtr& counters); 
 
} // namespace NYq 
