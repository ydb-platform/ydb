#pragma once

#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

#define BLOG_ENSURE(condition) do{if(!condition) LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::LOCAL_PGWIRE, #condition);}while(false)
