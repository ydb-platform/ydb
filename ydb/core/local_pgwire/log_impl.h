#pragma once

#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

#define BLOG_ENSURE(condition) do{if(!condition) YDB_LOG_COMP_ERROR(NKikimrServices::LOCAL_PGWIRE, #condition);}while(false)
