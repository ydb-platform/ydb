#pragma once
#include <ydb/library/actors/core/log.h>

#define BLOG_CRIT(stream) LOG_CRIT_S(*TlsActivationContext, NKikimrServices::HEALTH, stream)
#define BLOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::HEALTH, stream)
