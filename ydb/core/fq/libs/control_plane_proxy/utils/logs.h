#pragma once

#include <ydb/core/fq/libs/actors/logging/log.h>

#define CPP_LOG_D(s) \
    LOG_YQ_CONTROL_PLANE_PROXY_DEBUG(s)
#define CPP_LOG_I(s) \
    LOG_YQ_CONTROL_PLANE_PROXY_INFO(s)
#define CPP_LOG_W(s) \
    LOG_YQ_CONTROL_PLANE_PROXY_WARN(s)
#define CPP_LOG_E(s) \
    LOG_YQ_CONTROL_PLANE_PROXY_ERROR(s)
#define CPP_LOG_T(s) \
    LOG_YQ_CONTROL_PLANE_PROXY_TRACE(s)
