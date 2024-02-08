#pragma once

#include "public.h"

#include <yt/yt/core/ytree/ypath_service.h>

#include <yt/yt/core/http/public.h>

#include <yt/yt/library/profiling/solomon/public.h>

namespace NYT::NMonitoring {

////////////////////////////////////////////////////////////////////////////////

void Initialize(
    const NHttp::IServerPtr& monitoringServer,
    const NProfiling::TSolomonExporterConfigPtr& solomonExporterConfig,
    TMonitoringManagerPtr* monitoringManager,
    NYTree::IMapNodePtr* orchidRoot);

NHttp::IHttpHandlerPtr CreateTracingHttpHandler();

NHttp::IHttpHandlerPtr GetOrchidYPathHttpHandler(
    const NYTree::IYPathServicePtr& service);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMonitoring
