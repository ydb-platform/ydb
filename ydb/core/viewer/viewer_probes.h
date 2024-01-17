#pragma once
#include <library/cpp/lwtrace/all.h>

#define VIEWER_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES) \
    PROBE(ViewerClusterHandler, \
        GROUPS(), \
        TYPES(ui32, bool, bool, \
            double, double, double, double, \
            double, double, double, double, \
            double, double), \
        NAMES("nodeId", "tabletsParam", "requestTimeout", \
            "startTime", "totalDurationMs", \
            "getListTenantsResponseDurationMs","getNodesInfoDurationMs", \
            "collectingDurationMs", "mergeBSGroupsDurationMs", \
            "mergeVDisksDurationMs", "mergePDisksDurationMs", \
            "mergeTabletsDurationMs", "responseBuildingDurationMs")) \
/**/
LWTRACE_DECLARE_PROVIDER(VIEWER_PROVIDER)
