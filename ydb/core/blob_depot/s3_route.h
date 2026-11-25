#pragma once

#include <ydb/core/protos/blob_depot_config.pb.h>
#include <ydb/core/protos/s3_settings.pb.h>

namespace NKikimr::NBlobDepot {

    // Keep unknown and newly introduced fields in the comparison until their routing semantics are known.
    inline NKikimrBlobDepot::TS3BackendSettings GetS3Route(NKikimrBlobDepot::TS3BackendSettings route) {
        route.ClearSyncMode();
        route.ClearAsyncMode();
        route.ClearBalancerRefreshSecMin();
        route.ClearBalancerRefreshSecMax();
        route.ClearMetricsPushIntervalMs();
        route.ClearMaxRetiringWrappers();
        route.ClearMinRetireGracePeriodSec();
        if (route.GetBalancerHost().empty()) {
            route.ClearBalancerHost();
            route.ClearBalancerProxyScheme();
            route.ClearBalancerProxyPort();
        } else {
            // Proto2 equality distinguishes an absent field from its explicit default. Both route identically.
            route.SetBalancerProxyScheme(route.GetBalancerProxyScheme());
            route.SetBalancerProxyPort(route.GetBalancerProxyPort());
        }
        auto& settings = *route.MutableSettings();
        // The bucket and physical key prefix are checked separately.
        settings.ClearBucket();
        settings.ClearObjectKeyPattern();
        settings.ClearAccessKey();
        settings.ClearSecretKey();
        settings.ClearSecretableAccessKey();
        settings.ClearSecretableSecretKey();
        settings.ClearVerifySSL();
        settings.ClearStorageClass();
        settings.ClearExportDataSettings();
        settings.ClearLimits();
        settings.ClearRequestTimeoutMs();
        settings.ClearHttpRequestTimeoutMs();
        settings.ClearConnectionTimeoutMs();
        settings.ClearExecutorThreadsCount();
        settings.ClearMaxConnectionsCount();
        if (settings.GetEndpoint().empty()) {
            settings.ClearEndpoint();
        }
        // Compare effective transport defaults while preserving Region and ProxyHost presence for the SDK.
        settings.SetScheme(settings.GetScheme());
        settings.SetUseVirtualAddressing(settings.GetUseVirtualAddressing());
        if (settings.HasProxyHost()) {
            settings.SetProxyPort(settings.GetProxyPort());
            settings.SetProxyScheme(settings.GetProxyScheme());
        } else {
            settings.ClearProxyPort();
            settings.ClearProxyScheme();
        }
        return route;
    }

} // NKikimr::NBlobDepot
