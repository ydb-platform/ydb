#pragma once

#include <util/system/types.h>

namespace NKikimr {

union TBasicKikimrServicesMask {
    struct {
        bool EnableBasicServices:1;
        bool EnableIcbService:1;
        bool EnableWhiteBoard:1;
        bool EnableBSNodeWarden:1;
        bool EnableStateStorageService:1;
        bool EnableLocalService:1;
        bool EnableSharedCache:1;
        bool EnableBlobCache:1;
        bool EnableLogger:1;
        bool EnableSchedulerActor:1;
        bool EnableProfiler:1;
        bool EnableResourceBroker:1;
        bool EnableTabletResolver:1;
        bool EnableTabletMonitoringProxy:1;
        bool EnableTabletCountersAggregator:1;
        bool EnableRestartsCountPublisher:1;
        bool EnableBootstrapper:1;
        bool EnableMediatorTimeCastProxy:1;
        bool EnableTxProxy:1;
        bool EnableMiniKQLCompileService:1;
        bool EnableMessageBusServices:1;
        bool EnableStatsCollector:1;
        bool EnableSelfPing:1;
        bool EnableTabletMonitor:1;
        bool EnableViewerService:1;
        bool EnableLoadService:1;
        bool EnableFailureInjectionService:1;
        bool EnablePersQueueL2Cache:1;
        bool EnableKqp:1;
        bool EnableMemoryLog:1;
        bool EnableGRpcService:1;
        bool UNUSED_EnableNodeIdentifier:1;
        bool EnableCms:1;
        bool EnableNodeTable:1;
        bool EnableGRpcProxyStatus:1;
        bool EnablePQ:1;
        bool EnableSqs:1;
        bool EnableConfigsDispatcher:1;
        bool EnableSecurityServices:1;
        bool EnableTabletInfo:1;
        bool EnableQuoterService:1;
        bool EnablePersQueueClusterDiscovery:1;
        bool EnableNetClassifier:1;
        bool EnablePersQueueClusterTracker:1;
        bool EnablePersQueueDirectReadCache:1;
        bool EnableSysViewService:1;
        bool EnableMeteringWriter:1;
        bool EnableAuditWriter:1;
        bool EnableSchemeBoardMonitoring:1;
        bool EnableConfigsCache:1;
        bool EnableLongTxService:1;
        bool EnableHealthCheckService:1;
        bool EnableYandexQuery:1;
        bool EnableSequenceProxyService:1;
        bool EnableHttpProxy:1;
        bool EnableMetadataProvider:1;
        bool EnableReplicationService:1;
        bool EnableBackgroundTasks:1;
        bool EnableExternalIndex: 1;
        bool EnableScanConveyor : 1;
        bool EnableCompConveyor : 1;
        bool EnableInsertConveyor : 1;
        bool EnableLocalPgWire:1;
        bool EnableKafkaProxy:1;
        bool EnableIcNodeCacheService:1;
        bool EnableMemoryTracker:1;

        // next 64 flags

        bool EnableDatabaseMetadataCache:1;
        bool EnableGraphService:1;
        bool EnableCompDiskLimiter:1;
        bool EnableGroupedMemoryLimiter:1;
    };

    struct {
        ui64 Raw1;
        ui64 Raw2;
    };

    void DisableAll() {
        Raw1 = 0;
        Raw2 = 0;
    }

    void EnableAll() {
        Raw1 = 0xFFFFFFFFFFFFFFFFLL;
        Raw2 = 0xFFFFFFFFFFFFFFFFLL;
    }

    void EnableYQ() {
        EnableBasicServices = true;
        EnableLogger = true;
        EnableSchedulerActor = true;
        EnableStatsCollector = true;
        EnableSelfPing = true;
        EnableMemoryLog = true;
        EnableGRpcService = true;
        EnableSecurityServices = true;
        EnableYandexQuery = true;
        EnableViewerService = true;
        EnableMeteringWriter = true;
        EnableProfiler = true;
    }

    void SetTinyMode() {
        EnableSelfPing = false;
        EnableMemoryTracker = false;

        // TODO: set EnableStatsCollector to false as well
    }

    TBasicKikimrServicesMask() {
        EnableAll();
    }
};

static_assert(sizeof(TBasicKikimrServicesMask) == 16, "expected sizeof(TBasicKikimrServicesMask) == 16");

}
