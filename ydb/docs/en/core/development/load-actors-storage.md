# StorageLoad

Loads Distributed Storage without using tablet and Query Processor layers. The test outputs the performance of writes to Distributed Storage in blobs per second.

{% include notitle [addition](../_includes/addition.md) %}

## Actor specification {#proto}

```proto
message TStorageLoad {
    message TRequestInfo {
        optional float SendTime = 1;
        optional uint64 Type = 2;
        optional uint32 Size = 3;
        optional NKikimrBlobStorage.EPutHandleClass PutHandleClass = 4;
    }
    message TTabletInfo {
        optional uint64 TabletId = 1;
        optional uint32 Channel = 2;
        optional uint32 GroupId = 3;
        optional uint32 Generation = 4;
        repeated TRequestInfo Requests = 5;
        optional float ScriptedCycleDurationSec = 6;
    }
    message TPerTabletProfile {
        repeated TTabletInfo Tablets = 1;
        repeated TSizeInfo Sizes = 2;
        repeated TIntervalInfo WriteIntervals = 3;
        optional uint32 MaxInFlightRequests = 4;
        optional uint32 MaxInFlightBytes = 5;
        repeated TIntervalInfo FlushIntervals = 6;
        optional NKikimrBlobStorage.EPutHandleClass PutHandleClass = 7;
        optional bool Soft = 8;
        optional uint32 MaxInFlightReadRequests = 9;
        optional uint32 MaxInFlightReadBytes = 10;
        repeated TIntervalInfo ReadIntervals = 11;
        repeated TSizeInfo ReadSizes = 12;
        optional uint64 MaxTotalBytesWritten = 13;
        optional NKikimrBlobStorage.EGetHandleClass GetHandleClass = 14;
    };
    optional uint64 Tag = 1;
    optional uint32 DurationSeconds = 2;
    optional bool RequestTracking = 3 [default = false];
    repeated TPerTabletProfile Tablets = 4;
    optional uint64 ScheduleThresholdUs = 5;
    optional uint64 ScheduleRoundingUs = 6;
}
```
<!--
## Примеры {#example}

**Читающая нагрузка**

Нагрузка пишет в группу `$GROUPID`, состоит из двух частей. Первая - пишущая, подает небольшой фон пишущих запросов размера `$SIZE` каждые 50 мс, при этом ограничивает `InFlight` 1. То есть если запрос не успевает завершиться, то актор будет ждать завершения и после этого через 50мс, будет запущен следующий запрос.

Вторая часть основная, читающая. Читает запросами размера `$SIZE`, запросы отправляет каждые `${INTERVAL}` микросекунд. Можно его задать в 0, тогда этот параметр не будет играть роли. Конфигурация ограничивает количество запросов в полете числом `${IN_FLIGHT}`.

{% list tabs %}

- CLI

  ```proto
  NodeId: ${NODEID}
  Event: { StorageLoad: {
      DurationSeconds: ${DURATION}
      ScheduleThresholdUs: 0
      ScheduleRoundingUs: 0
      Tablets: {
          Tablets: { TabletId: ${TABLETID} Channel: 0 GroupId: ${GROUPID} Generation: 1 }
          Sizes: { Weight: 1.0 Min: ${SIZE} Max: ${SIZE} }
          WriteIntervals: { Weight: 1.0 Uniform: { MinUs: 50000 MaxUs: 50000 } }
          MaxInFlightRequests: 1

          ReadSizes: { Weight: 1.0 Min: ${SIZE} Max: ${SIZE} }
          ReadIntervals: { Weight: 1.0 Uniform: { MinUs: ${INTERVAL} MaxUs: ${INTERVAL} } }
          MaxInFlightReadRequests: ${IN_FLIGHT}
          FlushIntervals: { Weight: 1.0 Uniform: { MinUs: 10000000 MaxUs: 10000000 } }
          PutHandleClass: ${PUT_HANDLE_CLASS}
          GetHandleClass: ${GET_HANDLE_CLASS}
          Soft: true
      }
  }}
  ```

{% endlist %}

**Пишущая нагрузка**

Пишет в группу `$GROUPID` нагрузку длительностью `$DURATION` секунд. Пишет размерами `$SIZE`, ограничивая количество запросов в полете числом `$IN_FLIGHT`.

{% list tabs %}

- CLI

  ```proto
  NodeId: ${NODEID}
  Event: { StorageLoad: {
      DurationSeconds: ${DURATION}
      ScheduleThresholdUs: 0
      ScheduleRoundingUs: 0
      Tablets: {
          Tablets: { TabletId: ${TABLETID} Channel: 0 GroupId: ${GROUPID} Generation: 1 }
          Sizes: { Weight: 1.0 Min: ${SIZE} Max: ${SIZE} }
          WriteIntervals: { Weight: 1.0 Uniform: { MinUs: ${INTERVAL} MaxUs: ${INTERVAL} } }
          MaxInFlightRequests: ${IN_FLIGHT}
          FlushIntervals: { Weight: 1.0 Uniform: { MinUs: 10000000 MaxUs: 10000000 } }
          PutHandleClass: ${PUT_HANDLE_CLASS}
          Soft: true
      }
  }}
  ```

{% endlist %}
 -->
