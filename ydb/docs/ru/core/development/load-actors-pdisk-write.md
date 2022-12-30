# PDiskWriteLoad

Подает на PDisk нагрузку write-only. Имитирует VDisk. Актор создает на указанном PDisk чанки и записывает в них случайные данные с указанными параметрами. Результатом теста является производительность записи в байтах в секунду.

{% include notitle [addition](../_includes/addition.md) %}

## Спецификация актора {#proto}

<!-- 
```proto
enum ELogMode {
    LOG_PARALLEL = 1;
    LOG_SEQUENTIAL = 2;
    LOG_NONE = 3;
}
message TPDiskWriteLoad {
    message TChunkInfo {
        optional uint32 Slots = 1; // number of slots per chunk
        optional uint32 Weight = 2; // probability weight
    }
    optional uint64 Tag = 1;
    optional uint32 PDiskId = 2;
    optional uint64 PDiskGuid = 3;
    optional NKikimrBlobStorage.TVDiskID VDiskId = 4;
    repeated TChunkInfo Chunks = 5;
    optional uint32 DurationSeconds = 6;
    optional uint32 InFlightWrites = 7;
    optional ELogMode LogMode = 8;
    optional bool Sequential = 9 [default = true];
    optional uint32 IntervalMsMin = 10;
    optional uint32 IntervalMsMax = 11;
    optional bool Reuse = 12 [default = false];
    optional bool IsWardenlessTest = 13 [default = false];
}
```
-->

```proto
enum ELogMode {
    LOG_PARALLEL = 1; // Писать в чанк и писать в лог (о факте записи) параллельно, и считать запись завершенной только если обе записи прошли
    LOG_SEQUENTIAL = 2; // Писать сначала в чанк, потом в лог. Считать запись завершенной после записи в лог
    LOG_NONE = 3;
}

message TPDiskWriteLoad {
    message TChunkInfo {
        optional uint32 Slots = 1; // количество слотов в чанке. Фактически определяет размер записей/чтений,
                                   // которой можно расчитать, разделив размер чанка на количество слотов
        optional uint32 Weight = 2; // вес, с которым запись будет осуществляться именно в этот чанк
    }
    optional uint64 Tag = 1; // необязательный. Если не задан, то тег будет присвоен автоматически
    optional uint32 PDiskId = 2; // обязательный. Id, на который нужно подать нагрузку. Можно узнать из UI кластера
    optional uint64 PDiskGuid = 3; // обязательный. Guid того PDisk, на который подается нагрузка. Можно узнать на UI странице PDisk
    optional NKikimrBlobStorage.TVDiskID VDiskId = 4; // обязательный. Этим VDiskId нагружающий актор представится PDisk.
                                                    // Не должен дублироваться между разными нагружающими акторами.
    repeated TChunkInfo Chunks = 5; // обязательный. Описание того, сколько чанков использовать для подачи нагрузки. Позволяет варьировать нагрузку.
                                    // К примеру, можно сделать два чанка - один с большим количеством слотов и один с маленьким и задать веса чанков
                                    // таким образом, чтобы 95% записей были небольшого размера, а 5% -- большого, имитируя тем самым compaction VDisk
    optional uint32 DurationSeconds = 6; // обязательный. Длительность теста
    optional uint32 InFlightWrites = 7; // обязательный. Ограничивает количество запросов в полете к PDisk
    optional ELogMode LogMode = 8; // обязательный. См. выше ELogMode
    optional bool Sequential = 9 [default = true]; // необязательный. Писать в слоты чанков записи последовательно или случайно.
    optional uint32 IntervalMsMin = 10; // необязательный. См. ниже
    optional uint32 IntervalMsMax = 11; // необязательный. Позволяет подавать жесткую нагрузку, которая будет отправлять запросы строго регулярно.
                                        // Время между запросами случайно выбирается между [IntervalMsMin, IntervalMsMax].
                                        // Учитывает InFlightWrites и не превышает его. Если не заданы IntervalMsMin и IntervalMsMax, то учитывается
                                        // только InFlightWrites
    optional bool Reuse = 12 [default = false]; // должен ли актор переиспользовать полностью записанные чанки или должен выделять
                                                // новые чанки и освобождать старые чанки
    optional bool IsWardenlessTest = 13 [default = false]; // позволяет использовать в тестах, где нет NodeWarden
}
```
