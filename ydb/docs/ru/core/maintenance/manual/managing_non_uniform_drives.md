# Управление кластером с неоднородными дисками

Когда речь заходит о достаточно крупных кластерах, становится важным учитывать неоднородность используемого оборудования — в первую очередь дисков. Помимо различных типов (HDD / SSD / NVME), диски могут различаться по размеру и по производительности. Для эффективной работы кластера с неоднородными дисками в YDB предусмотрен ряд настроек, позволяющих учесть этот фактор:

1. Параметр [PDisk](../../concepts/glossary.md#pdisk) `expected_slot_count` определяет количество [слотов VDisk](../../concepts/glossary.md#slot), между которыми распределяются ресурсы диска — пропускная способность и свободное место.
1. Еще один параметр PDisk `slot_size_in_units` обозначает размер этих слотов в условных единицах.
1. У [групп хранения](../../concepts/glossary.md#storage-group) имеется похожий параметр `GroupSizeInUnits` (числовое значение) — он определяет сколько одинарных слотов занимает каждый из VDisk'ов данной группы. Исходя из количества занимаемых слотов рассчитывается квота VDisk.

## Выбор значений

// TODO

## Изменение параметров PDisk


// TODO

1. Получить текущую конфигурацию кластера:

    ```bash
    ydb -e <endpoint> admin cluster config fetch > config.yaml
    ```

    `<endpoint>` - grpc/grpcs эндпоинт произвольного узла кластера.

2. Добавить (или изменить) поле `expected_slot_count` для нужного устройства `drive` в секции `host_configs`.

    Примерный вид секции конфигурации:

    ```yaml
    config:
    host_configs:
    - host_config_id: 1
        drive:
        - path: <path_to_device>
          type: <type>
          expected_slot_count: <number>
          slot_size_in_units: <number>
        - path: ...
    - host_config_id: 2
        ...
    ```

3. Загрузить обновленный конфигурационный файл на кластер:

    ```bash
    ydb -e endpoint admin cluster config replace -f config.yaml
    ```

См. также:

- [{#T}](../../reference/configuration/index.md)
- [{#T}](balancing_load.md#izmenenie-kolichestva-slotov-dlya-vdiskov-na-pdiskah)
