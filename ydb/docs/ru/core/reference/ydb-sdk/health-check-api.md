---
title: "Инструкция по инициации сервиса Health Check API в {{ ydb-short-name }}"
description: "Из статьи вы узнаете, как инициировать проверку при помощи встроенной системы самодиагностики Health Check API в {{ ydb-short-name }}."
---

# Health Check API

{{ ydb-short-name }} имеет встроенную систему самодиагностики, с помощью которой можно получить краткий отчет о состоянии базы данных и информацию об имеющихся проблемах.

Чтобы инициировать проверку, вызовите метод `SelfCheck` из пространства имен `NYdb::NMonitoring` в SDK. Также необходимо передать имя проверяемой БД стандартным способом.

{% list tabs %}

- C++

  Пример кода приложения для создания клиента:
  ```cpp
  auto client = NYdb::NMonitoring::TMonitoringClient(driver);
  ```

  Вызов метода `SelfCheck`:

  ```
  auto settings = TSelfCheckSettings();
  settings.ReturnVerboseStatus(true);
  auto result = client.SelfCheck(settings).GetValueSync();
  ```

{% endlist %}

## Параметры вызова {#call-parameters}

`SelfCheck` возвращает информацию в форме [списка проблем](#emergency-example), каждая из которых может выглядеть так:

```json
{
  "id": "RED-27c3-70fb",
  "status": "RED",
  "message": "Database has multiple issues",
  "location": {
    "database": {
      "name": "/slice"
    }
  },
  "reason": [
    "RED-27c3-4e47",
    "RED-27c3-53b5",
    "YELLOW-27c3-5321"
  ],
  "type": "DATABASE",
  "level": 1
}
```

Это короткое сообщение об одной из проблем. Все параметры вызова влиять на размер информации , которая содержится в ответе.

Полный список дополнительных параметров представлен ниже:

{% list tabs %}

- C++

  ```c++
  struct TSelfCheckSettings : public TOperationRequestSettings<TSelfCheckSettings>{
      FLUENT_SETTING_OPTIONAL(bool, ReturnVerboseStatus);
      FLUENT_SETTING_OPTIONAL(EStatusFlag, MinimumStatus);
      FLUENT_SETTING_OPTIONAL(ui32, MaximumLevel);
  };
  ```

{% endlist %}

| Поле | Тип | Описание |
|:----|:----|:----|
| `ReturnVerboseStatus` | `bool`         | Если задан, ответ также будет содержать сводку общего состояния базы данных в поле `database_status` ([Example](#example-verbose)). По умолчанию `false`. |
| `MinimumStatus`       | [EStatusFlag] (#issue-status) | Каждая проблема содержит поле `status`. Если `minimum_status` определен, проблемы с менее серьезным статусом будут отброшены. По-умолчанию все проблемы будут перечислены. |
| `MaximumLevel`        | `int32`        | Каждая проблема содержит поле `level`. Если `maximum_level` определенd, более глубоки проблемы будут отброшены. По-умолчанию все проблемы будут перечислены. |

## Структура ответа {#response-structure}

Полную структуру ответа можно посмотреть в файле [ydb_monitoring.proto](https://github.com/ydb-platform/ydb/public/api/protos/ydb_monitoring.proto) в {{ ydb-short-name }} Git репозитории.
В результате вызова этого метода будет возвращена следующая структура:

```protobuf
message SelfCheckResult {
    SelfCheck.Result self_check_result = 1;
    repeated IssueLog issue_log = 2;
    repeated DatabaseStatus database_status = 3;
    LocationNode location = 4;
}
```

Если обнаружены проблемы, поле `issue_log` будет содержать описания проблем со следующей структурой:

```protobuf
message IssueLog {
    string id = 1;
    StatusFlag.Status status = 2;
    string message = 3;
    Location location = 4;
    repeated string reason = 5;
    string type = 6;
    uint32 level = 7;
}
```

Эти проблемы можно организовать в иерархию с помощью полей `id` и `reason`, что помогает визуализировать, как проблемы в отдельном модуле влияют на состояние системы в целом. Все проблемы организованы в иерархию, где верхние уровни могут зависеть от вложенных:

![cards_hierarchy](./_assets/hc_cards_hierarchy.png)

Каждая проблема имеет уровень вложенности `level` — чем выше `level`, тем глубже проблема находится в иерархии. Проблемы с одинаковым типов (поле `type`) всегда имеют одинаковый `level`, и их можно представить ввиде иерархии.

![issues_hierarchy](./_assets/hc_types_hierarchy.png)

Описание всех полей в ответе представлено ниже:

| Поле | Описание |
|:----|:----|
| `self_check_result` | Перечисляемое поле, которое содержит [результат проверки базы данных](#selfcheck-result) |
| `issue_log` | Это набор элементов, каждый из которых описывает проблему в системе на определенном уровне. |
| `issue_log.id` | Уникальный идентификатор проблемы в этом ответе. |
| `issue_log.status` | Перечисляемое поле, которое содержит [статус проблемы](#issue-status) |
| `issue_log.message` | Текст, описывающий проблему. |
| `issue_log.location` | Местоположение проблемы. Это может быть физической местоположение или контекст выполнения. |
| `issue_log.reason` | Это набор элементов, каждый из которых описывает причину проблемы в системе на определенном уровне. |
| `issue_log.type` | Категория проблемы. Каждый тип находится на определённом уровне и связан с другими через жёсткую иерархию (как показано на изображении выше). |
| `issue_log.level` | Глубина вложенности проблемы. |
| `database_status` | Если в настройках содержится параметр `verbose`, то поле `database_status` будет заполнено.<br/>Оно предоставляет сводку общего состояния базы данных.<br/>Используется для быстрой оценки состояния базы данных и выявления серьезных проблем на высоком уровне. [Пример](#example-verbose). |
| `location` | Содержит информацию о хосте, на котором был вызван сервис `HealthCheck`. |

### Результат проверки базы данных {#selfcheck-result}

Самый общий статусы базы данных, который может иметь следующие значения:

| Поле | Описание |
|:----|:----|
| `GOOD` | Проблем не обнаружено. |
| `DEGRADED` | Обнаружена деградация одной из систем базы данных, но база данных все еще функционирует (например, допустимая потеря диска). |
| `MAINTENANCE_REQUIRED` | Обнаружена значительная деградация, есть риск потери доступности, требуется обслуживание. |
| `EMERGENCY` | Обнаружена серьезная проблема в базе данных с полной или частичной потерей доступности. |

### Статус проблемы {#issue-status}

Статус (серьезность) текущей проблемы:

| Поле | Описание |
|:----|:----|
| `GREY` |  Обнаружена деградация одной из систем базы данных, но база данных все еще функционирует (например, допустимая потеря диска). |
| `GREEN` | Проблем не обнаружено. |
| `BLUE` | Временная небольшая деградация, не влияющая на доступность базы данных. Ожидается переход системы в `GREEN`. |
| `YELLOW` | Небольшая проблема, нет рисков для доступности. Рекомендуется продолжать мониторинг проблемы. |
| `ORANGE` | Серьезная проблема, мы в шаге от потери доступности. Может потребоваться обслуживание. |
| `RED` | Компонент неисправен или недоступен. |

## Возможные проблемы {#problems}

| Сообщение | Описание |
|:----|:----|
| **DATABASE** ||
| `Database has multiple issues`</br>`Database has compute issues`</br>`Database has storage issues` | Зависит от нижележащих слоев `COMPUTE` и `STORAGE`. Это самый общий статус базы данных. |
| **STORAGE** ||
| `There are no storage pools` | Пулы хранения не настроены. |
| `Storage degraded`</br>`Storage has no redundancy`</br>`Storage failed` | Зависит от нижележащего слоя `STORAGE_POOLS`. |
| `System tablet BSC didn't provide information` | Информация о сторадже не доступна. |
| `Storage usage over 75%` <br>`Storage usage over 85%` <br>`Storage usage over 90%` | Необходимо увеличить дисковое пространство. |
| **STORAGE_POOL** ||
| `Pool degraded` <br>`Pool has no redundancy` <br>`Pool failed` | Зависит от нижележащего слоя `STORAGE_GROUP`. |
| **STORAGE_GROUP** ||
| `Group has no vslots` | Эта ошибка не ожидается. Внутренняя ошибка. |
| `Group degraded` | В группе недоступно допустимое число дисков. |
| `Group has no redundancy` | Группа хранения потеряла избыточность. Еще один сбой в работе диска может привести к потере группы. |
| `Group failed` | Группа хранения потеряла целостность. Данные не доступны. |
|| `HealthCheck` проверяет различные параметры (режим отказоустойчивости, количество отказавших дисков, статус дисков и т. д.) и в зависимости от этого устанавливает соответствующий статус у группы. |
| **VDISK** ||
| `System tablet BSC didn't provide known status` | Эта ошибка не ожидается. Внутренняя ошибка. |
| `VDisk is not available` | Отсутствует виртуальный диск. |
| `VDisk is being initialized` | Инициализация виртуального диска в процессе. |
| `Replication in progress` | Диск в процессе репликации, но может принимать запросы. |
| `VDisk have space issue` | Зависит от нижележащего слоя `PDISK`. |
| **PDISK** ||
| `Unknown PDisk state` | `HealthCheck` не может разобрать состояние PDisk. Внутренняя ошибка. |
| `PDisk state is ...` | Cообщает состояние физического диска. |
| `Available size is less than 12%` <br>`Available size is less than 9%` <br>`Available size is less than 6%` | Заканчивается свободное место на физическом диске. |
| `PDisk is not available` | Отсутствует физический диск. |
| **STORAGE_NODE** ||
| `Storage node is not available` | Отсутствует нода с дисками. |
| **COMPUTE** ||
| `There are no compute nodes` | В базе нет нод для запуска таблеток. </br>Невозможно определить уровень `COMPUTE_NODE` ниже. |
| `Compute has issues with system tablets` | Зависит от нижележащего слоя `SYSTEM_TABLET`. |
| `Some nodes are restarting too often` | Зависит от нижележащего слоя `NODE_UPTIME`. |
| `Compute is overloaded` | Зависит от нижележащего слоя `COMPUTE_POOL`. |
| `Compute quota usage` | Зависит от нижележащего слоя `COMPUTE_QUOTA`. |
| `Compute has issues with tablets` | Зависит от нижележащего слоя `TABLET`. |
| **COMPUTE_QUOTA** ||
| `Paths quota usage is over than 90%` <br>`Paths quota usage is over than 99%` <br>`Paths quota exhausted` </br>`Shards quota usage is over than 90%` <br>`Shards quota usage is over than 99%` <br>`Shards quota exhausted` | Квоты исчерпаны. |
| **SYSTEM_TABLET** ||
| `System tablet is unresponsive ` <br>`System tablet response time over 1000ms` <br>`System tablet response time over 5000ms` | Системная таблетка не отвечает или отвечает долго |
| **TABLET** ||
| `Tablets are restarting too often` | Таблетки слишком часто перезапускаются. |
| `Tablets are dead` <br>`Followers are dead` | Таблетки не запущены (или не могут быть запущены). |
| **LOAD_AVERAGE** ||
| `LoadAverage above 100%` | ([Load](https://en.wikipedia.org/wiki/Load_(computing))) Физический хост перегружен. </br>Это указывает на то, что система работает на пределе, скорее всего из-за большого количества процессов, ожидающих операций ввода-вывода. </br></br>Информация о нагрузке: </br>Источник: </br>`/proc/loadavg` </br>Информация о логических ядрах </br></br>Количество логических ядер: </br>Основной источник: </br>`/sys/fs/cgroup/cpu.max` </br></br>Дополнительный источник: </br>`/sys/fs/cgroup/cpu/cpu.cfs_quota_us` </br>`/sys/fs/cgroup/cpu/cpu.cfs_period_us` </br>Количество ядер вычисляется путем деления квоты на период (quota / period) |
| **COMPUTE_POOL** ||
| `Pool usage is over than 90%` <br>`Pool usage is over than 95%` <br>`Pool usage is over than 99%` | один из CPU пулов перегружен. |
| **NODE_UPTIME** ||
| `The number of node restarts has increased` | Количество рестартов ноды превысило порог. По-умолчанию, это 10 рестартов в час. |
| `Node is restarting too often` | Узлы слишком часто перезапускаются. По-умолчанию, это 30 рестартов в час. |
| **NODES_TIME_DIFFERENCE** ||
| `Node is ... ms behind peer [id]` <br>`Node is ... ms ahead of peer [id]` | Расхождение времени на узлах, что может приводить к возможным проблемам с координацией распределённых транзакций. Начинает появляться с расхождения в 5ms|

## Пример ответа {#examples}

Самый короткий ответ сервиса будет выглядеть следующим образом. Он возвращается, если с базой данных все в порядке:

```json
{
  "self_check_result": "GOOD"
}
```

#### Пример verbose {#example-verbose}

Ответ `GOOD` при использовании параметра `verbose`:
```json
{
    "self_check_result": "GOOD",
    "database_status": [
        {
            "name": "/amy/db",
            "overall": "GREEN",
            "storage": {
                "overall": "GREEN",
                "pools": [
                    {
                        "id": "/amy/db:ssdencrypted",
                        "overall": "GREEN",
                        "groups": [
                            {
                                "id": "2181038132",
                                "overall": "GREEN",
                                "vdisks": [
                                    {
                                        "id": "9-1-1010",
                                        "overall": "GREEN",
                                        "pdisk": {
                                            "id": "9-1",
                                            "overall": "GREEN"
                                        }
                                    },
                                    {
                                        "id": "11-1004-1009",
                                        "overall": "GREEN",
                                        "pdisk": {
                                            "id": "11-1004",
                                            "overall": "GREEN"
                                        }
                                    },
                                    {
                                        "id": "10-1003-1011",
                                        "overall": "GREEN",
                                        "pdisk": {
                                            "id": "10-1003",
                                            "overall": "GREEN"
                                        }
                                    },
                                    {
                                        "id": "8-1005-1010",
                                        "overall": "GREEN",
                                        "pdisk": {
                                            "id": "8-1005",
                                            "overall": "GREEN"
                                        }
                                    },
                                    {
                                        "id": "7-1-1008",
                                        "overall": "GREEN",
                                        "pdisk": {
                                            "id": "7-1",
                                            "overall": "GREEN"
                                        }
                                    },
                                    {
                                        "id": "6-1-1007",
                                        "overall": "GREEN",
                                        "pdisk": {
                                            "id": "6-1",
                                            "overall": "GREEN"
                                        }
                                    },
                                    {
                                        "id": "4-1005-1010",
                                        "overall": "GREEN",
                                        "pdisk": {
                                            "id": "4-1005",
                                            "overall": "GREEN"
                                        }
                                    },
                                    {
                                        "id": "2-1003-1013",
                                        "overall": "GREEN",
                                        "pdisk": {
                                            "id": "2-1003",
                                            "overall": "GREEN"
                                        }
                                    },
                                    {
                                        "id": "1-1-1008",
                                        "overall": "GREEN",
                                        "pdisk": {
                                            "id": "1-1",
                                            "overall": "GREEN"
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                ]
            },
            "compute": {
                "overall": "GREEN",
                "nodes": [
                    {
                        "id": "50073",
                        "overall": "GREEN",
                        "pools": [
                            {
                                "overall": "GREEN",
                                "name": "System",
                                "usage": 0.000405479
                            },
                            {
                                "overall": "GREEN",
                                "name": "User",
                                "usage": 0.00265229
                            },
                            {
                                "overall": "GREEN",
                                "name": "Batch",
                                "usage": 0.000347933
                            },
                            {
                                "overall": "GREEN",
                                "name": "IO",
                                "usage": 0.000312022
                            },
                            {
                                "overall": "GREEN",
                                "name": "IC",
                                "usage": 0.000945925
                            }
                        ],
                        "load": {
                            "overall": "GREEN",
                            "load": 0.2,
                            "cores": 4
                        }
                    },
                    {
                        "id": "50074",
                        "overall": "GREEN",
                        "pools": [
                            {
                                "overall": "GREEN",
                                "name": "System",
                                "usage": 0.000619053
                            },
                            {
                                "overall": "GREEN",
                                "name": "User",
                                "usage": 0.00463859
                            },
                            {
                                "overall": "GREEN",
                                "name": "Batch",
                                "usage": 0.000596071
                            },
                            {
                                "overall": "GREEN",
                                "name": "IO",
                                "usage": 0.0006241
                            },
                            {
                                "overall": "GREEN",
                                "name": "IC",
                                "usage": 0.00218465
                            }
                        ],
                        "load": {
                            "overall": "GREEN",
                            "load": 0.08,
                            "cores": 4
                        }
                    },
                    {
                        "id": "50075",
                        "overall": "GREEN",
                        "pools": [
                            {
                                "overall": "GREEN",
                                "name": "System",
                                "usage": 0.000579126
                            },
                            {
                                "overall": "GREEN",
                                "name": "User",
                                "usage": 0.00344293
                            },
                            {
                                "overall": "GREEN",
                                "name": "Batch",
                                "usage": 0.000592347
                            },
                            {
                                "overall": "GREEN",
                                "name": "IO",
                                "usage": 0.000525747
                            },
                            {
                                "overall": "GREEN",
                                "name": "IC",
                                "usage": 0.00174265
                            }
                        ],
                        "load": {
                            "overall": "GREEN",
                            "load": 0.26,
                            "cores": 4
                        }
                    }
                ],
                "tablets": [
                    {
                        "overall": "GREEN",
                        "type": "SchemeShard",
                        "state": "GOOD",
                        "count": 1
                    },
                    {
                        "overall": "GREEN",
                        "type": "SysViewProcessor",
                        "state": "GOOD",
                        "count": 1
                    },
                    {
                        "overall": "GREEN",
                        "type": "Coordinator",
                        "state": "GOOD",
                        "count": 3
                    },
                    {
                        "overall": "GREEN",
                        "type": "Mediator",
                        "state": "GOOD",
                        "count": 3
                    },
                    {
                        "overall": "GREEN",
                        "type": "Hive",
                        "state": "GOOD",
                        "count": 1
                    }
                ]
            }
        }
    ]
}
```

#### пример EMERGENCY: {#example-emergency}

Ответ в случаи проблем может выглядеть так

```json
{
  "self_check_result": "EMERGENCY",
  "issue_log": [
    {
      "id": "RED-27c3-70fb",
      "status": "RED",
      "message": "Database has multiple issues",
      "location": {
        "database": {
          "name": "/slice"
        }
      },
      "reason": [
        "RED-27c3-4e47",
        "RED-27c3-53b5",
        "YELLOW-27c3-5321"
      ],
      "type": "DATABASE",
      "level": 1
    },
    {
      "id": "RED-27c3-4e47",
      "status": "RED",
      "message": "Compute has issues with system tablets",
      "location": {
        "database": {
          "name": "/slice"
        }
      },
      "reason": [
        "RED-27c3-c138-BSController"
      ],
      "type": "COMPUTE",
      "level": 2
    },
    {
      "id": "RED-27c3-c138-BSController",
      "status": "RED",
      "message": "System tablet is unresponsive",
      "location": {
        "compute": {
          "tablet": {
            "type": "BSController",
            "id": [
              "72057594037989391"
            ]
          }
        },
        "database": {
          "name": "/slice"
        }
      },
      "type": "SYSTEM_TABLET",
      "level": 3
    },
    {
      "id": "RED-27c3-53b5",
      "status": "RED",
      "message": "System tablet BSC didn't provide information",
      "location": {
        "database": {
          "name": "/slice"
        }
      },
      "type": "STORAGE",
      "level": 2
    },
    {
      "id": "YELLOW-27c3-5321",
      "status": "YELLOW",
      "message": "Storage degraded",
      "location": {
        "database": {
          "name": "/slice"
        }
      },
      "reason": [
        "YELLOW-27c3-595f-8d1d"
      ],
      "type": "STORAGE",
      "level": 2
    },
    {
      "id": "YELLOW-27c3-595f-8d1d",
      "status": "YELLOW",
      "message": "Pool degraded",
      "location": {
        "storage": {
          "pool": {
            "name": "static"
          }
        },
        "database": {
          "name": "/slice"
        }
      },
      "reason": [
        "YELLOW-27c3-ef3e-0"
      ],
      "type": "STORAGE_POOL",
      "level": 3
    },
    {
      "id": "RED-84d8-3-3-1",
      "status": "RED",
      "message": "PDisk is not available",
      "location": {
        "storage": {
          "node": {
            "id": 3,
            "host": "man0-0026.ydb-dev.nemax.nebiuscloud.net",
            "port": 19001
          },
          "pool": {
            "group": {
              "vdisk": {
                "pdisk": [
                  {
                    "id": "3-1",
                    "path": "/dev/disk/by-partlabel/NVMEKIKIMR01"
                  }
                ]
              }
            }
          }
        }
      },
      "type": "PDISK",
      "level": 6
    },
    {
      "id": "RED-27c3-4847-3-0-1-0-2-0",
      "status": "RED",
      "message": "VDisk is not available",
      "location": {
        "storage": {
          "node": {
            "id": 3,
            "host": "man0-0026.ydb-dev.nemax.nebiuscloud.net",
            "port": 19001
          },
          "pool": {
            "name": "static",
            "group": {
              "vdisk": {
                "id": [
                  "0-1-0-2-0"
                ]
              }
            }
          }
        },
        "database": {
          "name": "/slice"
        }
      },
      "reason": [
        "RED-84d8-3-3-1"
      ],
      "type": "VDISK",
      "level": 5
    },
    {
      "id": "YELLOW-27c3-ef3e-0",
      "status": "YELLOW",
      "message": "Group degraded",
      "location": {
        "storage": {
          "pool": {
            "name": "static",
            "group": {
              "id": [
                "0"
              ]
            }
          }
        },
        "database": {
          "name": "/slice"
        }
      },
      "reason": [
        "RED-27c3-4847-3-0-1-0-2-0"
      ],
      "type": "STORAGE_GROUP",
      "level": 4
    }
  ],
  "location": {
    "id": 5,
    "host": "man0-0028.ydb-dev.nemax.nebiuscloud.net",
    "port": 19001
  }
}
```
