# Работа с SelfHeal

В {{ ydb-short-name }} есть два механизма автоматического восстановления (SelfHeal):

1. **SelfHeal хранилища** (эта статья) — для дисков и [групп хранения](../../concepts/glossary.md#storage-group) с данными.
2. **SelfHeal State Storage** — для реплик [State Storage](../../concepts/glossary.md#state-storage), [Board](../../concepts/glossary.md#board) и [SchemeBoard](../../concepts/glossary.md#scheme-board). См. [{#T}](selfheal_statestorage.md).

Оба механизма сохраняют работоспособность и отказоустойчивость кластера, если вышедшие из строя узлы или диски нельзя быстро восстановить.

{% note info %}

SelfHeal State Storage доступен только при [конфигурации V2](../../devops/configuration-management/configuration-v2/config-overview.md). SelfHeal хранилища от версии конфигурации не зависит.

{% endnote %}

## Как работает SelfHeal хранилища {#how-it-works}

[CMS](../../concepts/glossary.md#cms) (компонент Sentinel) постоянно следит за состоянием [PDisk](../../concepts/glossary.md#pdisk) и узлов. Если неисправность сохраняется достаточно долго (по умолчанию около часа), CMS инициирует перенос затронутых [VDisk](../../concepts/glossary.md#vdisk) на исправное оборудование, чтобы снова соблюдалась [модель отказа](../../concepts/topology.md#cluster-config).

Команду исполняет [Blob Storage Controller](../../concepts/glossary.md#ds-controller): данные реплицируются в фоне. Сам перенос может занять от минут до суток в зависимости от объёма данных и оборудования. После принятия команды для CMS задача уже поставлена; завершение репликации обеспечивает распределённое хранилище.

SelfHeal хранилища включён по умолчанию для [динамических групп](../../concepts/glossary.md#dynamic-group). В кластерах с конфигурацией V2 можно также включить [SelfHeal статической группы](../../devops/configuration-management/configuration-v2/static-group-self-heal.md).

Ниже — как включать, выключать и настраивать SelfHeal хранилища.

## Включение и выключение SelfHeal {#on-off}

Вы можете включать и выключать SelfHeal с помощью утилиты [{{ ydb-short-name }} DSTool](../../reference/ydb-dstool/index.md).

Чтобы включить SelfHeal выполните команду:

```bash
ydb-dstool -e <bs_endpoint> cluster set --enable-self-heal
```

`<bs_endpoint>` - эндпоинт произвольного [узла хранения](../../concepts/glossary.md#storage-node) кластера.

Чтобы выключить SelfHeal выполните команду:

```bash
ydb-dstool -e <bs_endpoint> cluster set --disable-self-heal
```

## Настройки SelfHeal {#settings}

Вы можете настроить SelfHeal в **Viewer** → **Cluster Management System** → **CmsConfigItems**.

Чтобы создать настройки впервые, нажмите кнопку **Create**. Если вам нужно изменить существующие настройки, нажмите кнопку ![карандаш](../../_assets/pencil.svg).

Доступны следующие настройки:

| **Параметр**                             | **Описание**                                                                                                                                                             |
|:---------------------------------------- |:------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Status**                               | Включение и выключение SelfHeal в CMS. |
| **Dry run**                              | Включение и выключение режима, в котором CMS не изменяет настройку BSC. |
| **Config update interval (sec.)**        | Период обновления конфигурации из BSC. |
| **Retry interval (sec.)**                | Период повторных попыток обновления конфигурации. |
| **State update interval (sec.)**         | Период обновления состояний PDisk'ов. |
| **Timeout (sec.)**                       | Таймаут обновления состояний PDisk'ов. |
| **Change status retries**                | Количество повторных попыток на изменение статуса PDisk в BSC (`ACTIVE`, `FAULTY`, `BROKEN` и др.). |
| **Change status retry interval (sec.)**  | Задержка между попытками на изменение статуса PDisk в BSC. CMS наблюдает состояние диска с интервалом **State update interval**. Если диск пребывает в одном состоянии несколько циклов **Status update interval**, то CMS меняет его статус в BSC.<br/>Далее идут настройки количества циклов обновления, через которое CMS будет изменять статус диска. Если состояние диска `Normal`, то диск переводится в статус `ACTIVE`, в остальных состояниях диск переводится в статус `FAULTY`.<br/>Значение `0` выключает изменение статуса для состояния (так реализовано для `Unknown` по умолчанию).<br/>Например, при настройках по умолчанию, если CMS наблюдает состояние диска `Initial` на протяжении 5 циклов `Status update interval` по 60 с каждый, статус диска будет изменен на `FAULTY`. |
| **Default state limit**                  | Для состояний, для которых нет указана настройка, может использоваться это значение "по умолчанию". Для неизвестных состояний PDisk, для которых нет настройки, тоже используется это значение. Это значение используется если значение не задано для состояний `Initial`, `InitialFormatRead`, `InitialSysLogRead`, `InitialCommonLogRead`, `Normal`. |
| **Initial**                              | PDisk начинает инициализацию. Переход в `FAULTY`. |
| **InitialFormatRead**                    | PDisk читает свою запись формата. Переход в `FAULTY`. |
| **InitialFormatReadError**               | PDisk получил ошибку при чтении своей записи формата. Переход в `FAULTY`. |
| **InitialSysLogRead**                    | PDisk читает системный лог. Переход в `FAULTY`. |
| **InitialSysLogReadError**               | PDisk получил ошибку при чтении системного лога. Переход в `FAULTY`. |
| **InitialSysLogParseError**              | PDisk получил ошибку при парсинге или проверке консистентности системного лога. Переход в `FAULTY`. |
| **InitialCommonLogRead**                 | PDisk читает общий лог VDisk'ов. Переход в `FAULTY`. |
| **InitialCommonLogReadError**            | PDisk получил ошибку при чтении общего лога VDisk'ов. Переход в `FAULTY`. |
| **InitialCommonLogParseError**           | PDisk получил ошибку при парсинге или проверке консистентности общего лога. Переход в `FAULTY`. |
| **CommonLoggerInitError**                | PDisk получил ошибку при инициализации внутренних структур предназначенных для записи в общий лог. Переход в `FAULTY`. |
| **Normal**                               | PDisk завершил инициализацию и работает нормально. Переход в `ACTIVE` произойдет через указанное количество циклов (например, если `Normal` держится 5 минут, диск переходит в состояние `ACTIVE`). |
| **OpenFileError**                        | PDisk получил ошибку при открытии файла диска. Переход в `FAULTY`. |
| **Missing**                              | Нода отвечает, но в её списке нет данного PDisk. Переход в `FAULTY`. |
| **Timeout**                              | Нода не ответила за отведенный таймаут. Переход в `FAULTY`. |
| **NodeDisconnected**                     | Отключение ноды. Переход в `FAULTY`. |
| **Stopped**                              | PDisk остановлен. Переход в `FAULTY`. |
| **Unknown**                              | Неожиданный ответ, например, ответ `TEvUndelivered` на запрос состояния. Переход в `FAULTY`. |

## Работа с дисками-донорами {#disks}

Диск-донор — это предыдущий VDisk после переноса данных, который продолжает хранить свои данные и отвечает только на запросы чтения от нового VDisk'а. При переносе с включенными дисками-донорами предыдущие VDisk'и продолжают функционировать до тех пор, пока данные не будут полностью перенесены на новые диски. Чтобы предотвратить потерю данных при переносе VDisk'а, включите возможность использования дисков-доноров:

```bash
ydb-dstool -e <bs_endpoint> cluster set --enable-donor-mode
```

Чтобы выключить диски-доноры, введите команду:

```bash
ydb-dstool -e <bs_endpoint> cluster set --disable-donor-mode
```
