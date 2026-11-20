# Управление Bridge через YDB CLI

## Задача

Есть кластер YDB в пайле `pile1` с двумя БД: `/Root/orders` и `/Root/catalog`. Хотим подключить `pile2` и получить синхронизированную копию данных, на которую можно переключить работу при обслуживании или аварии.

Пройдём путь от обычного кластера до двух работающих пайлов, каждый раз проверяя результат перед следующим действиемш.

## Сначала узнаём, в каком режиме работает кластер

Администратор хочет понять, включён ли Bridge, но пока не знает нужную команду. Начинает со справки раздела:

```text
$ ydb admin cluster bridge --help
Usage: ydb admin cluster bridge <command> [options...]

Description:
  Manage cluster replication across piles.

Commands:
  list             Show pile state and data synchronization progress.
  switchover       Transfer the PRIMARY role to a synchronized pile.
  takedown         Take a pile down for planned maintenance.
  failover         Exclude failed piles and continue on the selected side.
  join-pile        Join or rejoin a pile and start synchronization.
  convert          Convert a prepared single-pile cluster to bridge mode.
  provision-pile   Prepare storage in a new pile.
  destroy-pile     Delete a disconnected pile's storage and data.
  rollback         Start returning a converted single-pile cluster to ordinary mode.
  set-database-mode Change bridge storage mode of a database.
```

В справке находит `list` — команду просмотра состояния. Теперь нужно узнать, как её вызвать и что она покажет. Открывает справку самой команды:

```text
$ ydb admin cluster bridge list --help
Usage: ydb admin cluster bridge list [piles|databases|groups] [options...]

Arguments:
  [piles|databases|groups]  What to list. Default: piles.
                           piles: pile state and current operations.
                           databases: database synchronization by pile.
                           groups: storage group synchronization by pile.

Options:
  --format FORMAT       Output format: pretty, json, csv. Default: pretty.
  --detailed            For piles, add database counters or node IDs.
                        For databases, add group counters and the last error.
                        For groups, add physical IDs, phases and the last error.
  --pile NAME           Show only this pile. An empty name is valid.
  --database-path PATH  For databases and groups, select database storage.
                        Serverless selects shared storage.
  --not-synced          For databases and groups, show incomplete required
                        synchronization, including unknown states.
  --mode MODE           For databases: bridged, ordinary, mixed.
  --pool NAME           For groups, select a uniquely named storage pool.
  --pool-id BOX:POOL    For groups, select a pool by ID. BOX may be zero;
                        POOL must be nonzero.
```

По умолчанию `list` показывает `piles` — состояния пайлов и текущие операции. Для первого знакомства с кластером этого достаточно; фильтры и подробности пока не нужны. Запускает команду без дополнительных опций:

```text
$ ydb admin cluster bridge list
Cluster mode: ordinary
```

Кластер обычный. Чтобы подключить второй пайл, сначала подготовим его к Bridge и выполним конвертацию. Если бы `list` уже показал пайлы, повторная конвертация не потребовалась бы.

## Готовим конфигурацию и конвертируем кластер

Оператор настраивает на всех узлах Bridge с единственным пайлом `pile1`. Это изменение конфигурации и запуск узлов выполняются отдельно от CLI. В нашем примере после подготовки трёх узлов получаем:

```text
$ ydb admin cluster bridge list
Cluster mode: ordinary
Bridge configuration: present on 3 nodes
```

Число узлов видим; теперь проверим, какие именно узлы уже используют новую конфигурацию. Для этого добавим `--detailed`:

```text
$ ydb admin cluster bridge list --detailed
Cluster mode: ordinary
Bridge configuration: present on 3 nodes
Node IDs with bridge configuration: 1, 2, 3
```

В нашем примере это все три подготовленных узла. Кластер ещё не сконвертирован: списка пайлов пока нет. В общей справке для перехода в Bridge находим `convert`. Перед запуском читаем условия:

```text
$ ydb admin cluster bridge convert --help
Usage: ydb admin cluster bridge convert [options...]

Description:
  Convert an ordinary cluster to bridge mode with one pile.
  All nodes must already use a single-pile bridge configuration.
  Existing storage pools and groups are converted in the background.

Options:
  --skip-storage-pool-conversion
      Leave existing storage pools and groups ordinary.
      Static groups are still converted.
      Requires bridge_config.enable_non_uniform_storage=true.
```

В нашем сценарии нужно реплицировать существующие БД, поэтому `--skip-storage-pool-conversion` не указываем. Узлы уже подготовлены с одним пайлом; запускаем конвертацию:

```text
$ ydb admin cluster bridge convert
Cluster converted to bridge mode successfully.

$ ydb admin cluster bridge list
Pile pile1: PRIMARY
Generation: 1
Conversion from non-bridged cluster is in progress, piles can't be added yet
```

Bridge уже включён, но существующее хранилище ещё преобразуется. Добавлять второй пайл рано. Повторяем `list`, пока предупреждение не исчезнет:

```text
$ ydb admin cluster bridge list
Pile pile1: PRIMARY
Generation: 1
```

Теперь можно менять состав пайлов. `Generation` — версия состояния; она меняется и при автоматических переходах, а не только после команд оператора.

## Подготавливаем второй пайл

Оператор разворачивает узлы `pile2` и добавляет новый пайл в конфигурацию. Проверяем, что кластер его увидел:

```text
$ ydb admin cluster bridge list
Pile pile1: PRIMARY
Pile pile2: INACTIVE
Generation: 2
```

INACTIVE означает, что пайл описан в конфигурации, но его хранилище ещё не подготовлено. Для подготовки в общей справке есть `provision-pile`. Уточняем, что ему нужно:

```text
$ ydb admin cluster bridge provision-pile --help
Usage: ydb admin cluster bridge provision-pile --pile NAME

Description:
  Prepare storage in an INACTIVE pile configured with its hosts.
  The pile becomes DISCONNECTED; use join-pile to start synchronization.

Options:
  --pile NAME  Required. Pile to prepare.
```

Обязателен `--pile`; указываем новый пайл `pile2`:

```text
$ ydb admin cluster bridge provision-pile --pile pile2
Pile provisioned successfully. Use join-pile to join it to the cluster.

$ ydb admin cluster bridge list
Pile pile1: PRIMARY
Pile pile2: DISCONNECTED
Generation: 3
```

Хранилище подготовлено. DISCONNECTED здесь ожидаем: пайл ещё не включён в репликацию. Следующий шаг — `join-pile`.

## Включаем пайл в репликацию

Подсказка после подготовки направила нас к `join-pile`. Сначала проверим его справку:

```text
$ ydb admin cluster bridge join-pile --help
Usage: ydb admin cluster bridge join-pile --pile NAME
Alias: rejoin

Description:
  Join a prepared pile or rejoin after maintenance or recovery.
  Synchronization runs in the background.
  Check list until the pile becomes SYNCHRONIZED.

Options:
  --pile NAME  Required. Pile to join or rejoin.
```

Команда принимает имя пайла и запускает синхронизацию в фоне. Указываем `pile2`; его готовность затем проверим через уже знакомый `list`:

```text
$ ydb admin cluster bridge join-pile --pile pile2
Cluster state updated successfully.

$ ydb admin cluster bridge list
Pile pile1: PRIMARY
Pile pile2: NOT_SYNCHRONIZED
Generation: 5
```

Команда применила изменение, копирование идёт в фоне. NOT_SYNCHRONIZED означает, что `pile2` ещё не готов заменить основной пайл. Теперь нужно понять, сколько осталось и какие данные его задерживают.

## Находим незавершённую синхронизацию

Обычный `list` сообщил, что `pile2` не синхронизирован, но не показал, сколько БД ещё не готовы. Добавим `--detailed` и выберем только этот пайл:

```text
$ ydb admin cluster bridge list piles --pile pile2 --detailed
┌───────┬──────────────────┬────────────────────────┬───────────────────┬───────────────────┬────────────────────────┬───────────────────┐
│ Pile  │ State            │ Synced/total databases │ Syncing databases │ Pending databases │ Not required databases │ Unknown databases │
├───────┼──────────────────┼────────────────────────┼───────────────────┼───────────────────┼────────────────────────┼───────────────────┤
│ pile2 │ NOT_SYNCHRONIZED │ 1/2                    │ 1                 │ 0                 │ 0                      │ 0                 │
└───────┴──────────────────┴────────────────────────┴───────────────────┴───────────────────┴────────────────────────┴───────────────────┘
Generation: 5
```

Готова одна из двух БД; вторая синхронизируется. Ожидающих БД и БД с неизвестным состоянием нет. Теперь хотим узнать, какая именно БД задерживает готовность пайла:

```text
$ ydb admin cluster bridge list databases --pile pile2
┌───────────────┬─────────────┬─────────┬───────┬─────────┬───────────────┐
│ Database      │ Pools       │ Mode    │ Pile  │ Sync    │ Synced groups │
├───────────────┼─────────────┼─────────┼───────┼─────────┼───────────────┤
│ /Root/catalog │ catalog-ssd │ bridged │ pile2 │ SYNCED  │ 2/2           │
│ /Root/orders  │ orders-ssd  │ bridged │ pile2 │ SYNCING │ 1/2           │
└───────────────┴─────────────┴─────────┴───────┴─────────┴───────────────┘
```

У `catalog` обе обязательные группы уже синхронизированы. У `orders` готова одна из двух. Нужно понять, оставшаяся группа копируется, ждёт запуска или её состояние неизвестно. Оставим незавершённые БД через `--not-synced` и запросим подробности:

```text
$ ydb admin cluster bridge list databases --pile pile2 --not-synced --detailed
┌──────────────┬────────────┬─────────┬───────┬─────────┬───────────────┬────────────────┬────────────────┬────────────────┬─────────────────┬─────────────────────────────┐
│ Database     │ Pools      │ Mode    │ Pile  │ Sync    │ Synced groups │ Syncing groups │ Pending groups │ Unknown groups │ Last sync error │ Last error time             │
├──────────────┼────────────┼─────────┼───────┼─────────┼───────────────┼────────────────┼────────────────┼────────────────┼─────────────────┼─────────────────────────────┤
│ /Root/orders │ orders-ssd │ bridged │ pile2 │ SYNCING │ 1/2           │ 1              │ 0              │ 0              │ timeout         │ 2026-11-11T10:00:00.000000Z │
└──────────────┴────────────┴─────────┴───────┴─────────┴───────────────┴────────────────┴────────────────┴────────────────┴─────────────────┴─────────────────────────────┘
```

Дополнительные колонки отвечают на вопрос: одна группа синхронизируется, ожидающих и неизвестных нет. Также видна последняя ошибка — `timeout` и время её возникновения. Ошибка могла остаться от предыдущей попытки; сама по себе она не означает, что копирование сейчас остановлено.

Проверим оставшуюся группу и фазу её работы. `--detailed` на уровне групп добавит физический ID, фазу и последнюю ошибку:

```text
$ ydb admin cluster bridge list groups --database-path /Root/orders --pile pile2 --not-synced --detailed
Database: /Root/orders
┌────────────┬────────────┬───────┬─────────┬────────────────┬───────┬─────────────────┬─────────────────────────────┐
│ Group      │ Pool       │ Pile  │ Sync    │ Physical group │ Phase │ Last sync error │ Last error time             │
├────────────┼────────────┼───────┼─────────┼────────────────┼───────┼─────────────────┼─────────────────────────────┤
│ 2181038082 │ orders-ssd │ pile2 │ SYNCING │ 2147483650     │ DATA  │ timeout         │ 2026-11-11T10:00:00.000000Z │
└────────────┴────────────┴───────┴─────────┴────────────────┴───────┴─────────────────┴─────────────────────────────┘
```

Осталась группа `2181038082`, сейчас она SYNCING, текущая фаза DATA — передача данных. Продолжим наблюдение до завершения. Если бы группа была PENDING, работа требовалась бы, но сейчас не выполнялась; UNKNOWN означал бы нехватку наблюдений. Оба статуса также попадают в `--not-synced`.

После завершения копирования проверяем общий результат:

```text
$ ydb admin cluster bridge list
Pile pile1: PRIMARY
Pile pile2: SYNCHRONIZED
Generation: 6
```

Задача выполнена: второй пайл синхронизирован и может стать PRIMARY. Такой же путь диагностики нужен после возвращения пайла из обслуживания или аварии.

## Что делаем дальше на таком кластере

### Плановые работы в основном пайле

Хотим обслужить `pile1`, передав его роль синхронизированному `pile2`. В общей справке для планового вывода есть `takedown`. Открываем его help:

```text
$ ydb admin cluster bridge takedown --help
Usage: ydb admin cluster bridge takedown --pile NAME [--new-primary NAME]

Description:
  Gracefully take a pile down for maintenance.
  Wait for DISCONNECTED before starting maintenance.

Options:
  --pile NAME         Required. Pile to take down.
  --new-primary NAME  Required when taking down the current PRIMARY.
                      Select a synchronized pile.
```

`pile1` сейчас PRIMARY, поэтому помимо `--pile` обязателен `--new-primary`. Выбираем синхронизированный `pile2`:

```text
$ ydb admin cluster bridge takedown --pile pile1 --new-primary pile2
Cluster state updated successfully.
```

Команда начинает плановый вывод. Через `list` дожидаемся DISCONNECTED у `pile1` и PRIMARY у `pile2`, затем приступаем к работам. После обслуживания выполняем `join-pile --pile pile1` и ждём SYNCHRONIZED. Если нужно только передать роль без отключения пайла, выбираем `switchover`. Уточняем его параметры:

```text
$ ydb admin cluster bridge switchover --help
Usage: ydb admin cluster bridge switchover --new-primary NAME

Description:
  Start a planned transition to a new PRIMARY.

Options:
  --new-primary NAME  Required. Synchronized pile to become PRIMARY.
```

Нужен только новый PRIMARY; выбираем синхронизированный `pile2`:

```text
$ ydb admin cluster bridge switchover --new-primary pile2
Cluster state updated successfully.
```

### Авария в пайле

Если `pile1` отказал, оператор выбирает работоспособную сторону, останавливает или изолирует отказавшую. В справке `failover` проверяет, как указать отказавшие пайлы и нового PRIMARY:

```text
$ ydb admin cluster bridge failover --help
Usage: ydb admin cluster bridge failover --pile NAME [--pile NAME ...]
                                       [--new-primary NAME]

Description:
  Exclude failed piles in one operation.

Options:
  --pile NAME         Required. Failed pile; repeat for multiple piles.
  --new-primary NAME  Required if the failed piles include PRIMARY.
                      Select a synchronized pile.
```

Отказал текущий PRIMARY `pile1`, поэтому указываем и `--pile`, и `--new-primary`. Выполняем в `pile2`:

```text
$ ydb admin cluster bridge failover --pile pile1 --new-primary pile2
Cluster state updated successfully.
```

Если отказали несколько пайлов, перечисляем их одной командой через повторяющийся `--pile`. Команда не выбирает доверенную сторону сетевого разделения и не изолирует её средствами инфраструктуры. После устранения аварии пайлы возвращаются через `join-pile`.

### Разные требования к копиям разных БД

Если дополнительные копии нужны только части БД, режим меняется отдельно для каждой. Для этого в справке раздела есть `set-database-mode`. Смотрим, как выбрать БД и сохраняемую копию:

```text
$ ydb admin cluster bridge set-database-mode --help
Usage: ydb admin cluster bridge set-database-mode MODE
       --database-path PATH [--retain-pile NAME]

Arguments:
  MODE  Required: bridged or ordinary.
        bridged enables replication across prepared piles.
        ordinary disables bridge replication.

Options:
  --database-path PATH  Required. Absolute path of the target database.
                        Global --database selects the connection database.
  --retain-pile NAME    Only with ordinary. Retain synchronized copies here.
                        Without it, copies are selected automatically
                        and may remain in different piles.
                        Does not relocate existing ordinary data.

Description:
  Requires bridge_config.enable_non_uniform_storage=true.
  For serverless storage, select the shared database;
  the change affects all its serverless databases.
  Synchronization and cleanup continue in the background.
```

Нужны включённый `bridge_config.enable_non_uniform_storage=true`, режим и явный путь БД. Отключаем репликацию `catalog`, сохраняя синхронизированную копию в `pile1`:

```text
$ ydb admin cluster bridge set-database-mode ordinary --database-path /Root/catalog --retain-pile pile1
Database bridge mode updated successfully. Data synchronization and cleanup continue in the background.
```

Без `--retain-pile` копии выбираются автоматически и могут остаться в разных пайлах; уже обычные данные эта опция не переносит. В диагностике обычное хранение получает NOT_REQUIRED, а не «нулевую готовность». Отчёты синхронизации не показывают завершение удаления лишних копий.

Обратное действие — `set-database-mode bridged --database-path /Root/catalog`. При поэтапной миграции `convert --skip-storage-pool-conversion` оставляет существующие пулы обычными, затем репликация включается для выбранных БД. Для serverless меняется режим shared БД: это общее хранилище, изменение затрагивает всех его пользователей.

### Отказ от второго пайла и возврат в обычный режим

Если `pile2` больше не нужен, сначала отключаем его. Для окончательного удаления в общей справке есть `destroy-pile`; перед запуском читаем последствия:

```text
$ ydb admin cluster bridge destroy-pile --help
Usage: ydb admin cluster bridge destroy-pile --pile NAME

Description:
  Delete storage-group parts and data in a DISCONNECTED pile.
  This cannot be undone. Waits for removal to complete.
  The pile becomes INACTIVE; then remove it and its hosts from configuration.

Options:
  --pile NAME  Required. Pile whose storage and data will be deleted.
```

Пайл должен быть DISCONNECTED, а его данные будут удалены. Указываем `pile2`:

```text
$ ydb admin cluster bridge destroy-pile --pile pile2
Pile destroyed successfully, it is INACTIVE now.
```

Данные пайла удалены, теперь его и узлы можно убрать из конфигурации. Если хотим отменить сам переход на Bridge, смотрим справку `rollback`:

```text
$ ydb admin cluster bridge rollback --help
Usage: ydb admin cluster bridge rollback [options...]

Description:
  Start returning a converted cluster to ordinary mode.
  The original pile must be the only pile;
  all bridged storage groups must be inline in it.
  Group IDs and data are preserved.
  Monitor progress with list piles.
  After storage rollback, remove bridge configuration and restart nodes.
  The report covers static and connected dynamic nodes.

Options:
  --skip-bsc-validation
      Skip the storage controller's preliminary rollback check.
      Use only when the storage controller cannot start.
```

Должен остаться единственный исходный пайл, а группы хранения — допускать возврат. В обычном сценарии предварительную проверку контроллера не пропускаем и запускаем команду без `--skip-bsc-validation`:

```text
$ ydb admin cluster bridge rollback
Rollback started. Monitor progress with "ydb admin cluster bridge list piles".
```

Через `list` ждём преобразования хранилища. После него увидим `Cluster mode: ordinary` и `Bridge configuration: present on 3 nodes`: остаётся одним изменением удалить `bridge_config` и имена пайлов узлов, затем перезапустить узлы без них. Финальная проверка замыкает сценарий:

```text
$ ydb admin cluster bridge list
Cluster mode: ordinary
```

Отчёт учитывает все статические и подключённые динамические узлы. Если bridge конфигурация уже не обнаружена, но часть статических узлов не отчиталась, он покажет `Bridge configuration status incomplete` и число таких узлов. Обычный режим пока не подтверждён.

## Предлагаемый интерфейс CLI

Сохраняем существующие команды по сценариям. Разделение планового `switchover` и аварийного `failover` принято в [Patroni](https://patroni.readthedocs.io/en/latest/patronictl.html#patronictl-switchover). Краткая таблица, подробности по флагу и машинные форматы соответствуют подходам [kubectl](https://kubernetes.io/docs/reference/kubectl/generated/kubectl_get/) и [gcloud](https://docs.cloud.google.com/sdk/gcloud/reference/topic/filters); имена опций остаются общими с YDB CLI.

`piles`, `databases` и `groups` — значения одного необязательного свободного аргумента команды `list`. Общая справка `list --help` описывает все опции и их применимость. Без аргумента используется `piles`.

Фильтры `--database-path`, `--pool` и `--pool-id` взаимоисключающие. В диагностике `--database-path` задаёт фильтр; глобальная `--database` выбирает БД подключения.

```text
$ ydb admin database bridge set-mode --help
Usage: ydb [connection options] -d PATH admin database bridge set-mode MODE [options...]

Arguments:
  MODE                 bridged: enable replication; ordinary: disable replication.

Options:
  --retain-pile NAME    With ordinary, retain synchronized copies in this pile.

Description:
  Change storage mode of the database selected with -d.
  Requires non-uniform storage. For serverless storage, select the shared database.
  Synchronization and cleanup continue in the background.
```

## Поведение и совместимость

CLI сообщает, что произошло: изменение принято, синхронизация запущена или удаление завершено. Для фоновой работы сообщение содержит команду проверки. Если обязательная часть отчёта недоступна или его части относятся к разным поколениям состояния, команда сообщает причину и возвращает ненулевой код.

Сохраняются подтверждения административных действий и `-y` / `--assume-yes` для скриптов. Вопрос называет кластер, цель и последствия; для `destroy-pile` — удаление данных. JSON и CSV выводятся без служебных сообщений в stdout, предупреждения идут в stderr. Для БД и групп JSON содержит все доступные детали независимо от `--detailed`.

Существующие команды и аргументы продолжают работать: `rejoin` остаётся алиасом `join-pile`, а `admin cluster bridge set-database-mode MODE --database-path PATH` — прежним способом изменить БД с явным выбором цели. Видимые изменения фиксируются в changelog YDB CLI.
