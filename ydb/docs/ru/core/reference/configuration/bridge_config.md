# bridge_config {#bridge-config}

Секция описывает pile кластера и режимы хранения для [режима bridge](../../concepts/bridge.md). Для каждого хоста необходимо указать имя соответствующего pile в секции `hosts` с помощью поля `location.bridge_pile_name`, см. [особенности режима bridge](hosts.md#hosts-bridge). Узел читает собственный `location` при запуске. После изменения привязки существующего узла к pile перезапустите узел. Настройки S3 резервного pile можно менять без перезапуска.

## Синтаксис

```yaml
bridge_config:
  piles:
  - name: <pile_name_1>
  - name: <pile_name_2>
  ...
  - name: <pile_name_n>
  enable_non_uniform_storage: <true_or_false>
```

## Параметры {#parameters}

| Параметр | Значение по умолчанию | Описание |
| --- | --- | --- |
| `piles` | `[]` | Список pile, настроенных в кластере. Pile из списка без выделенных ресурсов хранения находится в состоянии `INACTIVE`. |
| `enable_non_uniform_storage` | `false` | Позволяет базам данных с [обычным хранилищем и хранилищем в режиме bridge](../../concepts/bridge.md#storage-pools) существовать в одном кластере одновременно. После активации эту настройку нельзя отключить в кластере bridge; при [откате к обычному режиму](../../devops/deployment-options/manual/bridge-management.md#rollback) она удаляется вместе с `bridge_config`. |

При преобразовании обычного кластера по умолчанию не задавайте `enable_non_uniform_storage` до завершения преобразования. Преобразование с `--skip-storage-pool-conversion` требует установить этот параметр в `true` до начала преобразования. Подробнее см. в разделе [Преобразование кластера](../../devops/deployment-options/manual/bridge-management.md#convert).


## Резервные pile {#backup-piles}

Поле `type` по умолчанию имеет значение `REGULAR`. В pile типа `BACKUP` каждой динамической группе в режиме bridge соответствует виртуальная группа BlobDepot. Статические группы и реплики StateStorage используют локальные диски и узлы. При первоначальном развёртывании первый обычный pile становится основным, а резервные pile получают состояние `INACTIVE`.

```yaml
bridge_config:
  piles:
    - name: pile-a
    - name: archive
      type: BACKUP
      backup:
        channel_profiles:
          - storage_pool_name: archive-local
            channel_kind: System
            count: 2
          - storage_pool_name: archive-local
            channel_kind: Data
            count: 2
        s3:
          settings:
            endpoint: s3.example.com
            scheme: HTTPS
            bucket: ydb-archive
            object_key_pattern: production/cluster-a
            access_key: <access-key>
            secret_key: <secret-key>
          async_mode: {}
```

В каждом профиле каналов укажите имя физического пула хранения, который администратор выделил для этого pile. Первым двум каналам назначьте тип `System`. Добавьте хотя бы один канал типа `Data`. Значение `count` по умолчанию равно единице. Тип pile и привязки каналов можно менять, пока pile находится в состоянии `INACTIVE`.

В секции `s3` задайте настройки S3 для BlobDepot и укажите `async_mode`. BlobDepot подтверждает запись после сохранения данных в локальном хранилище pile, затем в фоновом режиме выгружает данные в S3. Каждый BlobDepot добавляет своё имя к `object_key_pattern` и использует полученный путь как префикс ключей в бакете. В бакете хранятся данные блобов. Для работы pile также нужны метаданные локальных таблеток и статические группы.

Кластер передаёт конфигурацию вместе с ключами доступа к S3 своим узлам. Ограничьте доступ к файлам конфигурации, учётным данным для регистрации узлов и сети interconnect. При установке соединения interconnect узлы обмениваются конфигурацией хранилища до начала сеанса TLS.

О создании пулов, смене ключей и переключении роли `PRIMARY` см. в разделе [Управление резервным pile](../../devops/deployment-options/manual/bridge-management.md#backup-pile).
