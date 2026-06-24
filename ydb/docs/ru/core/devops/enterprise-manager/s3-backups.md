# Настройка резервного копирования в S3 для YDB EM

В этом руководстве описана настройка резервного копирования баз данных в S3-совместимое хранилище через {{ ydb-short-name }} Enterprise Manager (YDB EM). Резервное копирование настраивается в конфигурационном файле Control Plane, например `kikimr/ydbcp/configs/em/config.yaml`.

Для настройки S3-бэкапов нужны три блока конфигурации:

* `backup_targets` — куда сохранять резервные копии;
* `secret_key` — каким мастер-ключом зашифрованы ключи доступа к S3;
* `locations[].default_backup_config` — когда запускать резервное копирование и сколько хранить копии.

## Настройка S3-хранилища {#backup-targets}

Добавьте в конфигурацию Control Plane блок `backup_targets`:

```yaml
backup_targets:
  - target_id: "target-em"
    tags:
      locations:
        - "em"
    settings:
      s3:
        endpoint: s3.mds.yandex.net
        bucket: enterprise-manager-backups-test
        scheme: 1
        access_key: "<encrypted access key>"
        secret_key: "<encrypted secret key>"
        compression: "zstd"
```

Параметр | Описание
--- | ---
`target_id` | Уникальный идентификатор целевого хранилища резервных копий.
`tags.locations` | Список `location_id`, для которых применяется этот target. База данных будет копироваться в этот target, если ее `location_id` есть в списке. Для YDB EM обычно используется значение `em`, соответствующее `locations[].database_location_id` и `meta_location_id`.
`settings.s3.endpoint` | Адрес S3-совместимого хранилища.
`settings.s3.bucket` | Имя бакета для резервных копий.
`settings.s3.scheme` | Протокол подключения: `1` — HTTP, `2` — HTTPS. Если параметр не задан, используется HTTPS. Для внутреннего MDS `s3.mds.yandex.net` обычно используется HTTP.
`settings.s3.access_key` | Зашифрованный ключ доступа к S3.
`settings.s3.secret_key` | Зашифрованный секретный ключ доступа к S3.
`settings.s3.compression` | Алгоритм сжатия экспортируемых данных. По умолчанию используется `zstd`. Удалите параметр, если сжатие не требуется.

{% note info %}

Вместо привязки target к `location_id` можно использовать `explicit_backup_targets` с правилами `by_database_path` или `by_cloud_id`, если конкретную базу данных или облако нужно закрепить за определенным target.

{% endnote %}

## Настройка мастер-ключа {#master-key}

На верхнем уровне конфигурационного файла укажите путь к файлу с мастер-ключом:

```yaml
secret_key: configs/em/secret_key
```

Этот мастер-ключ используется для шифрования и расшифровки значений `settings.s3.access_key` и `settings.s3.secret_key`. По умолчанию Control Plane ищет мастер-ключ в файле `/Berkanavt/ydbcp/secrets/secret_key.txt`. Файл должен быть доступен процессам Control Plane.

{% note warning %}

Не сохраняйте `access_key` и `secret_key` в конфигурации в открытом виде. YDB EM ожидает зашифрованные значения и расшифровывает их во время выполнения.

{% endnote %}

## Шифрование ключей доступа к S3 {#encrypt-s3-keys}

Зашифруйте ключи доступа к S3 тем же мастер-ключом, который указан в параметре `secret_key`. Для шифрования используйте CLI Control Plane:

```bash
ydbcp admin crypto encrypt --body '<plaintext access key>' --cfg-file configs/em/config.yaml
ydbcp admin crypto encrypt --body '<plaintext secret key>' --cfg-file configs/em/config.yaml
```

Скопируйте полученные зашифрованные строки в параметры `settings.s3.access_key` и `settings.s3.secret_key`.

Чтобы проверить, что значение можно расшифровать тем же мастер-ключом, выполните:

```bash
ydbcp admin crypto decrypt --body '<encrypted value>' --cfg-file configs/em/config.yaml
```

## Настройка расписания и срока хранения {#schedule-and-ttl}

Target определяет, куда сохранять резервные копии. Расписание и срок хранения задаются отдельно, в блоке `locations[].default_backup_config`:

```yaml
locations:
  - database_location_id: em
    default_backup_config:
      backup_settings:
        - name: daily
          type: SYSTEM
          backup_schedule:
            daily_backup_schedule:
              execute_time:
                hours: 20
          backup_time_to_live: "604800s"
```

Параметр | Описание
--- | ---
`backup_settings[].name` | Имя настройки резервного копирования.
`backup_settings[].type` | Тип настройки. Для системного расписания используйте `SYSTEM`.
`backup_schedule.daily_backup_schedule.execute_time.hours` | Час запуска ежедневного резервного копирования в UTC.
`backup_time_to_live` | Срок хранения резервной копии в секундах. Например, `604800s` — 7 суток.

## Применение конфигурации {#apply}

Чтобы применить настройки:

1. Разместите обновленный `config.yaml` в рабочей директории Control Plane. По умолчанию используются пути `/Berkanavt/ydbcp/cfg/config.yaml`, `/opt/ydbcp/config/config.yaml` или `/opt/ydb-em/ydb-em-cp/cfg/config.yaml`.
1. Убедитесь, что файл мастер-ключа, указанный в `secret_key`, доступен процессам Control Plane.
1. Проверьте, что `settings.s3.access_key` и `settings.s3.secret_key` зашифрованы тем же мастер-ключом.
1. Перезапустите процессы Control Plane: `server` и `worker`.

После перезапуска worker по расписанию определяет target по `location_id`, настраивает S3-хранилище и запускает экспорт данных в указанный бакет.

## Проверочный список {#checklist}

Перед запуском проверьте:

* `backup_targets[].tags.locations` содержит `location_id` баз данных, для которых нужно включить резервное копирование;
* `endpoint`, `bucket` и `scheme` указывают на нужное S3-хранилище;
* `access_key` и `secret_key` зашифрованы командой `ydbcp admin crypto encrypt`;
* верхнеуровневый параметр `secret_key` указывает на корректный файл мастер-ключа;
* в `locations[].default_backup_config` задано расписание и срок хранения;
* обновленный конфигурационный файл размещен в рабочей директории Control Plane;
* процессы Control Plane перезапущены после изменения конфигурации.
