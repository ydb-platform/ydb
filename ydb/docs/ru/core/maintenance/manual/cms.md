# Изменение конфигураций через CMS

## Получить текущие настройки

Следующая команда позволит получить текущие настройки по кластеру или по тенанту.

```
./kikimr -s <endpoint> admin console configs load --out-dir <config-folder>
```

```
./kikimr -s <endpoint> admin console configs load --out-dir <config-folder> --tenant <tenant-name>
```

## Обновить настройки

Сначала надо выкачать нужный конфиг как указано выше, после чего требуется подготовить protobuf файл с запросом на изменение.

```
Actions {
  AddConfigItem {
    ConfigItem {
      Cookie: "<cookie>"
      UsageScope {
        TenantAndNodeTypeFilter {
          Tenant: "<tenant-name>"
        }
      }
      Config {
          <config-name> {
              <full-config>
          }
      }
    }
  }
}
```

Поле UsageScope необязательно, и нужно для применения настроек для определенного тенанта.

```
./kikimr -s <endpoint> admin console configs update <protobuf-file>
```
