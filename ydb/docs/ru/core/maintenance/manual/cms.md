# Изменение конфигураций через CMS

## Получить текущие настройки

Следующая команда позволит получить текущие настройки по кластеру или по тенанту.

```bash
ydbd -s <endpoint> admin console configs load --out-dir <config-folder>
```

```bash
ydbd -s <endpoint> admin console configs load --out-dir <config-folder> --tenant <tenant-name>
```

## Обновить настройки

Сначала надо выкачать нужный конфиг как указано выше, после чего требуется подготовить protobuf файл с запросом на изменение.

```proto
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

```bash
ydbd -s <endpoint> admin console configs update <protobuf-file>
```
