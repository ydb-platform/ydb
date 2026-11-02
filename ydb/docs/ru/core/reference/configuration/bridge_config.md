# bridge_config {#bridge-config}

Секция `bridge_config` настраивает [pile](../../concepts/glossary.md#pile) и смешанное хранилище для [режима bridge](../../concepts/bridge.md). Используйте её для объявления имён pile и, при необходимости, совместного использования пулов в режиме bridge и обычных пулов.

Каждый хост должен указывать свой pile в секции `hosts` с помощью поля `location.bridge_pile_name`; см. [Особенности режима bridge](hosts.md#hosts-bridge). Узлы читают `bridge_config` и собственный параметр `location` только при запуске, поэтому после изменения этих настроек требуется rolling restart статических и динамических узлов.

## Синтаксис {#syntax}

```yaml
bridge_config:
  piles:
  - name: <pile_name_1>
  - name: <pile_name_2>
  ...
  - name: <pile_name_n>
  enable_non_uniform_storage: <true_or_false>
```

Где:

- `<pile_name_1>`, `<pile_name_2>` и `<pile_name_n>` — уникальные имена pile, например `pile-a`, `pile-b` и `pile-c`;
- `<true_or_false>` — `true` для включения неоднородного хранилища или `false`, чтобы оставить его отключённым.

## Параметры {#parameters}

| Параметр | Значение по умолчанию | Описание |
| --- | --- | --- |
| `piles` | `[]` | Список pile, настроенных в кластере. Зарегистрированный pile без выделенных ресурсов находится в состоянии `INACTIVE`. |
| `enable_non_uniform_storage` | `false` | Включает [неоднородное хранилище](../../concepts/bridge.md#storage-pools), позволяя использовать пулы в режиме bridge и обычные пулы в одном кластере. После активации настройку нельзя отключить. |

При преобразовании обычного кластера по умолчанию не задавайте `enable_non_uniform_storage` до завершения преобразования. Преобразование с `--skip-storage-pool-conversion` требует значения `true` до его запуска. Процедура описана в разделе [Преобразование кластера](../../devops/deployment-options/manual/bridge-management.md#convert).
