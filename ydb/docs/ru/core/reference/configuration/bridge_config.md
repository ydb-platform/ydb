# bridge_config {#bridge-config}

Секция описывает pile кластера и режимы хранения для [режима bridge](../../concepts/bridge.md). Для каждого хоста необходимо указать имя соответствующего pile в секции `hosts` с помощью поля `location.bridge_pile_name`, см. [особенности режима bridge](hosts.md#hosts-bridge). Узлы читают `bridge_config` и собственный `location` только при запуске, поэтому после изменения этих настроек необходимо выполнить скользящий перезапуск статических и динамических узлов.

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
| `enable_non_uniform_storage` | `false` | Позволяет базам данных с [обычным хранилищем и хранилищем в режиме bridge](../../concepts/bridge.md#storage-pools) существовать в одном кластере одновременно. После активации эту настройку нельзя отключить. |

При преобразовании обычного кластера по умолчанию не задавайте `enable_non_uniform_storage` до завершения преобразования. Преобразование с `--skip-storage-pool-conversion` требует установить этот параметр в `true` до начала преобразования. Подробнее см. в разделе [Преобразование кластера](../../devops/deployment-options/manual/bridge-management.md#convert).
