# MNC

MNC разворачивает и обслуживает тестовые кластеры YDB на удалённых Linux-хостах.
Локальный CLI собирает бинарники и доставляет файлы по SSH; агент на каждом хосте
управляет процессами и дисками. `mnc_viewer` предоставляет терминальный интерфейс
для выбора конфигурации, установки кластера и просмотра состояния хостов.

Один YAML описывает кластер и разрешённые агенту диски. При полной установке
MNC проверяет наличие агента и устанавливает его, если он недоступен или не
поддерживает необходимые функции. Отдельный конфиг агента не требуется.

## Перед началом

- Нужна рабочая копия YDB с `ya`; сторонние Python-пакеты входят в сборку через `ya`.
- На хостах должны работать Linux, SSH и `parted`; для доставки файлов нужен `rsync`.
- Пользователь SSH должен иметь беспарольный `sudo` для операций с дисками и процессами.
- Агент должен быть доступен с машины, на которой запущен MNC, по HTTP на порту `8999`.
- При `use_home_dir: true` запускайте MNC и подключайтесь по SSH под одним именем
  пользователя: путь установки вычисляется как `/home/$USER/multinode_home`.

**Полная установка `install` удаляет существующие узлы MNC и стирает данные
на выбранных разделах.** Используйте её для создания или пересоздания стенда.
Для обновления существующего кластера доступны отдельные команды ниже.

Все команды далее выполняются из подкаталога `ydb` рабочей копии. В родительском
каталоге находятся `ya` и подкаталог `ydb`.

## Сборка

```bash
../ya make --build relwithdebinfo tools/mnc/cli tools/mnc/viewer tools/mnc/agent
```

Получатся три исполняемых файла:

- `tools/mnc/viewer/mnc_viewer` — терминальный интерфейс.
- `tools/mnc/cli/mnc` — CLI для установки и обслуживания.
- `tools/mnc/agent/mnc_agent` — агент, который MNC доставляет на хосты.

В `~/.mnc/mnc.yaml` задайте путь к корню рабочей копии, содержащему `ya` и `ydb`:

```yaml
git_ydb_root: /path/to/checkout
```

CLI спросит этот путь при первом запуске, если конфиг ещё не создан.
В viewer его можно указать в `Settings` → `YDB source root`.

## Пример конфигурации

[example.yaml](example.yaml) описывает стенд на одном хосте `example.com`:
четыре storage-узла, четыре виртуальных DC, `mirror-3-dc` и 16 NVMe-разделов.
Каждому storage-узлу назначаются четыре последовательных раздела из списка.
Разделы с суффиксом `_4` исключены. На одном физическом хосте виртуальные DC
не обеспечивают защиту от отказа этого хоста.

Перед использованием замените все вхождения `example.com`, включая
`overridden_configs.hosts[].host`, и задайте реальные метки разделов
из `/dev/disk/by-partlabel`.

В примере interconnect storage-узлов использует `localhost` и `::1`,
а MNC обращается к агенту по `example.com`. Такая конфигурация рассчитана
на один физический хост. Идентификаторы узлов, порты и расположения
в `overridden_configs.hosts` согласованы с четырьмя storage-процессами;
при изменении их количества или портов обновите и этот блок.
В примере разделы уже созданы: `disks_for_split` отсутствует, поэтому MNC
не переразмечает диски, но при установке очищает выбранные разделы.

| Поле | Назначение |
| --- | --- |
| `hosts` | Хосты, на которые устанавливается кластер. |
| `nodes_per_host` | Количество storage-процессов на каждом хосте. |
| `node_layout` | Виртуальные DC и стойки для storage-узлов. |
| `device_type` | Тип дисков и storage-ресурсов баз: `NVME`, `SSD` или `ROT`. |
| `sector_map.use: never` | Использовать реальные разделы без подстановки SectorMap. |
| `disks[].disks_for_use` | Метки готовых разделов; агент разрешает работу с ними для соответствующего хоста. |
| `domain.databases[].storage_group_count` | Количество storage-групп базы. |
| `domain.databases[].compute_unit_count` | Количество compute-процессов базы на всём кластере. |
| `overridden_configs` | Поля, добавляемые в итоговую конфигурацию YDB; в примере заданы `fail_domain_type: disk` и адреса interconnect storage-узлов. |
| `build_args` | Аргументы сборки YDB через `ya`; в примере `--build=relwithdebinfo`. |
| `deploy_flags` | Управление сборкой, strip и доставкой бинарника. |

В примере включены `do_rebuild` и `do_redeploy_bin`: первая установка соберёт
и доставит YDB. `do_not_strip` сохраняет бинарник без strip.

## Установка через mnc_viewer

Viewer ищет конфиги в `~/.mnc` и в `junk/$USER/.mnc` внутри корня рабочей копии.
Скопируйте подготовленный пример в первый каталог и запустите viewer:

```bash
mkdir -p ~/.mnc
cp tools/mnc/example.yaml ~/.mnc/example.yaml
tools/mnc/viewer/mnc_viewer
```

1. Нажмите `Ctrl+P` и выберите `Open tab: Settings`. Проверьте `YDB source root`.
   Для первой установки включите `Rebuild binary` и `Redeploy binary`,
   а `Strip binary` выключите, если хотите сохранить настройку примера.
   Флаги из `Settings` имеют приоритет над флагами выбранного YAML.
2. Через `Ctrl+P` откройте `Open tab: Cluster`, выберите `example` и нажмите `Enter`.
   Конфиг должен получить статус `OK`. Клавиша `e` открывает его в редакторе
   из `$EDITOR` (по умолчанию `vi`). Изменения этой копии сохраняются в `~/.mnc/example.yaml`.
3. Откройте `Open tab: Hosts`, чтобы посмотреть состояние агента, диски и последние
   задачи. Статус `Not Installed` перед первой установкой ожидаем: `Install`
   установит агент автоматически.
4. Нажмите `o`, выберите `Install`, задайте `waiting = 60`, оставьте `bin_path`
   пустым и `do_not_init` выключенным. Нажмите `Run`, когда готовы пересоздать стенд.
5. Следите за деревом шагов и результатом операции. MNC установит агент, соберёт
   и доставит YDB, очистит разделы, запустит storage-узлы, выполнит bootstrap,
   создаст базы и запустит compute-процессы.

Навигация: `Ctrl+P` — палитра команд, `t` — выбор уже открытой вкладки,
`[` и `]` — соседние вкладки, `Ctrl+W` — закрыть вкладку, `Ctrl+Q` — выйти.
Viewer сам создаёт временный рабочий каталог для операции.

## Ручная установка через CLI

Для полной установки достаточно общей конфигурации:

```bash
mkdir -p /tmp/mnc-example
tools/mnc/cli/mnc --tui install \
  --config-path tools/mnc/example.yaml \
  --wd /tmp/mnc-example \
  --waiting 60
```

`--tui` включает отображение дерева шагов и логов. Без этого флага команда
выполняет те же действия с обычным выводом. Запуск `tools/mnc/cli/mnc` без команды
открывает интерактивный выбор команды и конфигурации.

Если хотите проверить диски и итоговый YAML до полной установки,
сначала установите агент из той же конфигурации. `--do-not-build` использует
агент, уже собранный командой из раздела «Сборка»:

```bash
mkdir -p /tmp/mnc-example-agent /tmp/mnc-example
tools/mnc/cli/mnc agent install \
  --config-path tools/mnc/example.yaml \
  --wd /tmp/mnc-example-agent \
  --do-not-build

tools/mnc/cli/mnc disks check \
  --config-path tools/mnc/example.yaml \
  --wd /tmp/mnc-example

tools/mnc/cli/mnc configs generate \
  --config-path tools/mnc/example.yaml \
  --wd /tmp/mnc-example
```

`disks check` проверяет наличие выбранных разделов. `configs generate` читает
сведения о дисках через агент и создаёт локальный `/tmp/mnc-example/config.yaml`
и конфиги процессов в `/tmp/mnc-example/replaced`.
После успешной проверки запускайте полную установку из начала этого раздела.

`--config-path` принимает путь к YAML. Вместо него можно использовать
`--config example`, если конфиг находится в `~/.mnc` или `junk/$USER/.mnc`.
При ручной работе задавайте `--wd`: этот каталог хранит сгенерированные файлы
и полные логи операций. Без `--wd` CLI использует временный каталог и удаляет его
после завершения команды.

## Проверка результата

Для неизменённых портов примера ожидаются следующие адреса:

| Назначение | Адрес |
| --- | --- |
| gRPC первого storage-узла | `grpc://example.com:2135` |
| Мониторинг первого storage-узла | `http://example.com:8765/` |
| Первый compute-процесс `/Root/db1` | `grpc://example.com:20003` |
| Compute-процесс `/Root/db2` | `grpc://example.com:20011` |

База `/Root/db1` получит четыре storage-группы и восемь compute-процессов,
а `/Root/db2` — одну storage-группу и один compute-процесс.
Состояние агента, сведения о дисках и последние задачи доступны на вкладке
`Hosts` viewer. Замените `example.com` в адресах на свой хост.

## Обслуживание существующего кластера

Обновить бинарник и затем последовательно перезапустить storage- и compute-узлы:

```bash
tools/mnc/cli/mnc deploy update_bin \
  --config-path tools/mnc/example.yaml --wd /tmp/mnc-example

tools/mnc/cli/mnc service nodes rolling_restart --type all \
  --config-path tools/mnc/example.yaml --wd /tmp/mnc-example
```

После изменения YAML сгенерировать и доставить конфигурацию существующих
storage-узлов:

```bash
tools/mnc/cli/mnc configs generate \
  --config-path tools/mnc/example.yaml --wd /tmp/mnc-example

tools/mnc/cli/mnc deploy update_cfg \
  --config-path tools/mnc/example.yaml --wd /tmp/mnc-example
```

Остановить или запустить процессы кластера без полной переустановки:

```bash
tools/mnc/cli/mnc service hosts stop --config-path tools/mnc/example.yaml
tools/mnc/cli/mnc service hosts start --config-path tools/mnc/example.yaml
```

`deploy install` — нижний уровень установки файлов из уже сгенерированных
конфигов. Для полного создания стенда используйте команду верхнего уровня `install`.
В старых notebook ей соответствовала `fullcycle promote`.

Для справки используйте `tools/mnc/cli/mnc --help` и справку подкоманд, например
`tools/mnc/cli/mnc service nodes --help`. Флаги `--verbose` / `-V` и `--quiet` / `-q`
задаются перед командой.

## Диагностика

- Ошибка схемы: проверьте сообщение CLI или статус `FAIL` на вкладке `Cluster`.
  Устаревшее `ydb_config_type` удалите; `fail_domain_type` задавайте внутри
  `overridden_configs`, как в примере.
- Агент недоступен: проверьте SSH, `sudo` и доступ к порту `8999`.
  Вывод агента находится в `/home/$USER/multinode_home/mnc_agent/stdout`.
- Метка раздела не найдена: сверьте YAML с `/dev/disk/by-partlabel` на хосте.
  Разрешения агента определяются списками дисков только для этого хоста.
  После изменения этих списков переустановите агент через `agent install`
  с той же общей конфигурацией.
- Для деталей сбоя CLI использует каталог `logs` внутри `--wd` и показывает
  путь к полному логу в сообщении об ошибке. Вывод YDB-процессов на хосте находится
  в `/home/$USER/multinode_home/ydb_node_static_N/` и `ydb_node_dynamic_N/`
  в файлах `stdout` и `stderr`.
