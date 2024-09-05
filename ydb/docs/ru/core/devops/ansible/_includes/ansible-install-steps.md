1. [Роль](https://github.com/ydb-platform/ydb-ansible/blob/main/roles/packages/tasks/main.yaml) `packages`. Задачи:

* `check dpkg audit` – проверка состояния dpkg с помощью команды `dpkg --audit` и сохранение результатов команды в переменной `dpkg_audit_result`. Задача завершится с ошибкой, если команда `dpkg_audit_result.rc` вернет значение отличное от 0 или 1.
* `run the equivalent of "apt-get clean" as a separate step` – очистка кеша apt, аналогично команде `apt-get clean`.
* `run the equivalent of "apt-get update" as a separate step` – обновление кеша apt, аналогично команде `apt-get update`.
* `fix unconfigured packages` – исправление неконфигурированных пакетов с помощью команды `dpkg --configure --pending`.
* `set vars_for_distribution_version variables` – установка переменных для конкретной версии дистрибутива.
* `setup apt repositories` – настройка репозиториев apt из заданного списка.
* `setup apt preferences` – настройка предпочтений apt (содержание переменных указано в `roles/packages/vars/distributions/<distributive name>/<version>/main.yaml`).
* `setup apt configs`– настройка конфигураций apt.
* `flush handlers` – принудительный запуск всех накопленных обработчиков (хендлеров). В данном случае запускается хендлер, который обновляет кеш apt.
* `install packages` – установка пакетов apt с учетом заданных параметров и времени валидности кеша.

Ссылки на списки пакетов, которые будут установлены для Ubuntu 22.04 или Astra Linux 1.7:

* [Список](https://github.com/ydb-platform/ydb-ansible/blob/main/roles/packages/vars/distributions/Ubuntu/22.04/main.yaml) пакетов для Ubuntu 22.04;
* [Список](https://github.com/ydb-platform/ydb-ansible/blob/main/roles/packages/vars/distributions/Astra%20Linux/1.7_x86-64/main.yaml) пакетов для Astra Linux 1.7.

1. [Роль](https://github.com/ydb-platform/ydb-ansible/blob/main/roles/system/tasks/main.yaml) `system`. Задачи:

* `configure clock` – блок задач настройки системных часов:
  + `assert required variables are defined` – проверка переменной `system_timezone` на существование. Эта проверка гарантирует, что необходимая переменная доступна для следующей задачи в блоке.
  + `set system timezone` – установка системного часового пояса. Часовой пояс задаётся значением переменной `system_timezone`, а аппаратные часы (`hwclock`) устанавливаются на UTC. После выполнения задачи отправляется уведомление для перезапуска службы `cron`.
  + `flush handlers` – принудительное выполнение накопленных обработчиков директивой `meta`. Будет произведен рестарт следующих процессов: `timesyncd`, `journald`, `cron`, `cpufrequtils`, и выполнена команда `sysctl -p`.
* `configure systemd-timesyncd` – блок задач настройки `systemd-timesyncd`:
  + `assert required variables are defined` - утверждает, что количество NTP серверов (`system_ntp_servers`) больше одного, если переменная `system_ntp_servers` определена. Если переменная `system_ntp_servers` не определена, выполнение блока задач `configure systemd-timesyncd` будет пропущено, включая проверку количества NTP серверов и настройку `systemd-timesyncd`.
  + `create conf.d directory for timesyncd` - создаёт директорию `/etc/systemd/timesyncd.conf.d`, если переменная `system_ntp_servers` определена.
  + `configure systemd-timesyncd` - создаёт конфигурационный файл `/etc/systemd/timesyncd.conf.d/ydb.conf` для службы `systemd-timesyncd` с основным и резервными NTP серверами. Задача будет выполнена, если переменная `system_ntp_servers` определена. После выполнения задачи отправляется уведомление для перезапуска службы `timesyncd`.
  + `flush handlers` - вызываются накопленные обработчики. Выполняется обработчик `restart timesyncd`, который выполняет перезапуск сервиса `systemd-timesyncd.service`.
  + `start timesyncd` - запуск и активация службы `systemd-timesyncd.service`. Далее служба будет стартовать автоматически при загрузке системы.
* `configure systemd-journald` – блок задач по настройке сервиса `systemd-journald`:
  + `create conf.d directory for journald` - создание директории `/etc/systemd/journald.conf.d` для хранения конфигурационных файлов `systemd-journald`.
  + `configure systemd-journald` - создание конфигурационного файла в `/etc/systemd/journald.conf.d/ydb.conf`  для `systemd-journald`, в котором указывается секция `Journal` с опцией `ForwardToWall=no`. Параметр `ForwardToWall=no` в конфигурации `systemd-journald` означает, что журнал сообщений (логи) системы не будет перенаправляться как сообщения "wall" всем вошедшим в систему пользователям. После выполнения задачи отправляется уведомление для перезапуска службы `journald`.
  + `flush handlers` - вызывает накопленные хендлеры. Будет выполнен хендлер `restart journald`, который перезапустит сервис `systemd-journald`.
  + `start journald` - запуск и активация службы `systemd-journald.service`. Далее служба будет стартовать автоматически при загрузке системы.
* `configure kernel` – блок задач конфигурирования ядра:
  + `configure /etc/modules-load.d dir` - создание директории `/etc/modules-load.d` с правами владельца и группы для пользователя root и разрешениями `0755`.
  +  `setup conntrack module` - копирование строки `nf_conntrack` в файл `/etc/modules-load.d/conntrack.conf` для загрузки модуля `nf_conntrack` при старте системы.
  + `load conntrack module` - загрузка модуля `nf_conntrack` в текущей сессии.
  + `setup sysctl files` - применяет шаблоны для создания конфигурационных файлов в `/etc/sysctl.d/` для различных системных настроек (таких, как настройки безопасности, сети и файловой системы). Список файлов включает `10-console-messages.conf`, `10-link-restrictions.conf` и другие. После выполнения этой задачи отправляется уведомление для применения изменений в настройках ядра.
  + `flush handlers` - вызывает накопленные хендлеры. Будет вызван хендлер  `apply kernel settings`, который выполнит команду `sysctl -p` для применения параметров ядра, указанных в файле `/etc/sysctl.conf` или в других файлах в каталоге `/etc/sysctl.d/`.
* `configure cpu governor` – блок задач настройки режима управления частотой процессора:
  + `install cpufrequtils` - установка пакета `cpufrequtils` из apt. В задаче установлены параметры проверки кеша apt и таймаут выполнения задачи в 300 секунд, чтобы ускорить выполнения задачи и избежать бесконечного цикла ожидания обновления пакетов apt.
  + `use performance cpu governor` - создание файла `/etc/default/cpufrequtils` с содержимым "GOVERNOR=performance", что устанавливает режим работы CPU governor в "performance" (Отключения режима энергосбережения при простое ядер CPU). После выполнения задачи отправляется уведомление для перезапуска службы `cpufrequtils`.
  + `disable ondemand.service` - отключение сервиса `ondemand.service`, если он присутствует в системе. Сервис останавливается, его автоматический запуск отключается, и он маскируется (предотвращается его запуск). После выполнения задачи отправляется уведомление для перезапуска cpufrequtils.
  + `flush handlers` - вызывает накопленные хендлеры. Будет вызван хендлер `restart cpufrequtils`, который перезапустит сервис `cpufrequtils`.
  + `start cpufrequtils` - запуск и активация службы `cpufrequtils.service`. Далее служба будет стартовать автоматически при загрузке системы.

1. [Роль](https://github.com/ydb-platform/ydb-ansible/blob/main/roles/ydbd/tasks/main.yaml) `ydbd`. Задачи:

* `check if required variables are defined` – проверка, что переменные `ydb_archive`, `ydb_config`, `ydb_tls_dir` определены. Если какая-либо из них не определена, Ansible выведет соответствующее сообщение об ошибке и остановит выполнение плейбука.
* `set vars_for_distribution variables` – установка переменных из указанного файла в переменной `vars_for_distribution_file` во время выполнения плейбука. Задача управляет набором переменных, зависящих от конкретного дистрибутива Linux.
* `ensure libaio is installed` – проверка, что пакет `libaio` установлен.
* `install custom libidn from archive` – установка пользовательской версии библиотеки `libidn` из архива.
* `create certs group` – создание системной группы `certs`.
* `create ydb group` – создание системной группы `ydb`.
* `create ydb user` – создание системного пользователя `ydb` с домашней директорией.
* `install YDB server binary package from archive` – установка YDB из скаченного архива.
* `create YDB audit directory` – создание поддиректории `audit` в директории установки YDB.
* `setup certificates` – блок задач по установки сертификатов безопасности:

  + `create YDB certs directory` – создание поддиректории `certs` в директории установки YDB.
  + `copy the TLS ca.crt` – копирование корневого сертификата `ca.crt` на сервер.
  + `copy the TLS node.crt` – копирование TLS-сертификата `node.crt` из директории сгенерированных сертификатов.
  + `copy the TLS node.key` – копирование TLS-сертификата `node.key` из директории сгенерированных сертификатов.
  + `copy the TLS web.pem` – копирование TLS pem ключа `web.pem` из директории сгенерированных сертификатов.

* `copy configuration file` – копирование конфигурационного файла `config.yaml` на сервер.
* `add configuration file updater script` – копирование скрипта `update_config_file.sh` на сервер.

1. [Роль](https://github.com/ydb-platform/ydb-ansible/blob/main/roles/ydbd_static/tasks/main.yaml) `ydbd_static`. Задачи:

* `check if required variables are defined` – проверка, что переменные `ydb_cores_static`, `ydb_disks`, `ydb_domain`, `ydb_user` определены. Если хотя бы одна из этих переменных не определена, задача завершится с ошибкой, и будет выведено соответствующее сообщение об ошибке для каждой переменной, которая не была определена.
* `check if required secrets are defined` – проверка определения секретной переменной `ydb_password`. Если эта переменная не определена, задача завершится с ошибкой, и будет выведено сообщение об ошибке.
* `create static node configuration file` – создание конфигурационного файла статической ноды путём запуска скопированного скрипта `update_config_file.sh` и передачи в него конфигураций `ydbd-config.yaml`, `ydbd-config-static.yaml`.
* `create static node systemd unit` – создание файла `ydbd-storage.service` для статического узла на основе шаблона. После выполнения задачи отправляется уведомление для перезапуска службы `systemd`.
* `flush handlers` – выполнение накопленных хендлеров. Выполняется перезапуск всех служб `systemd`.
* `format drives confirmation block` – блок задач по форматированию дисков и прерывания выполнения плейбука в случае отказа пользователя от подтверждения. В терминал будет выведен запрос на подтверждение форматирования подключенного к серверу диска. Варианты ответа: `yes` – продолжить выполнение плейбука с форматированием диска. Любое другое значение, будет восприниматься как отказ от форматирования. По умолчанию диски форматируется автоматически без запроса разрешения у пользователя, так как переменные `ydb_allow_format_drives` и `ydb_skip_data_loss_confirmation_prompt` равны `true`. Если нужно запросить подтверждение от пользователя, то нужно изменить значение переменной `ydb_skip_data_loss_confirmation_prompt` на `false` в инвентаризационном файле `50-inventory.yaml`.
* `prepare drives` – задача по форматированию подключенных дисков. Вызывается плагин `drive_prepare` – это специально разработанный Ansible-модуль для установки YDB, который входит в состав поставки коллекции YDB и располагается в директории `.../.ansible/collections/ansible_collections/ydb_platform/ydb/plugins/action/drive_prepare.py`. Модуль выполнит форматирование подключенного к серверу диска, указанного в переменной `ydb_disks`. Форматирование будет произведено если переменная `ydb_allow_format_drives` имеет значение `true`.
* `start storage node` – запуск процесса сторадж ноды с помощью `systemd`. В случае возникновения любых ошибок запуска сервиса исполнение плейбука будет прервано.
* `get ydb token` – запрос токена YDB для выполнения команды инициализации стораджа. Токен сохраняется в переменной `ydb_credentials`. В задаче вызывается модуль `get_token` из директории `.../.ansible/collections/ansible_collections/ydb_platform/ydb/plugins/modules`. В случае возникновение любых ошибок на данном шаге выполнение плейбука будет прервано.
* `wait for ydb discovery to start working locally` – вызывается модуль `wait_discovery`, который выполняет  запрос `ListEndpoints` к YDB для проверки работоспособности базовых подсистем кластера. Если подсистемы работают исправно – можно выполнять команды инициализации стороджа и создания базы данных.
* `init YDB storage if not initialized` – инициализация хранилища в случае если оно еще не создано. В задаче вызывается плагин `init_storage`, который выполняет команду инициализации хранилища с помощью grpcs-запроса к статической ноде на порт 2135. Результат выполнения команды сохраняется в переменной `init_storage`.
* `wait for ydb healthcheck switch to "GOOD" status` – ожидание получения статуса `GOOD` от системы проверки состояния YDB. В задаче вызывается плагин `wait_healthcheck`, который выполняет команду проверке состояния YDB.
* `set cluster root password` – установка пароля для root пользователя YDB. В задаче выполняется плагин `set_user_password`, который выполняет grpcs запрос к YDB и устанавливает заранее заданный пароль для root пользователя YDB. Пароль задаётся переменной `ydb_password` в инвентаризационном файле `/examples/9-nodes-mirror-3-dc/inventory/99-inventory-vault.yaml` в зашифрованном виде.

1. [Роль](https://github.com/ydb-platform/ydb-ansible/blob/main/roles/ydbd_dynamic/tasks/main.yaml) `ydbd_dynamic`. Задачи:

*  `check if required variables are defined` – проверка наличия установленных переменных (`ydb_domain`,`ydb_pool_kind`, `ydb_cores_dynamic`, `ydb_brokers`, `ydb_dbname`, `ydb_dynnodes`) и вывод ошибки в случае отсутствия любой из переменных.
* `create dynamic node configuration file` – создание конфигурационного файла для динамических нод.
* `create dynamic node systemd unit` – создание сервиса для systemd динамических нод. После выполнения задачи отправляется уведомление для перезапуска службы `systemd`.
* `flush handlers` – выполнение накопившихся хендлеров. Будет выполнен рестарт `systemd`.
* `start dynamic nodes` – запуск процесса динамических нод с помощью `systemd`.
* `get ydb token` – получение токена для создания базы данных.
* `create YDB database` – создание базы данных. В задаче выполняется плагин `create_database`, который выполняет запрос создания БД к YDB.
* `wait for ydb discovery to start working locally` – повторно вызывается модуль `wait_discovery` для проверки работоспособности базовых подсистем кластера.