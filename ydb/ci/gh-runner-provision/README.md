# GitHub runner provision

`shapes/` описывает только VM: платформу, CPU, память, диск. `fleet/` говорит, сколько таких машин нужно и с какими лейблами. `poll` читает весь `fleet/` и сверяет его с GitHub. Статики, analytic и tiny лежат там рядом. `fat` и `slim` — только `shapes/` плюс `configs/` для ручного `create`; в `fleet/` их нет, `poll`/`reconcile` их не считают. Лейбл `ghrun` в этот каталог не входит: им занимается автоскейлер.

```bash
python3 ydb/ci/gh-runner-provision/provision.py poll
python3 ydb/ci/gh-runner-provision/provision.py reconcile
python3 ydb/ci/gh-runner-provision/provision.py reconcile --apply
python3 ydb/ci/gh-runner-provision/provision.py create ydb/ci/gh-runner-provision/fleet/tiny.yaml --name tiny-worker-2026-10-13 --dry-run
python3 ydb/ci/gh-runner-provision/provision.py register ydb/ci/gh-runner-provision/fleet/tiny.yaml --name tiny-worker-2026-10-13
python3 ydb/ci/gh-runner-provision/provision.py labels ydb/ci/gh-runner-provision/fleet/tiny.yaml --name tiny-worker-2026-10-13
```

У пула в `fleet/` нет списка машин. `name` — префикс, из которого `reconcile` берёт следующий свободный номер. Раннер принадлежит пулу по служебному лейблу `pool:<имя файла>`, его ставит `create` и первый `reconcile`. Пока этот лейбл не проставлен, привязка идёт по точному совпадению `labels:`. Сначала один раз запусти `reconcile --apply`, и только потом меняй лейблы в файле: иначе старые раннеры не узнаются и будут созданы новые.

`reconcile` без `--apply` только печатает план. С `--apply` он сначала проставляет `pool:` и правит лейблы через GitHub API, затем снимает лишних (сначала offline и пропавшие VM, занятого джобой не трогает). Перед удалением и перед рестартом сервиса он читает раннер ещё раз: занятого или уже online не рестартует, занятого не удаляет. ВМ удаляется до конца, и только потом имя можно занять снова. После холодного старта ждёт до 5 минут. Не больше одной замены на конфигурацию, если раннер так и не стал online. Недостающих создаёт до `count`. На ВМ в Облаке пишется метка `pool=<имя конфигурации>` рядом с `purpose=gh-runner`. Уже существующим машинам `reconcile --apply` ставит её сам. ВМ без регистрации в GitHub забирается по этой метке, даже если начало имени общее для нескольких конфигураций. Без метки и с общим началом имени такая ВМ только печатается. Конфигурация, которой больше нет в `fleet/`, машины не удаляет: такие раннеры печатаются как orphan.

Workflow `Runner fleet check` только вручную: без галки `apply` печатает план, с галкой делает `--apply`.

`create`, `register` и `labels` по файлу конфигурации работают с одной VM, имя передаётся `--name`. Форма и лейблы берутся из файла. `labels` ставит тот же набор, что и `create`, включая `pool:`. `register` занятого раннера не перерегистрирует. Для смены лейблов у живого раннера достаточно `labels`.

`register` заново снимает и записывает регистрацию раннера. Для смены лейблов он не нужен. Если раннер сейчас занят джобой, `register` ничего не делает.

Лейбл `ghrun` не ставить на долгоживущие машины: автоскейлер удаляет простаивающие раннеры с этим лейблом. `instance:<id>` добавляется сам. `self-hosted`, `Linux`, `X64` добавляет сам GitHub Actions runner. В unit ставится `Restart=always`.

Диск `network-ssd-nonreplicated` должен быть кратен 93 ГБ. Облако, подсеть, образ и SSH-пути можно не писать: они уже стоят по умолчанию на текущий CI-фолдер. Workflow берёт cloud и folder из переменных `YC_CLOUD_ID` и `YC_FOLDER_ID`. Если задан `YC_FOLDER_ID`, скрипт использует его вместо folder по умолчанию. Публичный адрес: `public_ip: true` и, если нужен конкретный, `public_address`.

Нужны `yc`, `gh` (уже авторизованный) и SSH-ключ, который попадает в `ubuntu` на VM. По умолчанию прыжок идёт через хост `gh-runners-gw` из `~/.ssh/config`. Cloud-init только готовит хост. Регистрация в GitHub идёт отдельно по SSH.

Ключи чинилки лежат в секретах репозитория, не в git: приватный `GH_RUNNER_RECONCILE_SSH_KEY`, публичный `GH_RUNNER_RECONCILE_SSH_PUB`. К API раннеров workflow ходит секретом `GH_YDB_RUNNERS_PAT`: его пишут в `GH_TOKEN`, автоматический токен джобы раннеры не администрирует. Workflow перед проверкой пишет их в `~/.ssh/id_ed25519` и `~/.ssh/id_ed25519.pub`. `create` кладёт этот `.pub` в cloud-init новой VM, поэтому после замены чинилка заходит на неё тем же ключом через `gh-runners-gw`. Если файла `.pub` нет, публичная половина берётся из приватного ключа. С ноутбука по умолчанию используется своя пара `~/.ssh/id_ed25519`. Если в окружении задан `GH_RUNNER_RECONCILE_SSH_PUB`, эта публичная строка тоже записывается на новую ВМ, и workflow потом заходит своим ключом. Другая пара задаётся полями `ssh_public_key` и `ssh_identity`. Дополнительные публичные ключи — список путей `ssh_public_keys`. На уже созданную VM `authorize` заходит ключом `ssh_identity` и дописывает недостающие строки в `authorized_keys`. Строки, которые уже там есть, не удаляются:

```bash
python3 ydb/ci/gh-runner-provision/provision.py authorize path/to/runners.yaml --dry-run
python3 ydb/ci/gh-runner-provision/provision.py authorize path/to/runners.yaml
```

```yaml
ssh_public_keys:
  - ~/.ssh/id_ed25519_ci.pub
```

```yaml
# shapes/tiny.yaml
platform: standard-v4a
cores: 4
memory_gb: 8
disk_gb: 186
preset: tiny-worker
```

```yaml
# fleet/tiny.yaml
shape: ../shapes/tiny.yaml
name: tiny-worker-2026-10
count: 6
labels:
  - tiny-worker
```

`count: 1` без `start` и без `names` берёт `name` как есть. Иначе имена будут `name-8`, `name-9`.
