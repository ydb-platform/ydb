# GitHub runner provision

`shapes/` описывает только VM: платформу, CPU, память, диск. `fleet/` говорит, сколько таких машин нужно и с какими лейблами. `poll` читает весь `fleet/` и сверяет его с GitHub. Статики, analytic и tiny лежат там рядом. Лейбл `ghrun` в этот каталог не входит: им занимается автоскейлер.

```bash
python3 ydb/ci/gh-runner-provision/provision.py poll
python3 ydb/ci/gh-runner-provision/provision.py labels ydb/ci/gh-runner-provision/fleet/tiny.yaml --dry-run
python3 ydb/ci/gh-runner-provision/provision.py create ydb/ci/gh-runner-provision/fleet/tiny.yaml --dry-run
```

У статического пула `count` равен числу `names`. Пропавшее имя чинится на месте, новый номер не выдаётся. У tiny и analytic тоже перечислены конкретные имена: поллинг проверяет и количество, и что online именно они.

`labels` приводит custom labels раннера к списку из файла fleet через GitHub API. Сервис не останавливается, раннер может быть занят джобой. `self-hosted`, `Linux`, `X64` команда не трогает. `instance:<id>` оставляет. Перенести хост в другую группу — убрать его имя из одного файла fleet, добавить в другой и поправить оба `count`.

`register` заново снимает и записывает регистрацию раннера. Для смены лейблов он не нужен. Если раннер сейчас занят джобой, `register` ничего не делает.

Лейбл `ghrun` не ставить на долгоживущие машины: автоскейлер удаляет простаивающие раннеры с этим лейблом. `instance:<id>` добавляется сам. `self-hosted`, `Linux`, `X64` добавляет сам GitHub Actions runner. В unit ставится `Restart=always`.

Диск `network-ssd-nonreplicated` должен быть кратен 93 ГБ. Облако, подсеть, образ и SSH-пути можно не писать: они уже стоят по умолчанию на текущий CI-фолдер. Публичный адрес: `public_ip: true` и, если нужен конкретный, `public_address`.

Нужны `yc`, `gh` (уже авторизованный) и SSH-ключ, который попадает в `ubuntu` на VM. По умолчанию прыжок идёт через хост `gh-runners-gw` из `~/.ssh/config`. Cloud-init только готовит хост. Регистрация в GitHub идёт отдельно по SSH.

Сам ключ в репозиторий не входит. По умолчанию на пользователя `ubuntu` кладётся публичная половина `~/.ssh/id_ed25519.pub`, а команда ходит приватной `~/.ssh/id_ed25519`. Другая пара, которой ходит сам провижинер, задаётся полями `ssh_public_key` и `ssh_identity`. Дополнительные публичные ключи, которыми тоже можно зайти, — список путей `ssh_public_keys`. `create` кладёт основной ключ и дополнительные в cloud-init новой VM. На уже созданную VM `authorize` заходит ключом `ssh_identity` и дописывает недостающие строки в `authorized_keys`. Строки, которые уже там есть, не удаляются:

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
count: 6
names:
  - tiny-worker-2026-10-3
labels:
  - tiny-worker
```

`count: 1` без `start` и без `names` берёт `name` как есть. Иначе имена будут `name-8`, `name-9`.
