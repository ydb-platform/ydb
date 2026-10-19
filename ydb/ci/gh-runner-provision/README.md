# GitHub runner provision

Создаёт долгоживущую VM раннера из одного конфига. Cloud-init только готовит хост (пользователи, ssh, sysctl, rdma). Регистрация в GitHub идёт отдельно по SSH, поэтому смена лейблов — это `register`, без новой VM.

Нужны `yc`, `gh` (уже авторизованный) и SSH-ключ, который попадает в `ubuntu` на VM. По умолчанию прыжок идёт через хост `gh-runners-gw` из `~/.ssh/config`.

```bash
python3 ydb/ci/gh-runner-provision/provision.py create ydb/ci/gh-runner-provision/examples/tiny-worker.yaml --dry-run
python3 ydb/ci/gh-runner-provision/provision.py register ydb/ci/gh-runner-provision/examples/static-relwithdebinfo-release.yaml --dry-run
python3 ydb/ci/gh-runner-provision/provision.py create path/to/runners.yaml
python3 ydb/ci/gh-runner-provision/provision.py register path/to/runners.yaml
```

Один файл — один размер VM и один набор лейблов. `platform` — поле в этом файле. Имя файла его не кодирует.

`name` плюс `start` и `count` нарезает новые имена (`tiny-worker-2026-10-8`). `names` — готовый список уже существующих хостов. Статики так и устроены: `build-preset-relwithdebinfo` есть у всех, имя файла начинается с `static-relwithdebinfo`, дальше идёт то, чем группа отличается. Перенести хост в другую группу — убрать его из одного файла, добавить в другой и выполнить `register` на обоих.

`register` останавливает сервис, снимает регистрацию и записывает лейблы из конфига заново. Если раннер сейчас занят джобой, команда ничего не делает.

Лейбл `ghrun` не ставить на долгоживущие машины: автоскейлер удаляет простаивающие раннеры с этим лейблом. `instance:<id>` добавляется сам. `self-hosted`, `Linux`, `X64` добавляет сам GitHub Actions runner. В unit ставится `Restart=always`.

Диск `network-ssd-nonreplicated` должен быть кратен 93 ГБ. Облако, подсеть, образ и SSH-пути можно не писать: они уже стоят по умолчанию на текущий CI-фолдер. Публичный адрес: `public_ip: true` и, если нужен конкретный, `public_address`.

```yaml
name: tiny-worker-2026-10
start: 8
count: 2
platform: standard-v4a
cores: 4
memory_gb: 8
disk_gb: 186
labels:
  - tiny-worker
```

`count: 1` без `start` берёт `name` как есть. Иначе имена будут `name-8`, `name-9`.
