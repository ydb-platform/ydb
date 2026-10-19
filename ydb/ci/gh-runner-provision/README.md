# GitHub runner provision

Создаёт долгоживущую VM раннера из одного конфига. Cloud-init только готовит хост (пользователи, ssh, sysctl, rdma). Регистрация в GitHub идёт отдельно по SSH, поэтому смена лейблов — это `register`, без новой VM.

Нужны `yc`, `gh` (уже авторизованный) и SSH-ключ, который попадает в `ubuntu` на VM. По умолчанию прыжок идёт через хост `gh-runners-gw` из `~/.ssh/config`.

```bash
python3 ydb/ci/gh-runner-provision/provision.py create ydb/ci/gh-runner-provision/examples/tiny-worker.yaml --dry-run
python3 ydb/ci/gh-runner-provision/provision.py create path/to/runners.yaml
python3 ydb/ci/gh-runner-provision/provision.py register path/to/runners.yaml
```

`register` останавливает сервис, снимает регистрацию и записывает лейблы из конфига заново. Если раннер сейчас занят джобой, команда ничего не делает.

Лейбл `instance:<id>` добавляется сам. `self-hosted`, `Linux`, `X64` добавляет сам GitHub Actions runner. В unit ставится `Restart=always`.

Диск `network-ssd-nonreplicated` должен быть кратен 93 ГБ. Облако, подсеть, образ и SSH-пути можно не писать: они уже стоят по умолчанию на текущий CI-фолдер.

```yaml
name: tiny-worker-2026-10
start: 8
count: 2
platform: standard-v3
cores: 4
memory_gb: 8
disk_gb: 186
labels:
  - tiny-worker
```

`count: 1` без `start` берёт `name` как есть. Иначе имена будут `name-8`, `name-9`. Готовый список имён задаётся ключом `names` вместо `name`.
