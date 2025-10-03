# Bridge skipper demo

**Не используйте данную утилиту в продакшене!**

YDB поддерживает работу в режиме [Bridge](https://ydb.tech/docs/ru/concepts/bridge?version=main). CLI утилита `skipper.py` реализует демо-версию `Bridge keeper'a`: выполняет мониторинг состояния кластера, управляет частями кластера, называемыми [pile](https://ydb.tech/docs/ru/concepts/glossary?version=main#pile), и отображает состояние кластера в TUI.

По умолчанию утилита автоматически выполняет failover отказавших частей кластера:
* переводит в режим `DISCONNECTED`
* при необходимости назначает здоровый pile `PRIMARY`

Ограничения demo-версии:
* поддержано только два пайла;
* состояние compute-части кластера не учитывается.

## Установка

Установка зависимостей:
```
pip3 install textual requests pyyaml
```

Для работы `skipper'a` требуется наличие YDB CLI. По умолчанию автоматически находит в `PATH`. Требуется установить YDB CLI из `main` или ветки `rc-bridge`. Также `ydbd` должен быть из `main` либо `rc-bridge`.

## Использование

Пример использования:
```
./skipper.py -e <EXAMPLE.COM> -s ~/ydb-test-bridge-state.json --cluster ydb-test-bridge --tui
```

Пример с дополнительными параметрами аутентификации:
```
./skipper.py --ydb-auth-opts "--user suser --password-file password.txt" -e <EXAMPLE.COM> -s ~/ydb-test-bridge-state.json --cluster ydb-test-bridge --tui
```

![Import](img/skipper_demo.gif)

## Параметры

| Параметр | По умолчанию | Описание |
|---|---|---|
| `--endpoint` | — | Хост YDB для получения информации о кластере. |
| `--state` | — | Путь к файлу, где skipper хранит свой стейт. |
| `--ydb` | из PATH | Путь к исполняемому файлу YDB CLI. |
| `--ydb-auth-opts` | - | Параметры аутентификации для YDB CLI. |
| `--disable-auto-failover` | false | Отключить автоматический failover. |
| `--log-level` | INFO | Уровень логирования: `TRACE`, `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`. |
| `--cluster` | cluster | Имя кластера для отображения. |
| `--tui` | false | Включить TUI. |
| `--tui-refresh` | 1.0 | Интервал обновления TUI в секундах. |
