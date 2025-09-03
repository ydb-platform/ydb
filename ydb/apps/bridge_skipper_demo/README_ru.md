# Bridge skipper demo

**Не используйте данную утилиту в продакшене!**

YDB поддерживает работу в режиме [Bridge](https://ydb.tech/docs/ru/concepts/bridge?version=main). CLI утилита `skipper.py` реализует демо-версию `Bridge skipper'a`: выполняет мониторинг состояния кластера, управляет частями кластера, называемыми [pile](https://ydb.tech/docs/ru/concepts/glossary?version=main#pile), и отображает состояние кластера в TUI.

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
./skipper.py -e <EXAMPLE.COM> --cluster ydb-test-bridge --tui
```

![Import](img/skipper_demo.gif)

## Параметры

| Параметр | По умолчанию | Описание |
|---|---|---|
| `--endpoint` | — | Хост YDB для получения информации о кластере. |
| `--ydb` | из PATH | Путь к исполняемому файлу YDB CLI. |
| `--disable-auto-failover` | false | Отключить автоматический failover. |
| `--log-level` | INFO | Уровень логирования: `TRACE`, `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`. |
| `--cluster` | cluster | Имя кластера для отображения. |
| `--tui` | false | Включить TUI. |
| `--tui-refresh` | 1.0 | Интервал обновления TUI в секундах. |
| `--https` | false | Использовать HTTPS для healthcheck-запросов viewer. |

