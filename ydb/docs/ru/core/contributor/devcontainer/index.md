# Использование Dev Container для контрибьюторов клиентских SDK YDB

- [Официальная документация Dev Containers](https://containers.dev/)
- [Документация расширения VS Code Dev Containers](https://code.visualstudio.com/docs/devcontainers/containers)

Dev Container позволяет быстро развернуть воспроизводимую и изолированную среду для разработки и тестирования клиентских SDK YDB.

## Для чего использовать

- Не требуется ручная настройка окружения и зависимостей.
- Среда полностью совпадает с CI и другими участниками проекта.
- Можно запускать интеграционные тесты с локальной базой YDB без дополнительной подготовки.
- Все необходимые инструменты для разработки и тестирования уже установлены.

## Как начать работу

1. Установите контейнерный движок (например, Docker или Podman) и Visual Studio Code с расширением Dev Containers.
2. Клонируйте репозиторий нужного SDK.
3. Если в репозитории есть папка `.devcontainer`, откройте проект в VS Code и выберите команду **Reopen in Container**.
4. После запуска среды используйте стандартные команды для сборки, тестирования и запуска кода.

## Особенности реализации в SDK YDB

### Java SDK
- В [`devcontainer`](https://github.com/ydb-platform/ydb-java-sdk/tree/master/.devcontainer) есть `Dockerfile`, `devcontainer.json` и скрипты.
- Окружение включает JDK, Gradle и инструменты для разработки.
- Можно расширять Dockerfile для установки дополнительных средств.

### Go SDK
- В [`devcontainer`](https://github.com/ydb-platform/ydb-go-sdk/tree/master/.devcontainer) есть `Dockerfile`, `devcontainer.json`, `compose.yml` и скрипты.
- Автоматически поднимается локальный кластер YDB для интеграционных тестов.
- Все зависимости и нужная версия Go уже установлены.

### JavaScript/TypeScript SDK
- В [`devcontainer`](https://github.com/ydb-platform/ydb-js-sdk/tree/main/.devcontainer) есть `Dockerfile`, `devcontainer.json`, `compose.yml` и скрипты.
- Окружение включает Node.js, npm, TypeScript и инструменты для разработки.
- При запуске devcontainer автоматически поднимается кластер YDB для интеграционных тестов.

### Python SDK (PR #590)
- В [`devcontainer`](https://github.com/ydb-platform/ydb-python-sdk/pull/590/files) есть `Dockerfile`, `devcontainer.json`, `compose.yml`, скрипты и автоматическая настройка профиля ydb-cli.
- При запуске среды автоматически поднимается локальный кластер YDB, к которому сразу настроен доступ из контейнера.
- Все зависимости, flake8, tox, ydb-cli уже установлены.
