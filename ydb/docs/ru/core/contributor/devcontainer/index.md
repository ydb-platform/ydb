# Использование Dev Container для контрибьюторов клиентских SDK {{ ydb-short-name }}

- [Официальная документация Dev Containers](https://containers.dev/)
- [Документация расширения VS Code Dev Containers](https://code.visualstudio.com/docs/devcontainers/containers)

Dev Container позволяет быстро развернуть воспроизводимую и изолированную среду для разработки и тестирования клиентских {{ ydb-short-name }} SDK.

## Преимущества Dev Container

- Не требуется ручная настройка окружения и зависимостей.
- Среда полностью совпадает с CI и другими участниками проекта.
- Можно запускать интеграционные тесты с локальной базой {{ ydb-short-name }} без дополнительной подготовки.
- Все необходимые инструменты для разработки и тестирования уже установлены.

## Как начать работу

1. Установите контейнерный движок (например, [Docker](https://www.docker.com/) или [Podman](https://podman.io/)) и [Visual Studio Code](https://code.visualstudio.com/) с расширением [Dev Containers](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers).
2. Клонируйте репозиторий нужного [SDK](https://github.com/ydb-platform?q=sdk).
3. Если в репозитории есть папка `.devcontainer`, откройте проект в VS Code и выберите команду **Reopen in Container**.
4. После запуска среды используйте стандартные команды для сборки, тестирования и запуска кода.

## Особенности реализации в {{ ydb-short-name }} SDK

### Java SDK
- В папке [`.devcontainer`](https://github.com/ydb-platform/ydb-java-sdk/tree/master/.devcontainer) есть `Dockerfile`, `devcontainer.json` и скрипты для автоматической настройки окружения.
- Установлены все необходимые зависимости.

### Go SDK
- В папке [`.devcontainer`](https://github.com/ydb-platform/ydb-go-sdk/tree/master/.devcontainer) есть `Dockerfile`, `devcontainer.json`, `compose.yml` и скрипты для автоматической настройки окружения.
- Установлены все необходимые зависимости.
- При запуске среды автоматически поднимается локальный кластер {{ ydb-short-name }}, к которому сразу настроен доступ из контейнера.

### JavaScript/TypeScript SDK
- В папке [`.devcontainer`](https://github.com/ydb-platform/ydb-js-sdk/tree/main/.devcontainer) есть `Dockerfile`, `devcontainer.json`, `compose.yml` и скрипты для автоматической настройки окружения.
- Установлены все необходимые зависимости.
- При запуске среды автоматически поднимается локальный кластер {{ ydb-short-name }}, к которому сразу настроен доступ из контейнера.

### Python SDK (PR #590)
- В папке [`.devcontainer`](https://github.com/ydb-platform/ydb-python-sdk/pull/590/files) есть `Dockerfile`, `devcontainer.json`, `compose.yml` и скрипты для автоматической настройки окружения.
- Установлены все необходимые зависимости.
- При запуске среды автоматически поднимается локальный кластер {{ ydb-short-name }}, к которому сразу настроен доступ из контейнера.
