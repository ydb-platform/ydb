# Using Dev Container for Client {{ ydb-short-name }} SDK Contributors

- [Official Dev Containers Documentation](https://containers.dev/)
- [VS Code Dev Containers Extension Documentation](https://code.visualstudio.com/docs/devcontainers/containers)

Dev Container allows you to quickly set up a reproducible and isolated environment for developing and testing client {{ ydb-short-name }} SDKs.

## Benefits of Dev Container

- No need for manual environment and dependency setup.
- The environment matches CI and other project contributors.
- You can run integration tests with a local {{ ydb-short-name }} database without extra preparation.
- All necessary tools for development and testing are pre-installed.

## Getting Started

1. Install a container engine (e.g., [Docker](https://www.docker.com/) or [Podman](https://podman.io/)) and [Visual Studio Code](https://code.visualstudio.com/) with the [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers).
2. Clone the repository of the required [SDK](https://github.com/ydb-platform?q=sdk).
3. If the repository contains a `.devcontainer` folder, open the project in VS Code and select **Reopen in Container**.
4. After the environment starts, use standard commands to build, test, and run the code.

## Implementation Details in {{ ydb-short-name }} SDKs

### Java SDK
- The [`.devcontainer`](https://github.com/ydb-platform/ydb-java-sdk/tree/master/.devcontainer) contains a `Dockerfile`, `devcontainer.json`, and scripts for automatic environment setup.
- The environment includes JDK, Gradle, and development tools.
- You can extend the Dockerfile to install additional tools.

### Go SDK
- The [`.devcontainer`](https://github.com/ydb-platform/ydb-go-sdk/tree/master/.devcontainer) contains a `Dockerfile`, `devcontainer.json`, `compose.yml`, and scripts for automatic environment setup.
- All dependencies and the required Go version are pre-installed.
- [ydb-cli](https://ydb.tech/docs/en/reference/ydb-cli/) is installed.
- When the environment starts, a local {{ ydb-short-name }} cluster is automatically started and access from the container is pre-configured.

### JavaScript/TypeScript SDK
- The [`.devcontainer`](https://github.com/ydb-platform/ydb-js-sdk/tree/main/.devcontainer) contains a `Dockerfile`, `devcontainer.json`, `compose.yml`, and scripts for automatic environment setup.
- The environment includes Node.js, npm, TypeScript, and development tools.
- [ydb-cli](https://ydb.tech/docs/en/reference/ydb-cli/) is installed.
- When the environment starts, a local {{ ydb-short-name }} cluster is automatically started and access from the container is pre-configured.

### Python SDK (PR #590)
- The [`.devcontainer`](https://github.com/ydb-platform/ydb-python-sdk/pull/590/files) contains a `Dockerfile`, `devcontainer.json`, `compose.yml`, and scripts for automatic environment setup.
- All dependencies, flake8, tox are pre-installed.
- [ydb-cli](https://ydb.tech/docs/en/reference/ydb-cli/) is installed.
- When the environment starts, a local {{ ydb-short-name }} cluster is automatically started and access from the container is pre-configured.
