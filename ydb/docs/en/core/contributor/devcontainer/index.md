# Using Dev Container for YDB Client SDK Contributors

- [Official Dev Containers Documentation](https://containers.dev/)
- [VS Code Dev Containers Extension Documentation](https://code.visualstudio.com/docs/devcontainers/containers)

Dev Container allows you to quickly set up a reproducible and isolated environment for developing and testing YDB client SDKs.

## Why Use It

- No need for manual environment and dependency setup.
- The environment matches CI and other project contributors.
- You can run integration tests with a local YDB database without extra preparation.
- All necessary tools for development and testing are pre-installed.

## Getting Started

1. Install a container engine (e.g., Docker or Podman) and Visual Studio Code with the Dev Containers extension.
2. Clone the repository of the required SDK.
3. If the repository contains a `.devcontainer` folder, open the project in VS Code and select **Reopen in Container**.
4. After the environment starts, use standard commands to build, test, and run the code.

## Implementation Details in YDB SDKs

### Java SDK
- The [`devcontainer`](https://github.com/ydb-platform/ydb-java-sdk/tree/master/.devcontainer) contains a `Dockerfile`, `devcontainer.json`, and scripts.
- The environment includes JDK, Gradle, and development tools.
- You can extend the Dockerfile to install additional tools.

### Go SDK
- The [`devcontainer`](https://github.com/ydb-platform/ydb-go-sdk/tree/master/.devcontainer) contains a `Dockerfile`, `devcontainer.json`, `compose.yml`, and scripts.
- A local YDB cluster is automatically started for integration tests.
- All dependencies and the required Go version are pre-installed.

### JavaScript/TypeScript SDK
- The [`devcontainer`](https://github.com/ydb-platform/ydb-js-sdk/tree/main/.devcontainer) contains a `Dockerfile`, `devcontainer.json`, `compose.yml`, and scripts.
- The environment includes Node.js, npm, TypeScript, and development tools.
- When the devcontainer starts, a YDB cluster is automatically started for integration tests.

### Python SDK (PR #590)
- The [`devcontainer`](https://github.com/ydb-platform/ydb-python-sdk/pull/590/files) contains a `Dockerfile`, `devcontainer.json`, `compose.yml`, scripts, and automatic ydb-cli profile setup.
- When the environment starts, a local YDB cluster is automatically started and access from the container is pre-configured.
- All dependencies, flake8, tox, and ydb-cli are pre-installed.
