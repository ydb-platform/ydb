# Installing the {{ ydb-short-name }} SDK

<!-- markdownlint-disable blanks-around-fences -->

Follow the instructions below to quickly install the OpenSource SDK. Make sure to preinstall and configure tools for working with the selected programming language and package managers on your workstation.

The build process using the source code is described in the source code repositories on GitHub. Follow the links given on the [{{ ydb-short-name }} SDK - Overview](../index.md) page.

{% list tabs %}

- Go

  Run the command from the command line:

  {% include [install/cmd_go.md](install/cmd_go.md) %}

  To ensure that the installation is successful, make sure that your environment is running [Go](https://go.dev/doc/install) 1.17 or higher.

- Java

  Add dependencies to the Maven project as described in the ["Install the SDK"](https://github.com/ydb-platform/ydb-java-sdk#install-the-sdk) step of the `readme.md` file in the source code repository.

- Python

  Run the command from the command line:

  {% include [install/cmd_python.md](install/cmd_python.md) %}

  If the command fails, make sure your environment has [Python](https://www.python.org/downloads/) 3.8 or newer installed with the [pip](https://pypi.org/project/pip/) package manager enabled.

- C# (.NET)

  {% include [install/cmd_dotnet.md](install/cmd_dotnet.md) %}

- JavaScript

  {% include [install/cmd_npm.md](install/cmd_npm.md) %}

  The minimum supported [Node.js®](https://nodejs.org/en/download) version is 20.19.

- Rust

  {% include [install/cmd_rust.md](install/cmd_rust.md) %}

- C++

  ### Debian packages (Ubuntu 24.04)

  Pre-built `.deb` packages for Ubuntu 24.04 (Noble), amd64 are attached to each [GitHub release](https://github.com/ydb-platform/ydb-cpp-sdk/releases). Download and install them locally:

  {% include [install/cmd_cpp_deb.md](install/cmd_cpp_deb.md) %}

  Available packages:

  - `yandex-googleapis-api-common-protos` — required protobuf dependency;
  - `libydb-cpp-dev` — core SDK: static library, public headers, and CMake package files;
  - `libydb-cpp-iam-dev` — IAM credentials plugin (optional);
  - `libydb-cpp-otel-metrics-dev` — OpenTelemetry metrics plugin (optional);
  - `libydb-cpp-otel-tracing-dev` — OpenTelemetry tracing plugin (optional; requires `libydb-cpp-otel-metrics-dev` for OTel headers and libraries).

  {% note info %}

  - Supported platform: Ubuntu 24.04 (Noble), amd64.
  - Packages are bundled without apt dependencies; install `yandex-googleapis-api-common-protos` alongside the core SDK package.
  - For other platforms, use the build-from-source instructions below.

  {% endnote %}

  ### Using the SDK in CMake

  After installing the packages, use the SDK in your CMake project:

  {% include [install/cmd_cpp_cmake.md](install/cmd_cpp_cmake.md) %}

  Pass `-DCMAKE_PREFIX_PATH=/usr/share/yandex` when configuring your project, since the packages install under the Yandex prefix.

  ### Build from source

  Clone the [ydb-cpp-sdk](https://github.com/ydb-platform/ydb-cpp-sdk) repository and run the following commands from the command line:

  {% include [install/cmd_cpp_build.md](install/cmd_cpp_build.md) %}

  - `compiler` — your compiler (`clang` or `gcc`);
  - `ydb_install_dir` — the path where you want to install the SDK.

  Make sure all dependencies are installed before running the commands. See the [README](https://github.com/ydb-platform/ydb-cpp-sdk/blob/main/README.md) in the source repository for the full list of build dependencies and instructions.

- PHP

  {% include [install/cmd_php.md](install/cmd_php.md) %}

{% endlist %}
