# Installing the SDK

<!-- markdownlint-disable blanks-around-fences -->

Below are instructions for a quick SDK installation. Your workstation must have the tools for working with the selected programming language and package managers pre-installed and configured.

Instructions for building from source code are available in the source code repositories on GitHub, links to which are provided on the [{{ ydb-short-name }} SDK — Overview](../index.md) page.

{% list tabs %}

- C++

  Clone the [ydb-cpp-sdk](https://github.com/ydb-platform/ydb-cpp-sdk) repository and run the following command from the command line:

  {% include [install/cmd_cpp.md](install/cmd_cpp.md) %}

  - `compiler` — your compiler (`clang` or `gcc`)
  - `ydb_install_dir` — the path where you want to install the SDK.

  Before running the command, make sure all dependencies are installed.

- Python

  Run the following command from the command line:

  {% include [install/cmd_python.md](install/cmd_python.md) %}

  If the command fails, make sure your environment has [Python3](https://www.python.org/downloads/) version 3.8 or later installed, with the [pip](https://pypi.org/project/pip/) package manager enabled.

- Go

  Run the following command from the command line:

  {% include [install/cmd_go.md](install/cmd_go.md) %}

  For a successful installation, your environment must have [Go](https://go.dev/doc/install) version 1.17 or higher installed.

- C#

  {% include [install/cmd_dotnet.md](install/cmd_dotnet.md) %}

- Java

  Add the dependencies to your Maven project as described in the ["Install the SDK"](https://github.com/ydb-platform/ydb-java-sdk#install-the-sdk) section of the `readme.md` file in the source code repository.

- JavaScript

  {% include [install/cmd_npm.md](install/cmd_npm.md) %}

  The minimum supported version of [Node.js®](https://nodejs.org/en/download) is 20.19 or higher.

- Rust

  {% include [install/cmd_rust.md](install/cmd_rust.md) %}

- PHP

  {% include [install/cmd_php.md](install/cmd_php.md) %}

{% endlist %}
