# Installing the SDK

<!-- markdownlint-disable blanks-around-fences -->

The instructions below describe how to quickly install the SDK. Your workstation must have the tools for your chosen programming language and package managers pre-installed and configured.

Instructions for building from source code are available in the GitHub source repositories, links to which are provided on the [{{ ydb-short-name }} SDK - Overview](../index.md) page.

{% list tabs %}

- Python

  Run the following command from the command line:

  {% include [install/cmd_python.md](install/cmd_python.md) %}

  If the command fails, make sure your environment has [Python3](https://www.python.org/downloads/) version 3.8 or newer, with the [pip](https://pypi.org/project/pip/) package manager enabled.

- Go

  Run the following command from the command line:

  {% include [install/cmd_go.md](install/cmd_go.md) %}

  For a successful installation, your environment must have [Go](https://go.dev/doc/install) version 1.17 or higher.

- C# (.NET)

  {% include [install/cmd_dotnet.md](install/cmd_dotnet.md) %}

- Java

  Add the dependencies to your Maven project as described in the ["Install the SDK"](https://github.com/ydb-platform/ydb-java-sdk#install-the-sdk) section of the `readme.md` file in the source repository.

- JavaScript

  {% include [install/cmd_npm.md](install/cmd_npm.md) %}

  The minimum supported version of [Node.js®](https://nodejs.org/en/download) is 20.19 or higher.

- Rust

  {% include [install/cmd_rust.md](install/cmd_rust.md) %}

- PHP

  {% include [install/cmd_php.md](install/cmd_php.md) %}

{% endlist %}
