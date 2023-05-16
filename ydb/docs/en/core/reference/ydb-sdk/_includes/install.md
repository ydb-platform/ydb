# Installing the SDK

Follow the instructions below to quickly install the OpenSource SDK. Make sure to preinstall and configure tools for working with the selected programming language and package managers on your workstation.

The build process using the source code is described in the source code repositories on GitHub. Follow the links given on the [YDB SDK - Overview](../index.md) page.

{% list tabs %}

- Python

  Run the command from the command line:

  {% include [install/cmd_python.md](install/cmd_python.md) %}

  If the command fails, make sure your environment has [Python](https://www.python.org/downloads/) 3.8 or newer installed with the [pip](https://pypi.org/project/pip/) package manager enabled.

- Go

  Run the command from the command line:

  {% include [install/cmd_go.md](install/cmd_go.md) %}

  To ensure that the installation is successful, make sure that your environment is running [Go](https://go.dev/doc/install) 1.17 or higher.

- C# (.NET)

  {% include [install/cmd_dotnet.md](install/cmd_dotnet.md) %}

- Java

  Add dependencies to the Maven project as described in the ["Install the SDK"](https://github.com/ydb-platform/ydb-java-sdk#install-the-sdk) step of the `readme.md` file in the source code repository.

- PHP

  {% include [install/cmd_php.md](install/cmd_php.md) %}

- Node.JS

  {% include [install/cmd_nodejs.md](install/cmd_nodejs.md) %}

- Rust

  {% include [install/cmd_rust.md](install/cmd_rust.md) %}

{% endlist %}

