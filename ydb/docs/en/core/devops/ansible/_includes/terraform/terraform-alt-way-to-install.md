[Instructions](https://developer.hashicorp.com/terraform/install) for installing Terraform can be found on the HashiCorp website; however, access to the official repositories for downloading may be restricted for users in Russia.

You can download and install Terraform from the Yandex Cloud mirror:

{% list tabs %}

- Linux/macOS

    1. Go to this [link](https://hashicorp-releases.yandexcloud.net/terraform/) and choose the version of Terraform that suits you.
    1. Download the archive using:

        ```bash
        sudo curl -L -o <archive_name>.zip \
        https://hashicorp-releases.yandexcloud.net/terraform/<terraform_version>/ \
        <terraform_version_architecture_and_os>.zip
        ```

    1. Unpack the archive using the built-in archiver in macOS or with the command unzip `unzip <archive_name>.zip` for Linux. This will extract the Terraform binary file and accompanying files. You can install `unzip` with the command `apt update && apt install unzip`.
    1. Create an _alias_ for Terraform (a way to create shortcuts for commands):

    * Open the shell configuration file in a text editor (`~/.bashrc` or `~/.zshrc`) and add the following line to the end of the file: `alias terraform='<path_to_binary_file_terraform>'`;
    * Save the changes and reload the configuration with the command `source ~/.bashrc` or `source ~/.zshrc`;

    1. Check that Terraform is working by running the command `terraform -version`.

- Windows

    1. Go to this [link](https://hashicorp-releases.yandexcloud.net/terraform/) and choose the version of Terraform that suits you.
    1. Extract the archive using the standard Windows tools into a directory convenient for you.
    1. Open the PowerShell command prompt and verify that Terraform is working by running the command `terraform -version` in the directory where Terraform was extracted.

{% endlist %}