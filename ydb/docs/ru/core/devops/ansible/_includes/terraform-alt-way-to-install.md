[Инструкцию](https://developer.hashicorp.com/terraform/install) по установке Terraform можно найти на сайте HashiCorp, однако доступ к официальным репозиториям для скачивания может быть ограничен для пользователей из России. 

Скачать и установить Terraform можно с зеркала Yandex Cloud:

{% list tabs %}

- Linux/macOS

    1. Перейдите по [ссылке](https://hashicorp-releases.yandexcloud.net/terraform/) и выберите подходящую вам версию Terraform.
    1. Скачайте архив `sudo curl -L -o <archive_name>.zip https://hashicorp-releases.yandexcloud.net/terraform/<terraform_version>/<terraform_version_architecture_and_os>.zip`.
    1. Распакуйте архив с помощью встроенного архиватора в macOS или командой `unzip <archive_name>.zip` для Linux. Будет распакован бинарный файл Terraform и сопроводительные файлы. Установить `unzip` можно командой `apt update && apt install unzip`.
    1. Создайте _alias_ для Terraform (способ создания сокращений для команд):
    
    * Откройте в текстовом редакторе конфигурационный файл оболочки (`~/.bashrc` или `~/.zshrc`) и добавьте в конец файла `alias terraform='<path_to_binary_file_terraform>'`;
    * Сохраните изменения и перечитайте конфигурацию командой `source ~/.bashrc` или `source ~/.zshrc`

    1. Проверьте работоспособность Terraform, выполнив команду `terraform -version`. 

- Windows

    1. Перейдите по [ссылке](https://hashicorp-releases.yandexcloud.net/terraform/) и выберите подходящую вам версию Terraform и скачайте архив.
    1. Распакуйте архив стандартными средствами Windows в удобную вам директорию.
    1. Откройте командную строку PowerShell и проверьте работоспособность terraform, выполнив команду `terraform -version` в директории, куда был распакован Terraform.

{% endlist %}