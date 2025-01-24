Для создания инфраструктуры в Yandex Cloud с помощью Terraform нужно:

1. Подготовить облако к работе:

    * [Зарегистрироваться](https://console.yandex.cloud) в Yandex Cloud.
    * [Подключить]({{ yandex_docs }}/billing/concepts/billing-account) платежный аккаунт.
    * [Убедится](https://billing.yandex.cloud/) в наличии достаточного количества средств для создания девяти ВМ.

1. Установить и настроить Yandex Cloud CLI:

    * [Скачать]({{ yandex_docs }}/cli/quickstart) Yandex Cloud CLI.
    * [Создать]({{ yandex_docs }}/cli/quickstart#initialize) профиль

1. [Создать]({{ yandex_docs }}/tutorials/infrastructure-management/terraform-quickstart#get-credentials) сервисный аккаунт с помощью CLI.
1. [Сгенерировать]({{ yandex_docs }}/cli/operations/authentication/service-account#auth-as-sa) авторизованный ключ в JSON формате для подключения Terraform к облаку с помощью CLI: `yc iam key create --service-account-name <acc name> --output <file name> --folder-id <cloud folder id>`. В терминал будет выведена информация о созданном ключе:

    ```text
    id: ajenap572v8e1l...
    service_account_id: aje90em65r69...
    created_at: "2024-09-03T15:34:57.495126296Z"
    key_algorithm: RSA_2048
    ```

    Авторизованный ключ будет создан в директории, где вызывалась команда.

1. [Настроить]({{ yandex_docs }}/tutorials/infrastructure-management/terraform-quickstart#configure-provider) Yandex Cloud Terraform провайдера.
1. Скачать данный репозиторий командой `git clone https://github.com/ydb-platform/ydb-terraform.git`.
1. Перейти в директорию `yandex_cloud` (директория в скаченном репозитории) и внести изменения в следующие переменные, в файле `variables.tf`:

    * `key_path` – путь к сгенерированному авторизованному ключу с помощью CLI.
    * `cloud_id` – ID облака. Можно получить список доступных облаков командой `yc resource-manager cloud list`.
    * `folder_id` – ID Cloud folder. Можно получить командой `yc resource-manager folder list`.

Теперь, находясь в поддиректории `yandex_cloud`, можно выполнить последовательность следующих команд для установки провайдера, инициализации модулей и создания инфраструктуры:

1. `terraform init` – установка провайдера и инициализация модулей.
1. `terraform plan` – создание плана будущей инфраструктуры.
1. `terraform apply` (повторное выполнение) – создание ресурсов в облаке.

Далее используются команды `terraform plan`, `terraform apply` и `terraform destroy` (уничтожение созданной инфраструктуры).

{% note info %}

С помощью Yandex Cloud провайдера можно не только создавать инфраструктуру для дальнейшего развертывания на ней {{ ydb-short-name }} кластера с помощью [Ansible](../../initial-deployment.md), но и управлять [serverless или dedicated](https://yandex.cloud/ru/services/ydb) версией {{ ydb-short-name }} прямо из Terraform. О возможностях работы с {{ ydb-short-name }} в Yandex Cloud читайте в разделе [Работа с {{ ydb-short-name }} через Terraform]({{ yandex_docs }}/ydb/terraform/intro) документации Yandex Cloud.

{% endnote %}