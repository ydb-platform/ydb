Для создания инфраструктуры в Yandex Cloud с помощью Terraform нужно:

1. Подготовить облако к работе:
    * [Зарегистрироваться](https://console.cloud.yandex.ru/) в Yandex Cloud.
    * [Подключить](https://cloud.yandex.com/ru/docs/billing/concepts/billing-account) платежный аккаунт.
    * [Убедится](https://console.cloud.yandex.ru/billing) в наличии достаточного количества средств для создания девяти ВМ.
2. Установить и настроить Yandex Cloud CLI:
    * [Скачать](https://cloud.yandex.ru/ru/docs/cli/quickstart) Yandex Cloud CLI.
    * [Создать](https://cloud.yandex.ru/ru/docs/cli/quickstart#initialize) профиль
3. [Создать](https://cloud.yandex.com/ru/docs/tutorials/infrastructure-management/terraform-quickstart#get-credentials) сервисный аккаунт с помощью CLI.
4. [Сгенерировать](https://cloud.yandex.ru/ru/docs/cli/operations/authentication/service-account#auth-as-sa) SA ключ в JSON формате для подключения Terraform к облаку с помощью CLI: `yc iam key create --service-account-name <acc name> --output <file name> --folder-id <cloud folder id>`. Будет сгенерирован SA ключ, а в терминал будет выведена секретная информация:
    ```
    access_key:
        id: ajenhnhaqgd3vp...
        service_account_id: aje90em65r6922...
        created_at: "2024-03-05T20:10:50.0150..."
        key_id: YCAJElaLsa0z3snzH4E...
    secret: YCPKNJDVhRZgyywl4hQwVdcSRC...
    ```
    Скопируйте `access_key.id` и `secret`. Значения этих полей нужны будут в дальнейшем при работе с AWS CLI.
5. [Скачать](https://aws.amazon.com/ru/cli/) AWS CLI.
6. Настроить окружение AWS CLI:
    * Запустите команду `aws configure` и последовательно введите сохраненные ранее `access_key.id` и `secret`. Для значения региона используйте `ru-central1`:
    ```
    aws configure
    AWS Access Key ID [None]: AKIAIOSFODNN********
    AWS Secret Access Key [None]: wJalr********/*******/bPxRfiCYEX********
    Default region name [None]: ru-central1
    Default output format [None]:
    ```
    Будут созданы файлы `~/.aws/credentials` и `~/.aws/config`.
7. Отредактировать `~/.aws/credentials` и `~/.aws/config` следующим образом:
    * Добавьте `[Ya_def_reg]` в `~/.aws/config` перед `region = ru-central1-a`.
    * Добавьте `[Yandex]` перед секретной информацией о ключах подключения.
8. [Настроить](https://cloud.yandex.com/ru/docs/tutorials/infrastructure-management/terraform-quickstart#configure-provider) Yandex Cloud Terraform провайдера.
9. Скачать данный репозиторий командой `git clone https://github.com/ydb-platform/ydb-terraform.git`.
10. Перейти в директорию `yandex_cloud` (директория в скаченном репозитории) и внести изменения в следующие переменные, в файле `variables.tf`:
    * `key_path` – путь к сгенерированному SA ключу с помощью CLI.
    * `cloud_id` – ID облака. Можно получить список доступных облаков командой `yc resource-manager cloud list`.
    * `profile` – название профиля из файла `~/.aws/config`.
    * `folder_id` – ID Cloud folder. Можно получить командой `yc resource-manager folder list`.

Теперь, находясь в поддиректории `yandex_cloud`, можно выполнить последовательность следующих команд для установки провайдера, инициализации модулей и создания инфраструктуры:

1. `terraform init` – установка провайдера и инициализация модулей.
2. `terraform plan` – создание плана будущей инфраструктуры.
3. `terraform apply` (повторное выполнение) – создание ресурсов в облаке.

Далее используются команды `terraform plan`, `terraform apply` и `terraform destroy` (уничтожение созданной инфраструктуры).

{% note info %}

С помощью Yandex Cloud провайдера можно не только создавать инфраструктуру для дальнейшего развертывания на ней {{ ydb-short-name }} кластера с помощью [Ansible](../../initial-deployment.md), но и управлять [serverless или dedicated](https://cloud.yandex.ru/ru/services/ydb) версией {{ ydb-short-name }} прямо из Terraform. О возможностях работы с {{ ydb-short-name }} в Yandex Cloud читайте в разделе [Работа с YDB через Terraform](https://cloud.yandex.ru/ru/docs/ydb/terraform/intro) документации Yandex Cloud.

{% endnote %}