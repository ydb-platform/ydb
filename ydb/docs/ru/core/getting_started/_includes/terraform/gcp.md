1. Зарегистрируйтесь в Google Cloud console и [создайте](https://console.cloud.google.com/projectselector2/home) проект.
3. Активируйте [платежный аккаунт](https://console.cloud.google.com/billing/manage) и пополните его средствами для запуска девяти ВМ. Рассчитать стоимость можно в [калькуляторе](https://cloud.google.com/products/calculator?hl=en&dl=CiQ2N2I0OThlMS04NmQ1LTRhMzUtOTI0NS04YmVmZTVkMWQ2ODUQCBokRjJGMjBFOTgtQkY0MC00QTcyLUFFNjktODYxMDU2QUIyRDBD). 
4. Актируйте [Compute Engine API](https://console.cloud.google.com/apis/api/compute.googleapis.com/metrics) и [Cloud DNS API](https://console.cloud.google.com/apis/api/dns.googleapis.com/metrics).
5. Скачайте и установите GCP CLI, следуя [этой](https://cloud.google.com/sdk/docs/install) инструкции.
6. Перейдите в поддиректорию `.../google-cloud-sdk/bin` и выполните команду `./gcloud compute regions list` для получения списка доступных регионов.
7. Выполните команду `./gcloud auth application-default login` для настройки профиля подключения.
8. Скачайте репозиторий с помощью команды `git clone https://github.com/ydb-platform/ydb-terraform`.
9. Перейдите в поддиректорию `gcp` (находится в скаченном репозитории) и в файле `variables.tf` задайте актуальные значения следующим переменным:
    * `project` – название проекта, которое было задано в облачной консоли Google Cloud.
    * `region` – регион, где будет развернута инфраструктура.
    * `zones` – список зон доступности, в которых будут созданы подсети и ВМ.

Теперь, находясь в поддиректории `gcp` можно выполнить последовательность следующих команд для установки провайдера, инициализации модулей и создания инфраструктуры:
* `terraform init` – установка провайдера и инициализация модулей.
* `terraform plan` – создание плана будущей инфраструктуры.
* `terraform init` (повторное выполнение) – создание ресурсов в облаке. 

Далее используются команды `terraform plan`, `terraform init` и `terraform destroy` (уничтожение созданной инфраструктуры).